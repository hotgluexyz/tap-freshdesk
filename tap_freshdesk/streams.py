# If using the class-based model, this is where all the stream classes and their corresponding functions live.
import singer
import datetime
from tap_freshdesk import helper
import requests
import csv
from io import StringIO

LOGGER = singer.get_logger()


class Stream:
    def __init__(self, client, config, state):
        self.client = client
        self.config = config
        self.state = state


TICKET_SCOPE = {
    1: "Global Access",
    2: "Group Access",
    3: "Restricted Access"
}


class Agents(Stream):
    stream_id = 'agents'
    stream_name = 'agents'
    endpoint = 'agents'
    custom_fields = False
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []

    def sync(self, start_date):
        for page in self.client.get(self.endpoint, params={}):
            for rec in page:
                rec['ticket_label'] = TICKET_SCOPE.get(rec.get('ticket_scope', False), False)
                yield rec


class Companies(Stream):
    stream_id = 'companies'
    stream_name = 'companies'
    endpoint = 'companies'

    custom_fields = 'company_fields'
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []

    def sync(self, start_date):
        for page in self.client.get(self.endpoint, params={}):
            for rec in page:
                yield rec


class Contacts(Stream):
    stream_id = 'contacts'
    stream_name = 'contacts'
    endpoint = 'contacts'
    custom_fields = 'contact_fields'
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]

    def sync(self, start_date):
        params = {'_updated_since': start_date}
        pages = self.client.get(self.endpoint, params=params)
        for page in pages:
            for rec in page:
                if rec['updated_at'] >= start_date:
                    # updated with one second to not get doubled records for the same datetime
                    start_date = rec['updated_at']
                yield rec
            start_date = helper.strptime(start_date) + datetime.timedelta(seconds=1)
            start_date = helper.strftime(start_date)

            helper.update_state(self.state, self.stream_id, start_date)
            singer.write_state(self.state)


class Groups(Stream):
    stream_id = 'groups'
    stream_name = 'groups'
    endpoint = 'groups'
    custom_fields = False
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []

    def sync(self, start_date):
        for page in self.client.get(self.endpoint, params={}):
            for rec in page:
                yield rec


class Roles(Stream):
    stream_id = 'roles'
    stream_name = 'roles'
    endpoint = 'roles'
    custom_fields = False
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []

    def sync(self, start_date):
        for page in self.client.get(self.endpoint, params={}):
            for rec in page:
                yield rec


# Ticket mapping for source, status, priority
SOURCE = {
    1: "Email",
    2: "Portal",
    3: "Phone",
    7: "Chat",
    9: "Feedback Widget",
    10: "Outbound Email"
}

STATUS = {
    2: "Open",
    3: "Pending",
    4: "Resolved",
    5: "Closed"
}

PRIORITY = {
    1: "Low",
    2: "Medium",
    3: "High",
    4: "Urgent"
}


class Tickets(Stream):
    stream_id = 'tickets'
    stream_name = 'tickets'
    endpoint = 'tickets'
    custom_fields = 'ticket_fields'
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ['updated_at']

    ticket_ids = []

    def get_all_ticket_ids(self):
        if Tickets.ticket_ids:
            for ticket in Tickets.ticket_ids:
                yield ticket
        else:
            for page in self.client.get(self.endpoint):
                for rec in page:
                    Tickets.ticket_ids.append(rec['id'])
                    yield rec['id']

    def sync(self, start_date):
        params = {
            'updated_since': start_date,
            'order_by': 'updated_at',
            'order_type': "asc",
            'include': "requester,company,stats"
        }

        for predefined_filter in ["", "deleted", "spam"]:
            LOGGER.info("Syncing tickets with filter {}".format(predefined_filter))
            stream = self.stream_id
            if predefined_filter:
                params['filter'] = predefined_filter
                stream = params['filter'] + "_" + stream
            page_generator = self.client.get(self.endpoint, params=params)

            # Get filtered record, as deleted records won't show on unfiltered call
            for page in page_generator:
                for rec in page:
                    rec.pop('attachments', None)
                    rec['source_label'] = SOURCE.get(rec.get('source', False), False)
                    rec['status_label'] = STATUS.get(rec.get('status', False), False)
                    rec['priority_label'] = PRIORITY.get(rec.get('priority', False), False)
                    start_date = rec['updated_at']
                    yield rec
                start_date = helper.strptime(start_date) + datetime.timedelta(seconds=1)
                start_date = helper.strftime(start_date)

                helper.update_state(self.state, stream, start_date)
                singer.write_state(self.state)


class Conversations(Stream):
    stream_id = 'conversations'
    stream_name = 'conversations'
    endpoint = 'tickets/{id}/conversations'
    custom_fields = False
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    ticket_ids = []

    def get_all_tickets(self):
        tickets = Tickets(self.client, self.config, self.state)
        return tickets.get_all_ticket_ids()


    def sync(self, start_date):
        for ticket_id in self.get_all_tickets():
            pages = self.client.get(self.endpoint.format(id=ticket_id), params={})
            for page in pages:
                for rec in page:
                    rec.pop("attachments", None)
                    rec.pop("body", None)
                    yield rec


RATINGS = {
    103: "Extremely Happy",
    102: "Very Happy",
    101: "Happy",
    100: "Neutral",
    -101: "Unhappy",
    -102: "Very Unhappy",
    -103: "Extremely Unhappy",
    1: "Happy",
    2: "Neutral",
    3: "Unhappy"
}


class SatisfactionRatings(Stream):
    stream_id = 'satisfaction_ratings'
    stream_name = 'satisfaction_ratings'
    endpoint = 'surveys/satisfaction_ratings'
    custom_fields = False
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []

    def sync(self, start_date):
        params = {}
        questions = []
        # Get survey questions (id, label)
        for question_page in self.client.get("surveys", params={}):
            for question in question_page:
                if question.get("questions", False):
                    questions.append(question.get("questions", False)[0])

        records = self.client.get(self.endpoint, params=params)
        for page in records:
            for rec in page:
                if rec.get('ratings', False):
                    response = []
                    for k, v in rec['ratings'].items():
                        label = [qw["label"] for qw in questions if qw["id"] == k]
                        response.append({"question_id": k,
                                         "question_label": label and label[0] or "",
                                         "rating_id": v,
                                         "rating_label": RATINGS.get(v, False)})

                    rec.pop('ratings')  # remove dict
                    rec['ratings'] = response  # insert array of objects
                yield rec


class TimeEntries(Stream):
    stream_id = 'time_entries'
    stream_name = 'time_entries'
    endpoint = 'time_entries'
    custom_fields = False
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []

    def sync(self, start_date):
        for page in self.client.get(self.endpoint, params={}):
            for rec in page:
                yield rec
            singer.write_state(self.state)

class ExportReport(Stream):
    stream_id = 'export_report'
    stream_name = 'export_report'
    endpoint = 'reports/schedule/download_file.json'
    custom_fields = False
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    report_tickets = []

    def get_records(self):
        params = {"uuid":self.config.get("report_id")}
        url = "https://timeinternet.freshdesk.com/reports/schedule/download_file.json"
        resp = self.client._make_request_internal(full_url=url,api_key=self.config.get("api_key"),params=params)
        if resp.status_code == 200:
            data = resp.json()
            if "export" in data:
                if "url" in data['export']:
                    url = data['export']['url']
                    csv_file = requests.get(url)
                    # Read the CSV content from the response
                    csv_content = csv_file.text
                    # Use StringIO to create a file-like object for csv.reader
                    csv_file = StringIO(csv_content)
                    csv_reader = csv.DictReader(csv_file)
                    return csv_reader

    def sync(self, start_date):
        if self.config.get("report_id",None):
            csv_reader = self.get_records()
            # Iterate over each row in the CSV
            for row in csv_reader:
                # Convert keys to lowercase and replace spaces with underscores
                processed_row = {key.lower().replace(' ', '_'): value for key, value in row.items()}
                # Process each row as needed
                self.report_tickets.append(processed_row['ticket_id'])
                yield processed_row
        else:
            return []
    def get_report_tickets(self):
        if not self.report_tickets:
            res = self.sync(None)
            #process the generator
            res = list(res)
            return self.report_tickets
        return self.report_tickets    

class ReportTickets(Stream):
    stream_id = 'report_tickets'
    stream_name = 'report_tickets'
    endpoint = 'tickets/{id}'
    custom_fields = False
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    ticket_ids = []

    def get_all_tickets(self):
        tickets = ExportReport(self.client, self.config, self.state)
        return tickets.get_report_tickets()
    
    def sync(self, start_date):
        for ticket_id in self.get_all_tickets():
            url = self.client.get_base_url(self.endpoint.format(id=ticket_id))
            resp = self.client._make_request_internal(full_url=url,api_key=self.config.get("api_key"),params=None)
            resp.raise_for_status()
            yield resp.json()
class ReportConversations(Conversations):
    stream_id = 'report_conversations'
    stream_name = 'report_conversations'
    endpoint = 'tickets/{id}/conversations'

    def get_all_tickets(self):
        tickets = ExportReport(self.client, self.config, self.state)
        return tickets.get_report_tickets()
        
STREAM_OBJECTS = {
    'agents': Agents,
    'companies': Companies,
    'contacts': Contacts,
    'groups': Groups,
    'roles': Roles,
    'tickets': Tickets,
    'conversations': Conversations,
    'satisfaction_ratings': SatisfactionRatings,
    'time_entries': TimeEntries,
    'export_report':ExportReport,
    'report_tickets':ReportTickets,
    'report_conversations':ReportConversations,
}
