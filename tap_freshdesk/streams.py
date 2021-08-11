# If using the class-based model, this is where all the stream classes and their corresponding functions live.
import singer
import datetime
from tap_freshdesk import helper

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
        records = list(self.client.get(self.endpoint, params=params))
        for page in records:
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

    def push_records(self, pages):
        for page in pages:
            for record in page:
                record.pop('attachments', None)
                record['source_label'] = SOURCE.get(record.get('source', False), False)
                record['status_label'] = STATUS.get(record.get('status', False), False)
                record['priority_label'] = PRIORITY.get(record.get('priority', False), False)
                start_date = record['updated_at']
                yield record
            start_date = helper.strptime(start_date) + datetime.timedelta(seconds=1)
            start_date = helper.strftime(start_date)

            helper.update_state(self.state, self.stream_id, start_date)
            singer.write_state(self.state)
        return True

    def pagination_reset(self, args, flag=True):
        # Don't keep records in-memory, push data for each pagination reset
        while flag:
            records = list(self.client.get(self.endpoint, params=args))
            flag = args.get("reset_pagination", False)
            if 'reset_pagination' in args:
                del args['reset_pagination']
            yield from self.push_records(records)

    def get_records(self, args):
        # TODO: add state for deleted_tickets and spam_tickets along with args init start_date
        # because ticket state is updated after reset pagination
        # as a result deleted and spam will be filtered with the latest ticket state date
        for predefined_filter in ["", "deleted", "spam"]:
            message = "Syncing tickets "
            if predefined_filter:
                # Get filtered record, as deleted records won't show on unfiltered call
                args['filter'] = predefined_filter
                message += "with filter "
            LOGGER.info(message + "{}".format(predefined_filter))
            yield from self.pagination_reset(args)

    def sync(self, start_date):
        params = {
            'updated_since': start_date,
            'order_by': 'updated_at',
            'order_type': "asc",
            'include': "requester,company,stats"
        }
        yield from self.get_records(params)


class Conversations(Stream):
    stream_id = 'conversations'
    stream_name = 'conversations'
    endpoint = 'tickets/{id}/conversations'
    custom_fields = False
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    ticket_ids = []

    def sync(self, start_date):
        tickets = Tickets(self.client, self.config, self.state)
        for ticket_id in tickets.get_all_ticket_ids():
            records = self.client.get(self.endpoint.format(id=ticket_id), params={})

            for page in records:
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
    replication_method = "INCREMENTAL"
    replication_keys = ['created_at']

    def sync(self, start_date):
        params = {'created_since': start_date}
        questions = []
        # Get survey questions (id, label)
        for question_page in self.client.get("surveys", params={}):
            for question in question_page:
                if question.get("questions", False):
                    questions.append(question.get("questions", False)[0])

        records = self.client.get(self.endpoint, params=params)

        for page in records:
            for rec in page:
                if rec['created_at'] > start_date:
                    start_date = rec['created_at']
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
            start_date = helper.strptime(start_date) + datetime.timedelta(seconds=1)
            start_date = helper.strftime(start_date)

            helper.update_state(self.state, self.stream_id, start_date)
            singer.write_state(self.state)


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
}
