# If using the class-based model, this is where all the stream classes and their corresponding functions live.
import datetime

from singer import utils
import singer

LOGGER = singer.get_logger()


class Stream:
    def __init__(self, client, config, state):
        self.client = client
        self.config = config
        self.state = state


class Agents(Stream):
    stream_id = 'agents'
    stream_name = 'agents'
    endpoint = 'agents'
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []

    agent_ids = []

    def get_all_agent_ids(self):
        if Agents.agent_ids:
            for agent in Agents.agent_ids:
                yield agent
        else:
            records = self.client.get(self.endpoint)
            for rec in records.get('results'):
                Agents.agent_ids.append(rec['id'])
                yield rec['id']

    def sync(self):
        records = self.client.get(self.endpoint)
        for rec in records.get('results'):
            yield rec


class BusinessHours(Stream):
    stream_id = 'business_hours'
    stream_name = 'business_hours'
    endpoint = 'business_hours'
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []

    def sync(self):
        records = self.client.get(self.endpoint)
        for rec in records.get('results'):
            yield rec


class Companies(Stream):
    stream_id = 'companies'
    stream_name = 'companies'
    endpoint = 'companies'
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []

    def sync(self):
        records = self.client.get(self.endpoint, params={})
        for rec in records.get('results'):
            yield rec


class Contacts(Stream):
    stream_id = 'contacts'
    stream_name = 'contacts'
    endpoint = 'contacts'
    key_properties = ["id", "updated_at"]
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]

    def sync(self):
        records = self.client.get(self.endpoint, params={})
        for rec in records.get('results'):
            yield rec


class Conversations(Stream):
    stream_id = 'conversations'
    stream_name = 'conversations'
    endpoint = 'conversations'
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []

    def sync(self):
        records = self.client.get(self.endpoint, params={})
        for rec in records.get('results'):
            yield rec


class Groups(Stream):
    stream_id = 'groups'
    stream_name = 'groups'
    endpoint = 'groups'
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []

    def sync(self):
        records = self.client.get(self.endpoint, params={})
        for rec in records.get('results'):
            yield rec


class Roles(Stream):
    stream_id = 'roles'
    stream_name = 'roles'
    endpoint = 'roles'
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []


    def sync(self):
        records = self.client.get(self.endpoint, params={})
        for rec in records.get('results'):
            yield rec


class Tickets(Stream):
    stream_id = 'tickets'
    stream_name = 'tickets'
    endpoint = 'tickets'
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []

    def sync(self):
        records = self.client.get(self.endpoint, params={})
        for rec in records.get('results'):
            yield rec


class SatisfactionRatings(Stream):
    stream_id = 'satisfaction_ratings'
    stream_name = 'satisfaction_ratings'
    endpoint = 'satisfaction_ratings'
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []

    def sync(self):
        records = self.client.get(self.endpoint, params={})
        for rec in records.get('results'):
            yield rec


class TimeEntries(Stream):
    stream_id = 'time_entries'
    stream_name = 'time_entries'
    endpoint = 'time_entries'
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []

    def sync(self):
        records = self.client.get(self.endpoint, params={})
        for rec in records.get('results'):
            yield rec


STREAM_OBJECTS = {
    'agents': Agents,
    'business_hours': BusinessHours,
    'companies': Companies,
    'contacts': Contacts,
    'conversations': Conversations,
    'groups': Groups,
    'roles': Roles,
    'satisfaction_ratings': SatisfactionRatings,
    'tickets': Tickets,
    'time_entries': TimeEntries,
}
