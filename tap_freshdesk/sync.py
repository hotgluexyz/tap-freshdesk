import backoff
import sys
import time
import requests
# This is where the sync function is run
import singer
from requests.exceptions import HTTPError
from singer import Transformer, metadata

from tap_freshdesk import helper
from .streams import STREAM_OBJECTS

logger = singer.get_logger()
session = requests.Session()

BASE_URL = "https://{}.freshdesk.com"
CONFIG = {}
STATE = {}


def get_start(entity):
    if entity not in STATE:
        STATE[entity] = CONFIG.get('start_date', False)
    return STATE[entity]


def sync(client, config: dict, state: dict, catalog: singer.Catalog):
    logger.info("Starting FreshDesk sync")
    for stream in catalog.streams:
        stream_id = stream.tap_stream_id
        stream_schema = stream.schema
        stream_object = STREAM_OBJECTS.get(stream_id)(client, config, state)
        schema = stream_schema.to_dict()

        if stream_object is None:
            raise Exception("Attempted to sync unknown stream {}".format(stream_id))

        singer.write_schema(
            stream_id,
            schema,
            stream_object.key_properties,
            stream_object.replication_keys,
        )

        start = get_start(stream_id)
        logger.info("Syncing stream {} from {}".format(stream_id, start))

        with Transformer() as transformer:
            for rec in stream_object.sync(start):
                # update custom fields from data retrieved
                custom_fields = rec.get('custom_fields', False)
                if custom_fields:
                    rec.update(custom_fields)
                    rec.pop('custom_fields')

                singer.write_record(
                    stream_id,
                    transformer.transform(rec,
                                          schema,
                                          metadata.to_map(stream.metadata))
                )
