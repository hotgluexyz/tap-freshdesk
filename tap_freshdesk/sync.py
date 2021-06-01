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

PER_PAGE = 100
BASE_URL = "https://{}.freshdesk.com"
CONFIG = {}
STATE = {}

endpoints = {
    "tickets": "/api/v2/tickets",
    "sub_ticket": "/api/v2/tickets/{id}/{entity}",
    "agents": "/api/v2/agents",
    "roles": "/api/v2/roles",
    "groups": "/api/v2/groups",
    "companies": "/api/v2/companies",
    "contacts": "/api/v2/contacts"
}


def get_url(endpoint, **kwargs):
    return BASE_URL.format(CONFIG.get('domain', False)) + endpoints[endpoint].format(**kwargs)


@backoff.on_exception(backoff.expo,
                      requests.exceptions.RequestException,
                      max_tries=5,
                      giveup=lambda e: e.response is not None and 400 <= e.response.status_code < 500,
                      factor=2)
@helper.ratelimit(1, 2)
def request(url, params=None):
    params = params or {}
    params["per_page"] = PER_PAGE
    page = 1
    headers = {}
    if 'user_agent' in CONFIG:
        headers['User-Agent'] = CONFIG['user_agent']

    while True:
        params['page'] = page
        data = requests.Request('GET', url, params=params, auth=(CONFIG.get('api_key', False), ""),
                                headers=headers).prepare()
        logger.info("GET {}".format(data.url))
        resp = session.send(data)
        if 'Retry-After' in resp.headers:
            retry_after = int(resp.headers['Retry-After'])
            logger.info("Rate limit reached. Sleeping for {} seconds".format(retry_after))
            time.sleep(retry_after)
            return request(url, params)

        resp.raise_for_status()
        for row in data:
            yield row

        if len(data) == PER_PAGE:
            page += 1
        else:
            break

        return resp
    return


def get_start(entity):
    if entity not in STATE:
        STATE[entity] = CONFIG.get('start_date', False)
    return STATE[entity]


def sync(client, config: dict, state: dict, catalog: singer.Catalog):
    logger.info("Starting FreshDesk sync")
    selected_streams = catalog.get_selected_streams(state)
    for stream in selected_streams:
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
