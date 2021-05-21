#!/usr/bin/env python3
# Calls to the api are made here.

import sys
import json
import time
import singer
import backoff
import requests
from tap_freshdesk import helper

logger = singer.get_logger()

ENDPOINT_BASE = "https://{}.freshdesk.com/api/v2/"


# catch all errors and print exception raised
class FreshdeskError(Exception):
    pass


class FreshdeskClient():
    def __init__(self, config_path, config):
        self.config_path = config_path
        self.config = config
        self.session = requests.Session()
        try:
            # Make an authenticated request after creating the object to any endpoint
            tickets = self.get('tickets', {}, config)  # .get('results').get('id')
            self.tickets = tickets
        except Exception as e:
            logger.info("Error initializing FreshdeskClient during token refresh, please reauthenticate.")
            raise FreshdeskError(e)

    @backoff.on_exception(backoff.expo,
                          requests.exceptions.RequestException,
                          max_tries=5,
                          giveup=lambda e: e.response is not None and 400 <= e.response.status_code < 500,
                          factor=2)
    @helper.ratelimit(1, 2)
    def _make_request(self, method, endpoint, headers=None, params=None, data=None):
        params = params or {}
        headers = {}
        domain = False

        if "domain" in params:
            domain = params.get("domain", False)

        full_url = ENDPOINT_BASE.format(domain) + endpoint
        logger.info(
            "%s - Making request to %s endpoint %s, with params %s",
            full_url,
            method.upper(),
            endpoint,
            params,
        )

        if 'user_agent' in params:
            headers['User-Agent'] = params['user_agent']

        try:
            # TODO: params should be other than configuration data
            req = requests.Request('GET', full_url, params={}, auth=(params['api_key'], ""),
                                   headers=headers).prepare()
            logger.info("GET {}".format(req.url))
            resp = self.session.send(req)
            if 'Retry-After' in resp.headers:
                retry_after = int(resp.headers['Retry-After'])
                logger.info("Rate limit reached. Sleeping for {} seconds".format(retry_after))
                time.sleep(retry_after)
                return self._make_request(method, endpoint, headers, params, data)

            resp.raise_for_status()
            return resp.json()

        except Exception as e:
            print("Exception raised!", e)
            raise Exception("EXCEPTION RAISED: ", e)

    def get(self, url, headers=None, params=None):
        return self._make_request("GET", url, headers=headers, params=params)
