#!/usr/bin/env python3
# Calls to the api are made here.
import time
from concurrent.futures import ThreadPoolExecutor

import singer
import backoff
import requests
from tap_freshdesk import helper

logger = singer.get_logger()

ENDPOINT_BASE = "https://{}.freshdesk.com/api/v2/"
PER_PAGE = 100


# catch all errors and print exception raised
class FreshdeskError(Exception):
    pass


class RateLimitException(Exception):
    pass


class FreshdeskClient:
    def __init__(self, config_path, config):
        self.config_path = config_path
        self.config = config
        self.session = requests.Session()
        try:
            # Make an authenticated request after creating the object to any endpoint
            tickets = self.get('tickets', {}, {})
            self.tickets = tickets
        except Exception as e:
            logger.info("Error initializing FreshdeskClient, please authenticate.")
            raise FreshdeskError(e)

    @backoff.on_exception(backoff.expo,
                          requests.exceptions.RequestException,
                          max_tries=5,
                          giveup=lambda e: e.response is not None and 400 <= e.response.status_code < 500,
                          factor=2)
    @backoff.on_exception(backoff.expo, RateLimitException)
    @helper.ratelimit(1, 2)
    def _make_request_internal(self, full_url=None, params=None, api_key=None, headers=None):
        req = requests.Request('GET', full_url, params=params, auth=(api_key, ""),
                               headers=headers).prepare()
        logger.info("GET {}".format(req.url))
        resp = self.session.send(req)
        if 'Retry-After' in resp.headers:
            retry_after = int(resp.headers['Retry-After'])
            logger.info("Rate limit reached. Sleeping for {} seconds".format(retry_after))
            time.sleep(retry_after)
            raise RateLimitException()
        return resp

    def _make_request(self, method, endpoint, headers=None, params=None, data=None):
        params = params or {}
        params["per_page"] = PER_PAGE
        headers = headers or {}
        page = 1

        domain = self.config.get("domain", False)
        api_key = self.config.get("api_key", False)
        if not domain:
            raise FreshdeskError("EXCEPTION RAISED: Subdomain not found!")
        if not api_key:
            raise FreshdeskError("EXCEPTION RAISED: API KEY not found!")

        full_url = ENDPOINT_BASE.format(domain) + endpoint
        logger.info(
            "%s - Making request to %s endpoint %s, with params %s",
            full_url,
            method.upper(),
            endpoint,
            params,
        )
        # with ThreadPoolExecutor(max_workers=50) as executor:
        #     for _ in range(500):
        #         future = executor.submit(self._make_request_internal, full_url, params, api_key, headers)
                # print(future.result())

        try:

            while True:
                params['page'] = page

                resp = self._make_request_internal(full_url, params, api_key, headers)
                resp.raise_for_status()

                if len(resp.json()) == PER_PAGE:
                    page += 1
                else:
                    yield resp.json()
                    break

                yield resp.json()

        except Exception as e:
            raise Exception("EXCEPTION RAISED: ", e)

    def get(self, url, headers=None, params=None):
        yield from self._make_request("GET", url, headers=headers, params=params)
