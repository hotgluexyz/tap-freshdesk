#!/usr/bin/env python3
# Calls to the api are made here.
import time
import singer
import backoff
import requests
from tap_freshdesk import helper
from dateutil.relativedelta import relativedelta

logger = singer.get_logger()

ENDPOINT_BASE = "https://{}.freshdesk.com/api/v2/"
PER_PAGE = 100
PAGE_LIMIT = 300


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

        updated_at = params.get('updated_since', False) or params.get('_updated_since', )
        full_url = ENDPOINT_BASE.format(domain) + endpoint
        logger.info(
            "%s - Making request to %s endpoint %s, with params %s",
            full_url,
            method.upper(),
            endpoint,
            params,
        )

        try:

            while True:
                # if page is at its limit, reset page and update search param with updated_at from data's last record
                if page > PAGE_LIMIT:
                    logger.info("Reset pagination.")
                    params['reset_pagination'] = True
                    if params.get('updated_since', False):
                        params['updated_since'] = updated_at
                    if params.get('_updated_since', False):
                        params['_updated_since'] = updated_at
                    break

                params['page'] = page
                resp = self._make_request_internal(full_url, params, api_key, headers)
                resp.raise_for_status()

                response_data = resp.json()
                # get the last record fetched (ordered asc by updated date)
                last_record = response_data and response_data[-1]
                if last_record and 'updated_at' in last_record:
                    # add one sec to searching date in order to not got the same records twice
                    updated_at = helper.strptime(last_record['updated_at']) + relativedelta(seconds=1)
                    updated_at = helper.strftime(updated_at)
                yield response_data

                if len(resp.json()) == PER_PAGE:
                    page += 1
                else:
                    # no data in the next page
                    break

        except Exception as e:
            raise Exception("EXCEPTION RAISED: ", e)

    def get(self, url, headers=None, params=None):
        yield from self._make_request("GET", url, headers=headers, params=params)
