import singer
from singer import utils
from singer.catalog import Catalog, write_catalog
from tap_freshdesk.discover import discover
from tap_freshdesk.client import FreshdeskClient
from tap_freshdesk.sync import sync, CONFIG, STATE
from tap_freshdesk.helper import parse_args

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = ['api_key', 'domain', 'start_date']


@utils.handle_top_exception(LOGGER)
def main():
    required_config_keys = ['start_date']
    args = singer.parse_args(required_config_keys)

    config = args.config
    freshdesk_client = FreshdeskClient(args.config_path, config)
    catalog = args.catalog or Catalog([])
    state = args.state

    if args.properties and not args.catalog:
        raise Exception("DEPRECATED: Use of the 'properties' parameter is not supported. Please use --catalog instead")

    if args.discover:
        LOGGER.info("Starting discovery mode")
        catalog = discover(freshdesk_client)
        write_catalog(catalog)
    else:
        LOGGER.info("Starting sync mode")

        config, state = parse_args(REQUIRED_CONFIG_KEYS)
        CONFIG.update(config)
        STATE.update(state)

        sync(freshdesk_client, config, state, catalog)


if __name__ == "__main__":
    main()
