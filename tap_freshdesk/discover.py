# Discovery code is here.
import os
import json

from singer import metadata
from singer.catalog import Catalog
from .streams import STREAM_OBJECTS
from .helper import map_type


def _get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


# Load schemas from schemas folder
def _load_schemas():
    schemas = {}
    for filename in os.listdir(_get_abs_path("schemas")):
        path = _get_abs_path("schemas") + "/" + filename
        file_raw = filename.replace(".json", "")
        with open(path) as file:
            schemas[file_raw] = json.load(file)
    return schemas


def discover(client):
    # discover catalog schema
    raw_schemas = _load_schemas()
    catalog_entries = []

    for stream_name, schema in raw_schemas.items():
        # create and add catalog entry
        stream = STREAM_OBJECTS[stream_name]

        # Add custom fields
        if stream.custom_fields:
            response = client._make_request('GET', stream.custom_fields)

            for field in response:
                field_name = field.get('name', False)
                if field.get('default', False):
                    continue
                # add mapping ex. custom_number -> number
                field_type = field.get('type', False)
                schema["properties"][field_name] = map_type(field_type)
                if field_type == 'nested_field':
                    for nested_field in field.get('nested_ticket_fields', []):
                        schema["properties"][nested_field['name']] = map_type(field_type)
            # remove custom_fields parent as they are added directly in schema
            schema["properties"].pop('custom_fields')

        catalog_entry = {
            "stream": stream_name,
            "tap_stream_id": stream_name,
            "schema": schema,
            "metadata": metadata.get_standard_metadata(
                schema=schema,
                key_properties=stream.key_properties,
                valid_replication_keys=stream.replication_keys,
                replication_method=stream.replication_method,
            ),
            "key_properties": stream.key_properties
        }
        catalog_entries.append(catalog_entry)

    return Catalog.from_dict({"streams": catalog_entries})
