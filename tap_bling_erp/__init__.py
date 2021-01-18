#!/usr/bin/env python3
import os
import json
import singer
import requests
from datetime import datetime
from singer import Transformer, utils, metadata
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema


REQUIRED_CONFIG_KEYS = ["start_date", "api_token"]
LOGGER = singer.get_logger()

ENDPOINTS = {
    "orders": "orders",
    "products": "products",
	"clients": "clients"
}

API_VERSION = 'v2'

BOOKMARK_DATE_FORMAT = '%Y-%m-%dT%H:%M:%SZ'
TIME_EXTRACTED_FORMAT = '%Y-%m-%dT%H:%M:%S%z'


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """ Load schemas from schemas folder """
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = Schema.from_dict(json.load(file))
    return schemas


def discover():
    raw_schemas = load_schemas()
    streams = []
    # TODO: Review this, I made a quick copy paste
    stream_metadata = {
        'orders': [{
            "breadcrumb": [],
            "metadata": {
                "selected": "false",
                "replication-method": "INCREMENTAL",
                "replication-key": "updated_at"
            }
        }],
        'products': [{
            "breadcrumb": [],
            "metadata": {
                "selected": "true",
                "replication-method": "INCREMENTAL",
                "replication-key": "updated_at"
            }
        }]
        ,
        'clients': [{
            "breadcrumb": [],
            "metadata": {
                "selected": "true",
                "replication-method": "INCREMENTAL",
                "replication-key": "updated_at"
            }
        }]
    }
    for stream_id, schema in raw_schemas.items():
        
        this_stream_metadata = stream_metadata[stream_id]
        # LOGGER.info(this_stream_metadata)
        key_properties = []
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=key_properties,
                metadata=this_stream_metadata,
                is_view=None,
                database=None,
                table=None,
                row_count=None,
                stream_alias=None,
                replication_method=this_stream_metadata[0]["metadata"]["replication-method"],
                replication_key=this_stream_metadata[0]["metadata"]["replication-key"],
            )
        )
    return Catalog(streams)

# TODO: This part needs to be changed to collect data incrementally and not hard-coded
def get_api_data(stream,config,bookmark_column):
    url = "/".join([
        config['api_url'],
        API_VERSION,
        ENDPOINTS[stream.tap_stream_id]])
    # TODO: This part was done as quick way to run, it can be re-done
    REQ_PARAMS = { 
        'orders': {'start':'20200801','finish':'20200801'},
        'products': {},
        'clients': {}
    }
    params = REQ_PARAMS[stream.tap_stream_id]
    headers = {'Authorization': "Token \"{}\"".format(config['api_token'])}
    
    page = 1
    api_data = []
    response = [1]
    # iterate through VNDA pages
    while response:
        params['page'] = page
        req = requests.get(url=url, params=params, headers=headers)
        response = req.json()
        LOGGER.info(page)
        api_data.extend(req.json())
        page = page + 1

    
    return api_data


def sync(config, state, catalog):
    """ Sync data from tap source """
    # Loop over selected streams in catalog
    # LOGGER.info('init_func')
    # LOGGER.info(catalog.get_selected_streams(state))
    with Transformer() as transformer:
        for stream in catalog.get_selected_streams(state):
            
            LOGGER.info("Syncing stream:" + stream.tap_stream_id)

            
            is_sorted = False
            LOGGER.info("KEY PROP: {}".format(stream.key_properties))
            singer.write_schema(
                stream_name=stream.tap_stream_id,
                schema=stream.schema.to_dict(),
                key_properties=stream.key_properties,
            )
            
            
            now = singer.utils.now()
            bookmark_column = stream.replication_key
            
            # TODO: It's need to use the bookmark to do incremental sync
            # LOGGER.info(bookmark_column)
            # start_date = date_trunc(start_time)
            # end_date = date_trunc(now)
            # if record[bookmark_column] <= bookmark then skip, else write rec

            # LOGGER.info('after_api')
            # LOGGER.info(type(now))
            # LOGGER.info(now)
            # LOGGER.info(type(start_time))
            # LOGGER.info(start_time)

            start_time_str = singer.get_bookmark(state, stream.tap_stream_id, bookmark_column, config['start_date'])
            start_time = datetime.strptime(start_time_str,'%Y-%m-%dT%H:%M:%SZ').astimezone()
            
            tap_data = get_api_data(stream,config,bookmark_column)

            max_bookmark = None
            for record in tap_data:
                # TODO: place type conversions or transformations here
                transformed_record = transformer.transform(
                                record, stream.schema.to_dict(), metadata.to_map(stream.metadata),
                            )
                
                # write one or more rows to the stream:    
                singer.write_records(stream.tap_stream_id, [transformed_record])
            
                
                if bookmark_column:
                    if is_sorted:
                        # update bookmark to latest value
                        singer.write_state({stream.tap_stream_id: transformed_record[bookmark_column]})
                    else:
                        # if data unsorted, save max value until end of writes
                        record_bookmark = datetime.strptime(transformed_record[bookmark_column],'%Y-%m-%dT%H:%M:%S.%fZ')
                        # LOGGER.info(max_bookmark)
                        # LOGGER.info(record_bookmark)
                        if max_bookmark is None:
                            max_bookmark = record_bookmark
                        else:
                            max_bookmark = max(max_bookmark,record_bookmark)

                        
            if bookmark_column and not is_sorted:
                singer.write_state({stream.tap_stream_id: max_bookmark.strftime(BOOKMARK_DATE_FORMAT)})
    
    return


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()
