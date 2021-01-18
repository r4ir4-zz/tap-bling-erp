from datetime import timedelta, datetime, timezone
import singer

LOGGER = singer.get_logger()
BOOKMARK_DATE_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'

class Stream:
    def __init__(self, client):
        self.client = client


class Orders(Stream):
    tap_stream_id = 'orders'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    valid_replication_keys = ['updated_at']
    replication_key = 'updated_at'

    # To Sync orders it'll only be used date-time values as UTC 0.
    def sync(self, state, stream_schema, stream_metadata, config, transformer):
        # Bookmark is in timezone UTC
        start_time_str = singer.get_bookmark(
            state,
            self.tap_stream_id,
            self.replication_key,
            config['start_date'])
        # add timezone UTC 0 without changing the date time
        start_time = datetime.strptime(start_time_str,BOOKMARK_DATE_FORMAT).\
            replace(tzinfo=timezone.utc)
        
        max_record_value = start_time     
        
        # building the request parameters, truncating the date time to date
        extraction_time = singer.utils.now()
        start_date = datetime.strftime(start_time,'%Y%m%d')
        finish_date = datetime.strftime(extraction_time,'%Y%m%d')
        
        # get orders from API and iterate over results
        for record in self.client.get_orders(start_date,finish_date):
            
            transformed_record = transformer.transform(record, stream_schema, stream_metadata)

            # as the transformed_record returns any datetime field as a str in the format of "%04Y-%m-%dT%H:%M:%S.%fZ", it's necessary to convert it to datetime for comparisons.
            # replace method is used because data was already converted to UTC 0 timezone, so we cannot change it again
            updated_at = datetime.strptime(
                transformed_record[self.replication_key],
                BOOKMARK_DATE_FORMAT).replace(tzinfo=timezone.utc)

            # ignore records already imported in previous sync. This happens because the API request filter is date and not datetime.
            if updated_at > start_time:
                singer.write_record(self.tap_stream_id,transformed_record,time_extracted=extraction_time)

                if updated_at > max_record_value:
                    max_record_value = updated_at

        # Convert to bookmark format
        max_record_value = datetime.strftime(max_record_value,BOOKMARK_DATE_FORMAT)
        state = singer.write_bookmark(state, self.tap_stream_id, self.replication_key, max_record_value)
        singer.write_state(state)
        
        return state


STREAMS = {
    'orders': Orders
}