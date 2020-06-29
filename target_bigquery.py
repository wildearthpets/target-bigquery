#!/usr/bin/env python3

import argparse
import collections
import decimal
import http.client
import io
import json
import simplejson
import logging
import os
import re
import sys
import threading
import urllib
from tempfile import TemporaryFile

import pkg_resources
import singer
from google.api_core import exceptions
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage
from google.cloud.bigquery import Dataset, WriteDisposition
from google.cloud.bigquery import LoadJobConfig
from google.cloud.bigquery import SchemaField
from google.cloud.bigquery.job import SourceFormat
from google.oauth2 import service_account
from jsonschema import validate
from oauth2client import tools

try:
    parser = argparse.ArgumentParser(parents=[tools.argparser])
    parser.add_argument('-c', '--config', help='Config file', required=True)
    flags = parser.parse_args()

except ImportError:
    flags = None

with open(flags.config) as input:
    config = json.load(input)

logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)
logger = singer.get_logger()

SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]
CLIENT_SECRET_FILE = config.get('client_secrets') or '.credentials/gcloud.json'
APPLICATION_NAME = 'Singer BigQuery Target'

PROJECT_ID = config.get('project_id')
DATASET_ID = config.get('dataset_id')

STATE_BUCKET = config.get('gcs_state_bucket')
STATE_BLOB = config.get('gcs_state_blob')

StreamMeta = collections.namedtuple('StreamMeta', ['schema', 'key_properties', 'bookmark_properties'])


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


credentials = service_account.Credentials.from_service_account_file(get_abs_path(CLIENT_SECRET_FILE), scopes=SCOPES)
STORAGE_CLIENT = storage.Client(credentials=credentials, project=PROJECT_ID)

BIGQUERY_CLIENT = bigquery.Client(credentials=credentials, project=PROJECT_ID)


def sync_state_for_table(table_name, new_state):
    sync_state = {'currently_syncing': None, 'bookmarks': {}}

    bucket = STORAGE_CLIENT.bucket(STATE_BUCKET)
    blob = bucket.blob(STATE_BLOB)
    try:
        old_state = json.loads(blob.download_as_string())
    except NotFound:
        old_state = {}

    sync_state['currently_syncing'] = new_state['currently_syncing']
    for key in new_state['bookmarks']:
        if key == table_name:
            sync_state['bookmarks'][key] = new_state['bookmarks'][key]
        else:
            sync_state['bookmarks'][key] = old_state.get('bookmarks', {}).get(key, None)

    blob.upload_from_string(json.dumps(sync_state))


def sync_state(state):
    bucket = STORAGE_CLIENT.bucket(STATE_BUCKET)
    blob = bucket.blob(STATE_BLOB)
    blob.upload_from_string(json.dumps(state))


def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()


def clear_dict_hook(items):
    return {k: v if v is not None else '' for k, v in items}


def fix_name(name):
    name = re.sub('[^a-zA-Z0-9_]', '_', name)
    if name[0].isdigit():
        name = "_" + name
    if len(name) > 128:
        name = name[:128]
    return re.sub('[^a-zA-Z0-9_]', '_', name)


def define_schema(field, name):
    schema_name = fix_name(name)
    schema_type = "STRING"
    schema_mode = "NULLABLE"
    schema_description = None
    schema_fields = ()

    if 'type' not in field and 'anyOf' in field:
        for types in field['anyOf']:
            if types['type'] == 'null':
                schema_mode = 'NULLABLE'
            else:
                field = types

    if isinstance(field['type'], list):
        if field['type'][0] == "null":
            schema_mode = 'NULLABLE'
        else:
            schema_mode = 'required'
        schema_type = field['type'][-1]
    else:
        schema_type = field['type']
    if schema_type == "object":
        if "patternProperties" in field.keys() or "properties" not in field.keys():
            schema_type = "string"
        else:
            schema_type = "RECORD"
            schema_fields = tuple(build_schema(field))
    if schema_type == "array":
        items_type = field.get('items').get('type')
        schema_type = items_type[-1] if isinstance(items_type, list) else items_type
        schema_mode = "REPEATED"
        if schema_type == "object":
            schema_type = "RECORD"
            schema_fields = tuple(build_schema(field.get('items')))

    if schema_type == "string":
        if "format" in field:
            if field['format'] == "date-time":
                schema_type = "timestamp"

    if schema_type == 'number':
        schema_type = 'FLOAT'

    return schema_name, schema_type, schema_mode, schema_description, schema_fields


def build_schema(schema):
    SCHEMA = []

    # if "properties" not in schema:
    #     print(schema)

    for key in schema['properties'].keys():

        if not (bool(schema['properties'][key])):
            # if we endup with an empty record.
            continue

        schema_name, schema_type, schema_mode, schema_description, schema_fields = define_schema(
            schema['properties'][key], key)
        SCHEMA.append(SchemaField(schema_name, schema_type, schema_mode, schema_description, schema_fields))

    return SCHEMA


def apply_string_conversions(record, schema):
    tmp_record = {}
    for schema_field in schema:
        rec_field = record.get(schema_field.name)
        if rec_field:
            if schema_field.field_type.upper() == "STRING":
                if schema_field.mode == "REPEATED":
                    tmp_record[schema_field.name] = [str(rec_item) for rec_item in rec_field]
                else:
                    tmp_record[schema_field.name] = str(rec_field)
            elif schema_field.field_type.upper() in ["RECORD", "STRUCT"]:
                if schema_field.mode == "REPEATED":
                    tmp_record[schema_field.name] = [apply_string_conversions(rec_item, schema_field.fields) for
                                                     rec_item in
                                                     rec_field]
                else:
                    tmp_record[schema_field.name] = apply_string_conversions(rec_field, schema_field.fields)
            else:
                tmp_record[schema_field.name] = rec_field
    return tmp_record


def apply_decimal_conversions(record):
    new_record = {}
    for key, value in record.items():
        if isinstance(value, decimal.Decimal):
            value = float(value)
        new_record[fix_name(key)] = value
    new_record = simplejson.loads(simplejson.dumps(new_record, use_decimal=True))
    return new_record


def persist_lines_job(lines=None, truncate=False, validate_records=True):
    state = None
    schemas = {}
    key_properties = {}
    bq_schemas = {}
    rows = {}
    errors = {}

    # try:
    #     dataset = BIGQUERY_CLIENT.create_dataset(Dataset(dataset_ref)) or Dataset(dataset_ref)
    # except exceptions.Conflict:
    #     pass

    for line in lines:
        try:
            msg = singer.parse_message(line)
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(line))
            raise

        if isinstance(msg, singer.RecordMessage):
            if msg.stream not in schemas:
                raise Exception(
                    "A record for stream {} was encountered before a corresponding schema".format(msg.stream))

            schema = schemas[msg.stream]

            if validate_records:
                validate(msg.record, schema)

            msg.record = apply_string_conversions(msg.record, bq_schemas[msg.stream])
            # NEWLINE_DELIMITED_JSON expects literal JSON formatted data, with a newline character splitting each row.
            new_record = apply_decimal_conversions(msg.record)
            dat = bytes(json.dumps(new_record) + '\n', 'UTF-8')

            rows[msg.stream].write(dat)
            # rows[msg.stream].write(bytes(str(msg.record) + '\n', 'UTF-8'))

            state = None

        elif isinstance(msg, singer.StateMessage):
            logger.debug('Setting state to {}'.format(msg.value))
            state = msg.value

        elif isinstance(msg, singer.SchemaMessage):
            table = msg.stream
            schemas[table] = msg.schema
            key_properties[table] = msg.key_properties
            bq_schemas[table] = build_schema(schemas[table])
            rows[table] = TemporaryFile(mode='w+b')
            errors[table] = None
            # try:
            #     tables[table] = BIGQUERY_CLIENT.create_table(tables[table])
            # except exceptions.Conflict:
            #     pass

        elif isinstance(msg, singer.ActivateVersionMessage):
            # This is experimental and won't be used yet
            pass

        else:
            raise Exception("Unrecognized message {}".format(msg))

    for table in rows.keys():
        table_ref = BIGQUERY_CLIENT.dataset(DATASET_ID).table(fix_name(table))
        load_config = LoadJobConfig()
        load_config.schema = bq_schemas[table]
        load_config.source_format = SourceFormat.NEWLINE_DELIMITED_JSON

        if truncate:
            load_config.write_disposition = WriteDisposition.WRITE_TRUNCATE

        rows[table].seek(0, os.SEEK_END)
        if rows[table].tell() == 0:
            continue
        rows[table].seek(0)
        logger.info("loading {} to Bigquery.\n".format(table))
        load_job = BIGQUERY_CLIENT.load_table_from_file(
            rows[table], table_ref, job_config=load_config)
        logger.info("loading job {}".format(load_job.job_id))
        logger.info(load_job.result())
        sync_state_for_table(table, state)

    # for table in errors.keys():
    #     if not errors[table]:
    #         print('Loaded {} row(s) into {}:{}'.format(rows[table], DATASET_ID, table), tables[table].path)
    #     else:
    #         print('Errors:', errors[table], sep=" ")

    return state


def persist_lines_stream(lines=None, validate_records=True):
    state = None
    schemas = {}
    key_properties = {}
    tables = {}
    rows = {}
    errors = {}

    dataset_ref = BIGQUERY_CLIENT.dataset(DATASET_ID)
    dataset = Dataset(dataset_ref)
    try:
        dataset = BIGQUERY_CLIENT.create_dataset(Dataset(dataset_ref)) or Dataset(dataset_ref)
    except exceptions.Conflict:
        pass

    for line in lines:
        try:
            msg = singer.parse_message(line)
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(line))
            raise

        if isinstance(msg, singer.RecordMessage):
            if msg.stream not in schemas:
                raise Exception(
                    "A record for stream {} was encountered before a corresponding schema".format(msg.stream))

            schema = schemas[msg.stream]

            if validate_records:
                validate(msg.record, schema)

            msg.record = apply_string_conversions(msg.record, tables[msg.stream].schema)
            msg.record = apply_decimal_conversions(msg.record)
            errors[msg.stream] = BIGQUERY_CLIENT.insert_rows_json(tables[msg.stream], [msg.record])
            rows[msg.stream] += 1

            state = None

        elif isinstance(msg, singer.StateMessage):
            logger.debug('Setting state to {}'.format(msg.value))
            state = msg.value
            sync_state(state)
            emit_state(state)

        elif isinstance(msg, singer.SchemaMessage):
            table = msg.stream
            schemas[table] = msg.schema
            key_properties[table] = msg.key_properties
            tables[table] = bigquery.Table(dataset.table(table), schema=build_schema(schemas[table]))
            rows[table] = 0
            errors[table] = None
            try:
                tables[table] = BIGQUERY_CLIENT.create_table(tables[table])
            except exceptions.Conflict:
                pass

        elif isinstance(msg, singer.ActivateVersionMessage):
            # This is experimental and won't be used yet
            pass

        else:
            raise Exception("Unrecognized message {}".format(msg))

    for table in errors.keys():
        if not errors[table]:
            logging.info('Loaded {} row(s) into {}:{}'.format(rows[table], DATASET_ID, table, tables[table].path))
            emit_state(state)
        else:
            logging.error('Errors:', str(errors[table]))

    return state


def collect():
    try:
        version = pkg_resources.get_distribution('target-bigquery').version
        conn = http.client.HTTPConnection('collector.singer.io', timeout=10)
        conn.connect()
        params = {
            'e': 'se',
            'aid': 'singer',
            'se_ca': 'target-bigquery',
            'se_ac': 'open',
            'se_la': version,
        }
        conn.request('GET', '/i?' + urllib.parse.urlencode(params))
        conn.getresponse()
        conn.close()
    except:
        logger.debug('Collection request failed')


def main():
    if not config.get('disable_collection', False):
        logger.info('Sending version information to stitchdata.com. ' +
                    'To disable sending anonymous usage data, set ' +
                    'the config parameter "disable_collection" to true')
        threading.Thread(target=collect).start()

    if config.get('replication_method') == 'FULL_TABLE':
        truncate = True
    else:
        truncate = False

    validate_records = config.get('validate_records', True)

    input = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')

    if config.get('stream_data', True):
        state = persist_lines_stream(input, validate_records=validate_records)
    else:
        state = persist_lines_job(input, truncate=truncate, validate_records=validate_records)

    emit_state(state)
    # if flags.state and state:
    #     with open(flags.state, 'w') as f:
    #         f.write(json.dumps(state))
    logger.debug("Exiting normally")


if __name__ == '__main__':
    main()
