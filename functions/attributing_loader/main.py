import logging
import os

from google.cloud import bigquery
from google.cloud.bigquery import TimePartitioning, CreateDisposition, WriteDisposition, QueryJobConfig, \
    ExternalConfig, ExternalSourceFormat


PROJECT_ID = os.getenv('GCP_PROJECT')
SQL_VERSION = os.getenv('SQL_VERSION')
CLIENT = bigquery.Client(PROJECT_ID)
DATASET = 'sales_data_attributed'
TABLE = 'customer_123'
CLUSTERING_FIELDS = ['user_id']
JOB_ID_PREFIX = 'attributed_loader_'
SCHEMA = CLIENT.schema_from_json('sales_data_schema.json')
EXTERNAL_TABLE_NAME_IN_QUERY = 'sales_data_external_table'
QUERY_TEMPLATE_FILE = f'loader_template{SQL_VERSION}.sql'
COMPRESSION = 'GZIP'
LOCATION = 'EU'


def load_from_gcs(event, context):
    try:
        uri = f"gs://{event['bucket']}/{event['name']}"
        job_config = create_job_config(uri)
        query_template = load_query_template()
        query = query_template.format(EXTERNAL_TABLE_NAME=EXTERNAL_TABLE_NAME_IN_QUERY)

        job = CLIENT.query(query=query,
                           job_config=job_config,
                           job_id_prefix=JOB_ID_PREFIX,
                           location=LOCATION)

        logging.info(f"Started job {job.job_id}")
        job.result()
        logging.info(f"Processed {uri}")
    except Exception as err:
        logging.error(f"Error happened during processing of {uri}: {str(err)}")


def load_query_template():
    with open(QUERY_TEMPLATE_FILE) as query_template_file:
        query_template = query_template_file.read()
    return query_template


def create_job_config(external_source_uri):
    config = QueryJobConfig()
    if SQL_VERSION == '1':
        config.create_disposition = CreateDisposition.CREATE_IF_NEEDED
        config.write_disposition = WriteDisposition.WRITE_APPEND
        config.clustering_fields = CLUSTERING_FIELDS
        config.time_partitioning = TimePartitioning()
        config.destination = CLIENT.dataset(DATASET).table(TABLE)

    external_config = ExternalConfig(ExternalSourceFormat.CSV)
    external_config.schema = SCHEMA
    external_config.source_uris = external_source_uri
    external_config.compression = COMPRESSION
    external_config.options.skip_leading_rows = 1

    config.table_definitions = {EXTERNAL_TABLE_NAME_IN_QUERY: external_config}
    return config
