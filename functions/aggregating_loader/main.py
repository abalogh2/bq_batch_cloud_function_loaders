import logging
import os

from google.cloud import bigquery
from google.cloud.bigquery import TimePartitioning, CreateDisposition, WriteDisposition, QueryJobConfig, \
    ExternalConfig, ExternalSourceFormat

PROJECT_ID = os.getenv('GCP_PROJECT')
CLIENT = bigquery.Client(PROJECT_ID)
DATASET = 'sales_data_aggregated'
TABLE = 'customer_123'
JOB_ID_PREFIX = 'aggregating_loader_'
SCHEMA = CLIENT.schema_from_json('sales_data_schema.json')
CLUSTERING_FIELDS = ['contact_id']
QUERY_TEMPLATE_FILE = 'loader_template.sql'
EXTERNAL_TABLE_NAME_IN_QUERY = 'sales_data_external_table'
COMPRESSION = 'GZIP'


def load_from_gcs(event, context):
    try:
        uri = f"gs://{event['bucket']}/{event['name']}"
        destination_table = CLIENT.dataset(DATASET).table(TABLE)
        job_config = create_job_config(uri, destination_table)
        query_template = load_query_template()
        query = query_template.format(EXTERNAL_TABLE_NAME=EXTERNAL_TABLE_NAME_IN_QUERY)

        job = CLIENT.query(query=query,
                           job_config=job_config,
                           job_id_prefix=JOB_ID_PREFIX)

        logging.info(f"Started job {job.job_id}")
        job.result()
        logging.info(f"Processed {uri}")
    except Exception as err:
        logging.error(f"Error happened during processing of {uri}: {str(err)}")


def load_query_template():
    with open(QUERY_TEMPLATE_FILE) as query_template_file:
        query_template = query_template_file.read()
    return query_template


def create_job_config(external_source_uri, destination_table):
    config = QueryJobConfig()
    config.create_disposition = CreateDisposition.CREATE_IF_NEEDED
    config.write_disposition = WriteDisposition.WRITE_APPEND
    config.clustering_fields = CLUSTERING_FIELDS
    config.time_partitioning = TimePartitioning()
    config.destination = destination_table

    external_config = ExternalConfig(ExternalSourceFormat.CSV)
    external_config.schema = SCHEMA
    external_config.source_uris = external_source_uri
    external_config.compression = COMPRESSION
    external_config.options.skip_leading_rows = 1

    config.table_definitions = {EXTERNAL_TABLE_NAME_IN_QUERY: external_config}
    return config
