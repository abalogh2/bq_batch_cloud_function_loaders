import logging
import os

from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig, TimePartitioning, CreateDisposition, WriteDisposition, SchemaField

PROJECT_ID = os.getenv('GCP_PROJECT')
CLIENT = bigquery.Client(PROJECT_ID)
DATASET = 'sales_data'
TABLE = 'customer_123'
JOB_ID_PREFIX = 'simple_loader_'
CLUSTERING_FIELDS = ['user_id']
SCHEMA_DICT = [
    {"mode": "NULLABLE", "name": "user_id", "type": "INTEGER"},
    {"mode": "NULLABLE", "name": "event_time", "type": "TIMESTAMP"},
    {"mode": "NULLABLE", "name": "order_id", "type": "INTEGER"},
    {"mode": "NULLABLE", "name": "item_id", "type": "INTEGER"},
    {"mode": "NULLABLE", "name": "quantity", "type": "FLOAT"},
    {"mode": "NULLABLE", "name": "price", "type": "FLOAT"}
]
LOCATION = 'EU'


def load_from_gcs(event, context):
    try:
        uri = f"gs://{event['bucket']}/{event['name']}"
        destination_table = CLIENT.dataset(DATASET).table(TABLE)
        job_config = create_job_config()

        job = CLIENT.load_table_from_uri(source_uris=uri,
                                         destination=destination_table,
                                         job_id_prefix=JOB_ID_PREFIX,
                                         job_config=job_config,
                                         location=LOCATION)

        logging.info(f"Started job {job.job_id}")
        job.result()
        logging.info(f"Processed {uri}")
    except Exception as err:
        logging.error(f"Error happened during processing of {uri}: {str(err)}")


def create_schema_for_table():
    return [SchemaField.from_api_repr(field) for field in SCHEMA_DICT]


def create_job_config():
    config = LoadJobConfig()
    config.skip_leading_rows = 1
    config.time_partitioning = TimePartitioning()
    config.clustering_fields = CLUSTERING_FIELDS
    config.schema = create_schema_for_table()
    config.create_disposition = CreateDisposition.CREATE_IF_NEEDED
    config.write_disposition = WriteDisposition.WRITE_APPEND
    return config
