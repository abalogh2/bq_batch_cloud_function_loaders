import logging
import os

from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig, TimePartitioning, CreateDisposition, WriteDisposition

PROJECT_ID = os.getenv('GCP_PROJECT')
CLIENT = bigquery.Client(PROJECT_ID)
DATASET = 'sales_data'
TABLE = 'customer_123'
JOB_ID_PREFIX = 'simple_loader_'
SCHEMA = CLIENT.schema_from_json('sales_data_schema.json')
CLUSTERING_FIELDS = ['contact_id']


def load_from_gcs(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    uri = f"gs://{event['bucket']}/{event['name']}"
    destination_table = CLIENT.dataset(DATASET).table(TABLE)
    job_config = create_job_config()
    try:
        job = CLIENT.load_table_from_uri(uri, destination_table, job_id_prefix=JOB_ID_PREFIX, job_config=job_config)
        logging.info(f"Started job {job.job_id}")
        result = job.result()
        logging.info(f"Processed {uri}")
    except Exception as err:
        logging.error(f"Error happened during processing of {uri}: {str(err)}")


def create_job_config():
    config = LoadJobConfig()
    config.skip_leading_rows = 1
    config.time_partitioning = TimePartitioning()
    config.clustering_fields = CLUSTERING_FIELDS
    config.schema = SCHEMA
    config.create_disposition = CreateDisposition.CREATE_IF_NEEDED
    config.write_disposition = WriteDisposition.WRITE_APPEND
    return config



