import asyncio
import logging
from datetime import datetime
from typing import List, Callable

from google.api_core.exceptions import GoogleAPICallError
from google.cloud import bigquery, storage
from google.cloud.bigquery import SchemaField
from google.cloud.exceptions import NotFound

import bigquery_gcs_insert as gcs_insert

# Schema and partitioning used across all order types
PLACE_ORDER_SCHEMA = [
    SchemaField("sent_at", "TIMESTAMP", mode="REQUIRED"),
    SchemaField("uuid", "STRING", mode="REQUIRED"),
    SchemaField("validator_address", "STRING", mode="REQUIRED"),
    SchemaField("block", "INT64", mode="REQUIRED"),
    SchemaField("address", "STRING", mode="REQUIRED"),
    SchemaField("side", "STRING", mode="REQUIRED"),
    SchemaField("good_til_block", "INT64", mode="REQUIRED"),
    SchemaField("client_id", "INT64", mode="REQUIRED"),
]
PLACE_ORDER_TIME_PARTITIONING = bigquery.TimePartitioning(field="sent_at")
PLACE_ORDER_CLUSTERING_FIELDS = ["validator_address"]


def create_table(
        dataset_id, table_id, schema, time_partitioning=None, clustering_fields=None
):
    """Creates a BigQuery table with the specified schema, time partitioning, and clustering."""
    bq_client = bigquery.Client()
    dataset_ref = bq_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    try:
        bq_client.get_table(table_ref)
        logging.info(f"Table {table_id} already exists.")
    except NotFound:
        table = bigquery.Table(table_ref, schema=schema)
        table.time_partitioning = time_partitioning
        table.clustering_fields = clustering_fields
        bq_client.create_table(table)
        logging.info(f"Table {table_id} created.")


class BatchWriter:
    """Handles batching and inserting data into BigQuery."""

    def __init__(
            self, dataset_id, table_id, worker_count=8, batch_size=5000, batch_timeout=10
    ):
        self.bq_client = bigquery.Client()
        self.queue = asyncio.Queue()
        self.table_ref = self.bq_client.dataset(dataset_id).table(table_id)
        self.worker_count = worker_count
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self.last_flush_time = datetime.utcnow()

    async def enqueue_data(self, data):
        await self.queue.put(data)

    async def flush_data(self, data_buffer):
        try:
            errors = await asyncio.to_thread(
                self.bq_client.insert_rows_json, self.table_ref, data_buffer
            )
            if errors:
                logging.error(f"Errors occurred: {errors}")
        # TODO: Catch batch too large thing and fall back to GCS writer
        except GoogleAPICallError as e:
            logging.error(f"Error inserting {len(data_buffer)} rows (code = {e.code}): {e}")
        except Exception as e:
            logging.error(f"Error inserting {len(data_buffer)} rows: {e}")
        finally:
            self.last_flush_time = datetime.utcnow()

    async def batch_writer_loop(self):
        workers = [asyncio.create_task(self.worker()) for _ in range(self.worker_count)]
        await asyncio.gather(*workers)

    async def worker(self):
        data_buffer = []
        while True:
            try:
                elapsed_time = (
                        datetime.utcnow() - self.last_flush_time
                ).total_seconds()
                dynamic_timeout = max(0, self.batch_timeout - elapsed_time)
                data = await asyncio.wait_for(self.queue.get(), timeout=dynamic_timeout)
                data_buffer.append(data)

                if len(data_buffer) >= self.batch_size:
                    await self.flush_data(data_buffer)
                    data_buffer = []
            except asyncio.TimeoutError:
                if data_buffer:
                    await self.flush_data(data_buffer)
                    data_buffer = []


class GCSWriter:
    """
    Handles sideloading data into BigQuery via Google Cloud Storage, for large
    rows that cannot be directly inserted.

    Note that JSON fields need to be parsed into Python objects, not strings.
    """
    def __init__(
            self,
            dataset_id: str,
            table_name: str,
            schema: List[bigquery.SchemaField],
            bucket_name: str,
            location: str = "EU"
    ):
        self.client = bigquery.Client()
        self.queue = asyncio.Queue()

        project_id = self.client.project
        table_id = f"{project_id}.{dataset_id}.{table_name}"
        self.table = bigquery.Table(table_id, schema=schema)

        self.bucket = gcs_insert.get_or_create_bucket(
            storage.Client(), bucket_name, location
        )

        # Keep bucket data for 1 day
        gcs_insert.set_lifecycle_policy(self.bucket, 1)

        # Default middleware function does not modify data before insert
        self.middleware_fn = lambda x: x

    async def enqueue_data(self, data):
        await self.queue.put(data)

    async def gcs_writer_loop(self):
        while True:
            try:
                data = await self.queue.get()
                await asyncio.to_thread(self._process_and_insert, data)
            except Exception as e:
                logging.error(f"Error inserting data: {e}")

    def _process_and_insert(self, data: dict) -> None:
        data = self.middleware_fn(data)
        return gcs_insert.insert_via_gcs(
            self.client, self.bucket, self.table, [data]
        )

    def set_middleware(self, middleware_fn: Callable[[dict], dict]):
        """
        Set a middleware function that pre-processes each row before inserting
        it.
        """
        self.middleware_fn = middleware_fn
