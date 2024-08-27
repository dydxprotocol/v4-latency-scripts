import json
import logging
import os
import tempfile
import time
import uuid
from datetime import datetime
from typing import List

import google.cloud.exceptions
from google.cloud import storage, bigquery


def get_or_create_bucket(
        client: storage.Client, name: str, location: str, retries: int = 1
) -> storage.Bucket:
    """Get or create a GCS bucket."""
    bucket = client.bucket(name)

    try:
        bucket = client.get_bucket(bucket)
        logging.info(f"Using existing GCS bucket: {name}")
        return bucket
    except google.cloud.exceptions.NotFound:
        try:
            bucket = client.create_bucket(bucket, location=location)
            logging.info(f"Created GCS bucket: {name}")
            return bucket
        except Exception as e:
            logging.error(f"Error creating GCS bucket: {e}")
            # Retries in case of two processes trying to create the same bucket
            # at once, and only one succeeding on the first try
            if retries > 0:
                return get_or_create_bucket(client, name, location, retries - 1)
            raise e


def insert_via_gcs(
        client: bigquery.Client,
        bucket: storage.Bucket,
        tbl: bigquery.Table,
        rows: List[dict]
) -> None:
    """
    Insert a series of `rows` to the given BQ `tbl` via the GCS `bucket`.

    This is useful when the rows are too large to insert directly into BigQuery,
    but note that there is still a file size limit (15TB) and row size limit
    (100MB), per [1].

    [1] https://cloud.google.com/bigquery/quotas#load_jobs

    NOTE: Any JSON fields must be in Python dict form, not serialized strings.

    Mechanically, this function
     - Writes the rows to a JSON temp file
     - Uploads the temp file to GCS
     - Deletes the local copy
     - Creates a load job to insert the GCS file into the table
     - Waits for the job to complete
    """
    try:
        # Generate a unique blob name for the payload
        blob_name = f"{datetime.utcnow().isoformat()}_{uuid.uuid4()}.json"
        blob_name = blob_name.replace(":", "-")

        # Create a temporary file for the JSON payload
        with tempfile.NamedTemporaryFile(delete=True, mode="w") as temp_file:
            for row in rows:
                json.dump(row, temp_file)
                temp_file.write("\n")
            temp_file.flush()

            file_mb = os.path.getsize(temp_file.name) / (1024 * 1024)
            logging.info(f"Wrote {file_mb}MB to temp file {temp_file.name}")

            # Upload the file to GCS
            blob = bucket.blob(blob_name)
            blob.upload_from_filename(temp_file.name)
            logging.info(f"Uploaded file to GCS: {blob.name}")

        # Configure the BigQuery load job
        job_config = bigquery.LoadJobConfig(
            schema=tbl.schema,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            time_partitioning=tbl.time_partitioning,
            clustering_fields=tbl.clustering_fields,
        )

        # Load data from GCS into BigQuery
        logging.info(f"Loading data from GCS to BigQuery: {tbl}")
        load_job = client.load_table_from_uri(
            f"gs://{bucket.name}/{blob_name}", tbl, job_config=job_config
        )

        # Wait for the job to complete
        n = 10
        for _ in range(n):
            result = load_job.result()
            logging.info(f"Got job {result} with state {result.state}")
            if result.state == "DONE":
                return
            time.sleep(5)
        logging.error(f"Job did not succeed within {n} iterations")

    except Exception as e:
        logging.error(f"Error during GCS-based insertion: {e}")
        raise
