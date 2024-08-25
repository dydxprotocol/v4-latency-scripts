"""
This script tests the `insert_via_gcs` function from `bigquery_gcs_insert.py`,
which inserts rows into BigQuery via Google Cloud Storage.

It creates a new GCS bucket and BigQuery table, inserts two rows into the table,
one directly and one via GCS, and then validates the results before deleting
the resources.
"""
from typing import Tuple

from bigquery_gcs_insert import *


def create_test_resources(
        client: bigquery.Client,
        schema: list[bigquery.SchemaField],
        time_partitioning: bigquery.TimePartitioning,
        clustering_fields: list[str],
) -> Tuple[storage.Bucket, bigquery.Table]:
    """Create a new GCS bucket and a BigQuery table for testing."""
    try:
        # Generate unique names for the table and bucket
        bucket_name = f"test-bucket-{uuid.uuid4()}"
        project_id = client.project
        dataset_id = "integration_test_dataset"
        table_name = f"test_table_{uuid.uuid4().hex}"
        table_id = f"{project_id}.{dataset_id}.{table_name}"

        # Create BigQuery dataset if it doesn't exist
        logging.info(f"Creating BQ table project_id: {project_id} "
                     f"dataset_id: {dataset_id} table_id: {table_id}")
        dataset_ref = bigquery.Dataset(f"{project_id}.{dataset_id}")
        client.create_dataset(dataset_ref, exists_ok=True)
        logging.info(f"Created or verified dataset: {dataset_id}")

        # Create BigQuery table
        tbl = bigquery.Table(table_id, schema=schema)
        tbl.time_partitioning = time_partitioning
        tbl.clustering_fields = clustering_fields
        tbl = client.create_table(tbl)
        logging.info(f"Created BigQuery table: {table_id}")

        bucket = get_or_create_bucket(storage.Client(), bucket_name, "EU")
        return bucket, tbl

    except Exception as e:
        logging.error(f"Error during setup: {e}")
        raise


def cleanup_test_resources(client: bigquery.Client, tbl: bigquery.Table, bucket: storage.Bucket):
    """Delete the BigQuery table and GCS bucket."""
    try:
        # Delete the BigQuery table
        client.delete_table(tbl, not_found_ok=True)
        logging.info(f"Deleted BigQuery table: {tbl}")

        # Delete the GCS bucket and its contents
        bucket.delete(force=True)
        logging.info(f"Deleted GCS bucket: {bucket.name}")

        logging.info("Cleanup completed successfully.")

    except Exception as e:
        logging.error(f"Error during cleanup: {e}")
        raise


def validate_results(client: bigquery.Client, tbl: bigquery.Table) -> None:
    """Validate that the two inserted rows exist and are identical except for the UUID."""
    try:
        query = f"SELECT * FROM `{tbl}` ORDER BY uuid"
        logging.info(f"Running query: {query}")
        query_job = client.query(query)
        rows = list(query_job.result())
        logging.info(f"Query results: {rows}")

        # Print as JSON
        for idx, row in enumerate(rows):
            for k in row.keys():
                logging.info(f"row={idx} k={k} type={type(row[k])} v={row[k]}")

        # Ensure there are two rows
        assert len(rows) == 2, "There should be exactly two rows in the table"

        # Validate the UUIDs
        assert rows[0].uuid == "bar", "First row UUID should be 'bar'"
        assert rows[1].uuid == "bar_modified", "Second row UUID should be 'bar_modified'"

        # Validate other fields are identical
        for field in ["received_at", "server_address", "response"]:
            assert rows[0][field] == rows[1][field], f"{field} field does not match"

        logging.info("Validation successful: rows identical except for UUID.")
    except Exception as e:
        logging.error(f"Error during validation: {e}")
        raise


def run_integration_test():
    """Run the full integration test."""
    # Dataset configuration
    schema = [
        bigquery.SchemaField("received_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("server_address", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("uuid", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("response", "JSON", mode="NULLABLE"),
    ]
    tp = bigquery.TimePartitioning(field="received_at")
    clustering = ["server_address"]

    test_row_data = {
        "received_at": datetime.utcnow().isoformat("T") + "Z",
        "server_address": "foo",
        "uuid": "bar",
        "response": '{"updates": [{"foo": "short JSON string"}, {"bar": "long JSON string"}]}',
    }

    try:
        # Setup temporary cloud resources for testing
        client = bigquery.Client()
        bucket, tbl = create_test_resources(client, schema, tp, clustering)

        # Give them time to init (empirically determined)
        time.sleep(2)

        try:
            # Insert directly into BigQuery
            errors = client.insert_rows_json(tbl, [test_row_data])
            if errors:
                logging.error(f"Failed to insert row directly: {errors}")

            # Modify the payload and insert via GCS
            modified_row = test_row_data.copy()
            modified_row['uuid'] = 'bar_modified'
            modified_row['response'] = json.loads(modified_row['response'])
            insert_via_gcs(client, bucket, tbl, [modified_row])

            # Validate the results
            validate_results(client, tbl)

        except Exception as e:
            logging.error(f"Integration test failed: {e}")
        finally:
            # Cleanup resources
            cleanup_test_resources(client, tbl, bucket)
            pass
    except Exception as e:
        logging.error(f"Error during setup: {e}")
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.info(
        f"GOOGLE_APPLICATION_CREDENTIALS="
        f"{os.environ['GOOGLE_APPLICATION_CREDENTIALS']}"
    )
    run_integration_test()