"""Script that listens to grpc stream and writes to bigquery when each message is received

Usage: python listen_to_grpc_stream.py --server_address $SERVER_ADDRESS_WITH_PORT
"""
import argparse
import asyncio
import json
import logging
import uuid
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler

import grpc
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
from google.protobuf.json_format import MessageToJson
from v4_proto.dydxprotocol.clob.query_pb2 import StreamOrderbookUpdatesRequest
from v4_proto.dydxprotocol.clob.query_pb2_grpc import QueryStub

from bq_helpers import create_table, BatchWriter, GCSWriter

# Dataset configuration
DATASET_ID = "full_node_stream"
TABLE_ID = "responses"
CLOB_PAIR_IDS = range(1000)  # Have to update if too many more pairs are added

# If data to too large for direct insert, use this GCS bucket to sideload
GCS_BUCKET = "full_node_stream_sideload"

# Schema, partitioning, and clustering
SCHEMA = [
    SchemaField("received_at", "TIMESTAMP", mode="REQUIRED"),
    SchemaField("server_address", "STRING", mode="REQUIRED"),
    SchemaField("uuid", "STRING", mode="REQUIRED"),
    SchemaField("response", "JSON", mode="NULLABLE"),
]

TIME_PARTITIONING = bigquery.TimePartitioning(field="received_at")
CLUSTERING_FIELDS = ["server_address"]

# Batch settings
BATCH_SIZE = 25
BATCH_TIMEOUT = 10

MAX_RETRIES_PER_DAY = 100
RETRY_DELAY = 5
WORKER_COUNT = 8

GRPC_OPTIONS = [
    ("grpc.keepalive_time_ms", 30000),  # Send keepalive ping every 30 seconds
    (
        "grpc.keepalive_timeout_ms",
        10000,
    ),  # Wait 10 seconds for ping ack before considering the connection dead
    (
        "grpc.keepalive_permit_without_calls",
        True,
    ),  # Allow keepalive pings even when there are no calls
    (
        "grpc.http2.min_time_between_pings_ms",
        30000,
    ),  # Minimum allowed time between pings
    (
        "grpc.http2.min_ping_interval_without_data_ms",
        30000,
    ),  # Minimum allowed time between pings with no data
]


def process_message(message, server_address) -> dict:
    # Convert the protobuf message to a json
    # use indent=None to trim whitespace and reduce message size
    received_at = datetime.utcnow().isoformat("T") + "Z"
    message_json = MessageToJson(message, indent=None)
    return {
        "received_at": received_at,
        "server_address": server_address,
        "uuid": str(uuid.uuid4()),
        "response": message_json,
    }


def process_error(error_msg, server_address):
    data = {
        "received_at": datetime.utcnow().isoformat("T") + "Z",
        "server_address": server_address,
        "uuid": str(uuid.uuid4()),
        "response": json.dumps({"error": error_msg}),
    }
    logging.error(data)
    return data


async def listen_to_stream_and_write_to_bq(
        channel, batch_writer, gcs_writer, server_address
):
    retry_count = 0
    start_time = datetime.utcnow()

    msg_count = 0
    msg_count_time = datetime.utcnow()

    while retry_count < MAX_RETRIES_PER_DAY:
        try:
            stub = QueryStub(channel)
            request = StreamOrderbookUpdatesRequest(clob_pair_id=CLOB_PAIR_IDS)

            # Here, we initiate the streaming call and asynchronously iterate over the responses
            async for response in stub.StreamOrderbookUpdates(request):
                row = process_message(response, server_address)

                # If the row is too large, sideload into BQ via GCS
                too_large_for_direct_insert = len(row['response']) > 10_000_000
                if too_large_for_direct_insert:
                    await gcs_writer.enqueue_data([row])
                else:
                    await batch_writer.enqueue_data(row)

                # Log message counts every 15 mins
                msg_count += 1
                if datetime.utcnow() - msg_count_time > timedelta(minutes=15):
                    logging.info(f"Saw {msg_count} msgs in the last 15 minutes")
                    msg_count_time = datetime.utcnow()
                    msg_count = 0

            await batch_writer.enqueue_data(
                process_error("Stream ended", server_address)
            )
        except grpc.aio.AioRpcError as e:
            # Handle gRPC-specific errors here
            if e.code() in [
                grpc.StatusCode.UNAVAILABLE,
                grpc.StatusCode.DEADLINE_EXCEEDED,
            ]:
                retry_count += 1
            else:
                # For other gRPC errors, write the error and break the loop
                await batch_writer.enqueue_data(
                    process_error(
                        f"gRPC error occurred: {e.code()} - {e.details()}",
                        server_address,
                    )
                )
        except Exception as e:
            # Handle other possible errors
            await batch_writer.enqueue_data(
                process_error(f"Unexpected error in stream: {e}", server_address)
            )

        # Check if a day has passed since the start time, reset retry count if needed
        if datetime.utcnow() - start_time > timedelta(days=1):
            retry_count = 0
            start_time = datetime.utcnow()

        # Sleep before retrying
        logging.error(
            f"Connection failed, retrying... ({retry_count}/{MAX_RETRIES_PER_DAY})"
        )
        await asyncio.sleep(RETRY_DELAY)

    if retry_count == MAX_RETRIES_PER_DAY:
        await batch_writer.enqueue_data(
            process_error(
                f"Max retries per day exceeded, failed to connect.", server_address
            )
        )


def preprocess_row_for_gcs(row: dict) -> dict:
    """
    When writing via GCS, the JSON fields must be Python objects rather than
    JSON strings.
    """
    row['response'] = json.loads(row['response'])
    return row


async def main(server_address):
    # Writer for exceptionally large rows (book snapshots)
    gcs_writer = GCSWriter(DATASET_ID, TABLE_ID, SCHEMA, GCS_BUCKET)
    gcs_writer.set_middleware(preprocess_row_for_gcs)
    gcs_writer_task = asyncio.create_task(gcs_writer.gcs_writer_loop())

    # Writer for direct BigQuery inserts
    batch_writer = BatchWriter(
        DATASET_ID, TABLE_ID, WORKER_COUNT, BATCH_SIZE, BATCH_TIMEOUT
    )
    batch_writer_task = asyncio.create_task(batch_writer.batch_writer_loop())

    # If the direct writer encounters a 413 error, fall back to sideloading
    async def error_413_handler(data_buffer, e):
        logging.error(f"Error 413 occurred, sideloading {len(data_buffer)} rows.")
        await gcs_writer.enqueue_data(data_buffer)
    batch_writer.set_error_413_handler(error_413_handler)

    # Adjust to use secure channel if needed
    logging.info(f"Connecting to server at {server_address}...")
    async with grpc.aio.insecure_channel(server_address, GRPC_OPTIONS) as channel:
        await listen_to_stream_and_write_to_bq(
            channel, batch_writer, gcs_writer, server_address
        )

    await asyncio.gather(batch_writer_task, gcs_writer_task)
    logging.error("Listening task finished (unexpected)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run the GRPC client with for a server address."
    )
    parser.add_argument(
        "--server_address",
        type=str,
        help="The server address in the format ip:port",
    )
    args = parser.parse_args()

    log_id = args.server_address.replace(":", "_")
    handler = RotatingFileHandler(
        f"listen_to_grpc_stream_{log_id}.log",
        maxBytes=5 * 1024 * 1024,  # 5 MB
        backupCount=5
    )
    logging.basicConfig(
        handlers=[handler],
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    create_table(DATASET_ID, TABLE_ID, SCHEMA, TIME_PARTITIONING, CLUSTERING_FIELDS)
    asyncio.run(main(args.server_address))