"""Script that listens to grpc stream and writes to bigquery when each message is received

Usage: python listen_to_grpc_stream.py --server_address $SERVER_ADDRESS_WITH_PORT
"""

import grpc
import json
import asyncio
import uuid
from datetime import datetime, timedelta
import argparse

from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
from google.protobuf.json_format import MessageToJson

# Import your generated gRPC classes
from v4_proto.dydxprotocol.clob.query_pb2 import StreamOrderbookUpdatesRequest
from v4_proto.dydxprotocol.clob.query_pb2_grpc import QueryStub

# Import the BigQuery helpers
from bq_helpers import create_table, BatchWriter

# Dataset configuration
DATASET_ID = "full_node_stream"
TABLE_ID = "responses"
CLOB_PAIR_IDS = range(69)

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
BATCH_SIZE = 5000
BATCH_TIMEOUT = 10

MAX_RETRIES_PER_DAY = 100
RETRY_DELAY = 30

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


def process_message(message, server_address):
    # Convert the protobuf message to a json
    message_json = MessageToJson(message)
    return {
        "received_at": datetime.utcnow().isoformat("T") + "Z",
        "server_address": server_address,
        "uuid": str(uuid.uuid4()),
        "response": message_json,
    }


def process_error(error_msg, server_address):
    print(error_msg)
    return {
        "received_at": datetime.utcnow().isoformat("T") + "Z",
        "server_address": server_address,
        "uuid": str(uuid.uuid4()),
        "response": json.dumps({"error": error_msg}),
    }


async def listen_to_stream_and_write_to_bq(channel, batch_writer, server_address):
    retry_count = 0
    start_time = datetime.utcnow()
    while retry_count < MAX_RETRIES_PER_DAY:
        try:
            stub = QueryStub(channel)
            request = StreamOrderbookUpdatesRequest(clob_pair_id=CLOB_PAIR_IDS)

            # Here, we initiate the streaming call and asynchronously iterate over the responses
            async for response in stub.StreamOrderbookUpdates(request):
                await batch_writer.enqueue_data(
                    process_message(response, server_address)
                )

            await batch_writer.enqueue_data(
                process_error("Stream ended", server_address)
            )
            break  # Break out of the loop if connection is successful and messages are processed
        except grpc.aio.AioRpcError as e:
            # Handle gRPC-specific errors here
            if e.code() in [
                grpc.StatusCode.UNAVAILABLE,
                grpc.StatusCode.DEADLINE_EXCEEDED,
            ]:
                retry_count += 1
                print(
                    f"Connection failed, retrying... ({retry_count}/{MAX_RETRIES_PER_DAY})"
                )
                await asyncio.sleep(RETRY_DELAY)  # Wait before retrying
            else:
                # For other gRPC errors, write the error and break the loop
                await batch_writer.enqueue_data(
                    process_error(
                        f"gRPC error occurred: {e.code()} - {e.details()}",
                        server_address,
                    )
                )
                break
        except Exception as e:
            # Handle other possible errors
            await batch_writer.enqueue_data(
                process_error(f"Unexpected error in stream: {e}", server_address)
            )
            break

        # Check if a day has passed since the start time, reset retry count if needed
        if datetime.utcnow() - start_time > timedelta(days=1):
            retry_count = 0
            start_time = datetime.utcnow()

    if retry_count == MAX_RETRIES_PER_DAY:
        await batch_writer.enqueue_data(
            process_error(
                f"Max retries per day exceeded, failed to connect.", server_address
            )
        )


async def main(server_address):
    batch_writer = BatchWriter(
        DATASET_ID, TABLE_ID, WORKER_COUNT, BATCH_SIZE, BATCH_TIMEOUT
    )
    batch_writer_task = asyncio.create_task(batch_writer.batch_writer_loop())
    # Adjust to use secure channel if needed
    async with grpc.aio.insecure_channel(server_address, GRPC_OPTIONS) as channel:
        await listen_to_stream_and_write_to_bq(channel, batch_writer, server_address)

    await batch_writer_task


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

    create_table(DATASET_ID, TABLE_ID, SCHEMA, TIME_PARTITIONING, CLUSTERING_FIELDS)
    asyncio.run(main(args.server_address))
