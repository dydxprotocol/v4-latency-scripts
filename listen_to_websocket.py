"""Script that listens to the websocket of a specific address 
   and writes to bigquery when each message is received

Usage: python listen_to_websocket.py 
"""
import argparse
import asyncio
import json
import logging
import uuid
from datetime import datetime
from logging.handlers import RotatingFileHandler

import websockets
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField

# Import the BigQuery helpers
from bq_helpers import create_table, BatchWriter

# Loading mnemonic from config.json
with open("config.json", "r") as config_file:
    config_json = json.load(config_file)

DATASET_ID = "indexer_stream_new"
TABLE_ID = "responses"

SCHEMA = [
    SchemaField("received_at", "TIMESTAMP", mode="REQUIRED"),
    SchemaField("uuid", "STRING", mode="REQUIRED"),
    SchemaField("response", "JSON", mode="NULLABLE"),
    SchemaField("server_address", "STRING", mode="REQUIRED"),
]

TIME_PARTITIONING = bigquery.TimePartitioning(field="received_at")
CLUSTERING_FIELDS = ["server_address"]

# Batch settings
BATCH_SIZE = 9
BATCH_TIMEOUT = 10
WORKER_COUNT = 1


def process_message(message, url):
    return {
        "received_at": datetime.utcnow().isoformat("T") + "Z",
        "uuid": str(uuid.uuid4()),
        "response": message,
        "server_address": url
    }


class AsyncSocketClient:
    def __init__(self, indexer_url: str, subaccount_ids, batch_writer):
        self.url = indexer_url
        self.subaccount_ids = subaccount_ids
        self.batch_writer = batch_writer

    async def connect(self):
        retries = 0
        while True:
            try:
                async with websockets.connect(f"wss://{self.url}/v4/ws") as websocket:
                    if self.subaccount_ids:
                        for subaccount_id in self.subaccount_ids:
                            await self.subscribe(
                                websocket, "v4_subaccounts", {"id": subaccount_id}
                            )
                    await self.consumer_handler(websocket)
            except (
                websockets.exceptions.ConnectionClosedError,
                asyncio.exceptions.IncompleteReadError,
            ) as e:
                logging.error(f"WebSocket connection error: {e}. Reconnecting...")
                retries += 1
                await asyncio.sleep(
                    min(2**retries, 60)
                )  # Exponential backoff with a max delay of 60 seconds
            except Exception as e:
                logging.error(f"Unexpected error: {e}. Reconnecting...")
                retries += 1
                await asyncio.sleep(min(2**retries, 60))

    async def consumer_handler(self, websocket):
        async for message in websocket:
            await self.batch_writer.enqueue_data(
                process_message(message, self.url)
            )  # Enqueue data for batch writing

    async def send(self, websocket, message):
        await websocket.send(message)

    async def close(self, websocket):
        await websocket.close()

    async def subscribe(self, websocket, channel, params=None):
        if params is None:
            params = {}
        message = json.dumps({"type": "subscribe", "channel": channel, **params})
        await self.send(websocket, message)


async def main(indexer_url: str):
    batch_writer = BatchWriter(
        DATASET_ID, TABLE_ID, WORKER_COUNT, BATCH_SIZE, BATCH_TIMEOUT
    )
    subaccount_ids = [
        "/".join([config_json["maker_address"], str(0)]),
        "/".join([config_json["taker_address"], str(0)]),
    ]
    client = AsyncSocketClient(
        indexer_url,
        subaccount_ids=subaccount_ids,
        batch_writer=batch_writer,
    )

    batch_writer_task = asyncio.create_task(batch_writer.batch_writer_loop())
    await client.connect()
    await batch_writer_task


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run the Indexer Websocket client for a given URL, e.g. indexer.v4testnet.dydx.exchange."
    )
    parser.add_argument(
        "--indexer_url",
        type=str,
        help="The indexer API to read from.",
    )
    args = parser.parse_args()

    log_id = args.indexer_url.replace(":", "_").replace("/", "_")
    handler = RotatingFileHandler(
        f"listen_to_websocket_{log_id}.log",
        maxBytes=5 * 1024 * 1024,  # 5 MB
        backupCount=5
    )
    logging.basicConfig(
        handlers=[handler],
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    create_table(DATASET_ID, TABLE_ID, SCHEMA, TIME_PARTITIONING, CLUSTERING_FIELDS)
    asyncio.run(main(args.indexer_url))
