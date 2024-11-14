"""Script that places orders every block with a new client id each time 
   and writes the time that it was placed along with the order info to BigQuery

Usage: python place_orders.py
"""

import asyncio
import logging
import threading
import time
from logging.handlers import RotatingFileHandler
from random import randrange

from vendor.v4_client_py.chain.aerial.wallet import LocalWallet
from vendor.v4_client_py.clients import Subaccount
from vendor.v4_client_py.clients.constants import BECH32_PREFIX
from vendor.v4_client_py.clients.helpers.chain_helpers import OrderSide
from vendor.v4_client_py.clients.helpers.chain_helpers import (
    Order_TimeInForce,
    ORDER_FLAGS_SHORT_TERM,
)

from bq_helpers import (
    create_table,
    BatchWriter,
    PLACE_ORDER_CLUSTERING_FIELDS,
    PLACE_ORDER_SCHEMA,
    PLACE_ORDER_TIME_PARTITIONING,
)
from client_helpers import (
    get_markets_data,
    load_config,
    place_orders,
    precompute_order,
    setup_clients,
    get_current_block_with_retries,
)

config = load_config()
# Dataset configuration
DATASET_ID = "latency_experiments"
TABLE_ID = "long_running_two_sided_orders"

# Batch settings for BQ writes
BATCH_SIZE = 50
BATCH_TIMEOUT = 10
WORKER_COUNT = 1

# Constants on how to place orders
TIME_IN_FORCE = Order_TimeInForce.TIME_IN_FORCE_POST_ONLY
BUY_PRICE = 0.01
SELL_PRICE = 10
SIZE = 1
MARKET = "AXL-USD"
NUM_ORDERS_PER_SIDE_EACH_BLOCK = 1
STARTING_CLIENT_ID = randrange(0, 2**32 - 1)
NUM_BLOCKS = 1_000_000
WAIT_BLOCKS = 5
MAX_LEN_ORDERS = 50
DYDX_MNEMONIC = config["maker_mnemonic"]
GTB_DELTA = 10


def orders_at_height(
        client,
        ledger_client,
        market,
        subaccount,
        account,
        current_block
):
    client_id = STARTING_CLIENT_ID + NUM_ORDERS_PER_SIDE_EACH_BLOCK * 2 * current_block
    client_id = client_id % (2**32 - 1)
    orders = []

    for i in range(NUM_ORDERS_PER_SIDE_EACH_BLOCK):
        orders.append(
            precompute_order(
                client,
                ledger_client,
                market,
                subaccount,
                OrderSide.BUY,
                BUY_PRICE,
                client_id + i * 2,
                current_block + GTB_DELTA,
                0,
                SIZE,
                ORDER_FLAGS_SHORT_TERM,
                TIME_IN_FORCE,
                account.sequence,
                account.number,
            )
        )
        orders.append(
            precompute_order(
                client,
                ledger_client,
                market,
                subaccount,
                OrderSide.SELL,
                SELL_PRICE,
                client_id + i * 2 + 1,
                current_block + GTB_DELTA,
                0,
                SIZE,
                ORDER_FLAGS_SHORT_TERM,
                TIME_IN_FORCE,
                account.sequence,
                account.number,
            )
        )

    return orders


async def listen_to_block_stream_and_place_orders(batch_writer):
    # Setup clients to broadcast
    wallet = LocalWallet.from_mnemonic(DYDX_MNEMONIC, BECH32_PREFIX)
    client, ledger_client = setup_clients(config["full_node_submission_address"])
    subaccount = Subaccount(wallet, 0)
    market = await asyncio.to_thread(get_markets_data, client, MARKET)
    account = await asyncio.to_thread(
        ledger_client.query_account,
        subaccount.wallet.address()
    )

    previous_block = 0
    num_blocks_placed = 0
    while num_blocks_placed < NUM_BLOCKS:
        current_block = await asyncio.to_thread(
            get_current_block_with_retries,
            client,
        )
        if previous_block < current_block:
            logging.info(f"New block: {current_block}")
            orders = orders_at_height(
                client,
                ledger_client,
                market,
                subaccount,
                account,
                current_block
            )

            await place_orders(
                ledger_client,
                current_block,
                orders,
                batch_writer,
            )

            previous_block = current_block
            num_blocks_placed += 1

        await asyncio.sleep(0.25)

    logging.info("Done - shutting down")


async def main():
    batch_writer = BatchWriter(
        DATASET_ID, TABLE_ID, WORKER_COUNT, BATCH_SIZE, BATCH_TIMEOUT
    )
    batch_writer_task = asyncio.create_task(batch_writer.batch_writer_loop())
    listen_task = asyncio.create_task(
        listen_to_block_stream_and_place_orders(batch_writer)
    )
    await asyncio.gather(batch_writer_task, listen_task)


if __name__ == "__main__":
    handler = RotatingFileHandler(
        "place_orders.log",
        maxBytes=5 * 1024 * 1024,  # 5 MB
        backupCount=5
    )
    logging.basicConfig(
        handlers=[handler],
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    create_table(DATASET_ID, TABLE_ID, PLACE_ORDER_SCHEMA, PLACE_ORDER_TIME_PARTITIONING, PLACE_ORDER_CLUSTERING_FIELDS)
    asyncio.run(main())
