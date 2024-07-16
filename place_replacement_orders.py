"""Script that places orders every block with the same client id (order replacement)
   and writes the time that it was placed along with the order info to BigQuery

Usage: python place_replacement_orders.py
"""

import asyncio
import json
import logging
import time
import threading
from random import randrange
from datetime import datetime

from google.cloud import bigquery
from google.cloud.bigquery import SchemaField

from v4_client_py.chain.aerial.wallet import LocalWallet
from v4_client_py.clients import Subaccount
from v4_client_py.clients.constants import BECH32_PREFIX
from v4_client_py.clients.helpers.chain_helpers import OrderSide
from v4_client_py.clients.helpers.chain_helpers import (
    Order_TimeInForce,
    ORDER_FLAGS_SHORT_TERM,
    Order,
)

# The idea of this experiment is to see what is the lag between the remove and place messages when we replace orders

# Import helpers
from bq_helpers import (
    create_table,
    BatchWriter,
    CLUSTERING_FIELDS,
    SCHEMA,
    TIME_PARTITIONING,
)
from client_helpers import (
    get_markets_data,
    load_config,
    place_orders,
    precompute_order,
    setup_clients,
)


# Dataset configuration
DATASET_ID = "latency_experiments"
TABLE_ID = "two_sided_replace_orders"

# Batch settings for BQ writes
BATCH_SIZE = 50
BATCH_TIMEOUT = 10
WORKER_COUNT = 1

config = load_config()
# Constants on how to place orders
TIME_IN_FORCE = Order_TimeInForce.TIME_IN_FORCE_POST_ONLY
BUY_PRICE = 0.01
SELL_PRICE = 10
SIZE = 1
MARKET = "AXL-USD"
NUM_ORDERS_PER_SIDE_EACH_BLOCK = 1
CLIENT_ID_BUY = 111111111
CLIENT_ID_SELL = 222222222
NUM_BLOCKS = 1000
WAIT_BLOCKS = 10
MAX_LEN_ORDERS = 20000
DYDX_MNEMONIC = config["maker_mnemonic"]
GTB_DELTA = 15

# Logging setup
logging.basicConfig(
    filename=f"order_logs_{GTB_DELTA}.log",
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


# This presigns all the orders and puts it into a dictionary that is used to write later
def pre_signing_thread(client, ledger_client, market, subaccount, orders, lock):
    current_block = client.get_current_block() + WAIT_BLOCKS
    account = ledger_client.query_account(subaccount.wallet.address())
    while True:
        with lock:
            if current_block not in orders and len(orders) < MAX_LEN_ORDERS:
                orders[current_block] = []
                # place one order on each side
                for i in range(NUM_ORDERS_PER_SIDE_EACH_BLOCK):
                    orders[current_block].append(
                        precompute_order(
                            client,
                            ledger_client,
                            market,
                            subaccount,
                            OrderSide.BUY,
                            BUY_PRICE,
                            CLIENT_ID_BUY,
                            current_block + GTB_DELTA,
                            0,
                            SIZE,
                            ORDER_FLAGS_SHORT_TERM,
                            TIME_IN_FORCE,
                            account.sequence,
                        )
                    )
                    orders[current_block].append(
                        precompute_order(
                            client,
                            ledger_client,
                            market,
                            subaccount,
                            OrderSide.SELL,
                            SELL_PRICE,
                            CLIENT_ID_SELL,
                            current_block + GTB_DELTA,
                            0,
                            SIZE,
                            ORDER_FLAGS_SHORT_TERM,
                            TIME_IN_FORCE,
                            account.sequence,
                        )
                    )
                current_block += 1

        time.sleep(0.1)


async def listen_to_block_stream_and_place_orders(batch_writer):
    # Setup clients to broadcast
    wallet = LocalWallet.from_mnemonic(DYDX_MNEMONIC, BECH32_PREFIX)
    client, ledger_client = setup_clients(config["full_node_submission_address"])

    subaccount = Subaccount(wallet, 0)
    market = get_markets_data(client, MARKET)

    orders = {}
    lock = threading.Lock()

    threading.Thread(
        target=pre_signing_thread,
        args=(client, ledger_client, market, subaccount, orders, lock),
        daemon=True,
    ).start()

    previous_block = 0
    num_blocks_placed = 0
    time.sleep(5)
    while num_blocks_placed < NUM_BLOCKS:
        current_block = client.get_current_block()
        if previous_block < current_block:
            logging.info(f"New block: {current_block}")
            with lock:
                if current_block in orders:
                    logging.info(f"Placing orders for block: {current_block}")
                    asyncio.create_task(
                        place_orders(
                            ledger_client,
                            current_block,
                            orders[current_block],
                            batch_writer,
                        )
                    )
                    orders.pop(current_block)
            previous_block = current_block
            num_blocks_placed += 1
            # place orders every 10 seconds
            await asyncio.sleep(10)
        await asyncio.sleep(0.01)


async def main():
    batch_writer = BatchWriter(
        DATASET_ID, TABLE_ID, WORKER_COUNT, BATCH_SIZE, BATCH_TIMEOUT
    )
    batch_writer_task = asyncio.create_task(batch_writer.batch_writer_loop())
    await listen_to_block_stream_and_place_orders(batch_writer)
    await batch_writer_task


if __name__ == "__main__":
    create_table(DATASET_ID, TABLE_ID, SCHEMA, TIME_PARTITIONING, CLUSTERING_FIELDS)
    asyncio.run(main())
