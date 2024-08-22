"""Script that places stateful orders every N seconds
   and writes the time that it was placed along with the order info to BigQuery

Usage: python place_stateful_orders.py
"""

import asyncio
import logging
import time
from logging.handlers import RotatingFileHandler
from random import randrange

from v4_client_py.chain.aerial.wallet import LocalWallet
from v4_client_py.clients import Subaccount
from v4_client_py.clients.constants import BECH32_PREFIX
from v4_client_py.clients.helpers.chain_helpers import OrderSide
from v4_client_py.clients.helpers.chain_helpers import (
    Order_TimeInForce,
    ORDER_FLAGS_LONG_TERM,
)

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

# The idea of this experiment is to see what is the lag between placing a stateful order and when it shows up in a stream

# Dataset configuration
DATASET_ID = "latency_experiments"
TABLE_ID = "long_running_stateful_orders"

# Batch settings for BQ writes
BATCH_SIZE = 2
BATCH_TIMEOUT = 10
WORKER_COUNT = 1

config = load_config()

# Constants on how to place orders
TIME_IN_FORCE = Order_TimeInForce.TIME_IN_FORCE_POST_ONLY
BUY_PRICE = 0.01
SELL_PRICE = 10
SIZE = 1
MARKET = "AXL-USD"
NUM_BLOCKS = 1000
WAIT_BLOCKS = 10
MAX_LEN_ORDERS = 20000
DYDX_MNEMONIC = config["stateful_mnemonic"]
GTBT_DELTA = 5
PLACE_INTERVAL = 10


async def listen_to_block_stream_and_place_orders(batch_writer):
    # Setup clients to broadcast
    wallet = LocalWallet.from_mnemonic(DYDX_MNEMONIC, BECH32_PREFIX)
    client, ledger_client = setup_clients(config["full_node_submission_address"])

    subaccount = Subaccount(wallet, 0)
    market = get_markets_data(client, MARKET)
    client_id = randrange(0, 2**32 - 1)
    num_blocks_placed = 0
    while num_blocks_placed < NUM_BLOCKS:
        logging.info(f"Presigning orders {num_blocks_placed}")
        account = ledger_client.query_account(subaccount.wallet.address())
        # only place one order each time, due to Stateful order rate limit
        orders = [
            precompute_order(
                client,
                ledger_client,
                market,
                subaccount,
                OrderSide.BUY,
                BUY_PRICE,
                client_id,
                0,
                client.calculate_good_til_block_time(GTBT_DELTA),
                SIZE,
                ORDER_FLAGS_LONG_TERM,
                TIME_IN_FORCE,
                account.sequence,
            )
        ]
        logging.info(f"Placing orders {num_blocks_placed}")
        current_block = client.get_current_block()

        # await the order placement task to avoid lapping and scheduling a
        # second  in case it takes longer than PLACE_INTERVAL
        start_time = time.monotonic()
        await asyncio.create_task(
            place_orders(
                ledger_client,
                current_block,
                orders,
                batch_writer,
            )
        )
        elapsed_time = time.monotonic() - start_time

        client_id += 1
        num_blocks_placed += 1

        # place orders every PLACE_INTERVAL seconds to avoid hitting the place stateful order limit
        sleep_time = max(0.0, PLACE_INTERVAL - elapsed_time)
        await asyncio.sleep(sleep_time)

    logging.info("Finished placing orders")



async def main():
    batch_writer = BatchWriter(
        DATASET_ID, TABLE_ID, WORKER_COUNT, BATCH_SIZE, BATCH_TIMEOUT
    )
    batch_writer_task = asyncio.create_task(batch_writer.batch_writer_loop())
    await listen_to_block_stream_and_place_orders(batch_writer)
    await batch_writer_task


if __name__ == "__main__":
    handler = RotatingFileHandler(
        "place_stateful_orders.log",
        maxBytes=5 * 1024 * 1024,  # 5 MB
        backupCount=5
    )
    logging.basicConfig(
        handlers=[handler],
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    create_table(DATASET_ID, TABLE_ID, SCHEMA, TIME_PARTITIONING, CLUSTERING_FIELDS)
    asyncio.run(main())
