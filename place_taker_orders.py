import asyncio
import logging
import threading
import time
from logging.handlers import RotatingFileHandler
from random import randrange

from v4_client_py.chain.aerial.wallet import LocalWallet
from v4_client_py.clients import Subaccount
from v4_client_py.clients.constants import BECH32_PREFIX
from v4_client_py.clients.helpers.chain_helpers import OrderSide
from v4_client_py.clients.helpers.chain_helpers import (
    Order_TimeInForce,
    ORDER_FLAGS_SHORT_TERM,
)

# Import helpers
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
)

# Dataset configuration
DATASET_ID = "latency_experiments"
TABLE_ID = "long_running_taker_orders"


# Batch settings for BQ writes
BATCH_SIZE = 2
BATCH_TIMEOUT = 60
WORKER_COUNT = 1

config = load_config()

# Constants on how to place orders
TIME_IN_FORCE = Order_TimeInForce.TIME_IN_FORCE_IOC
BUY_PRICE = 5000
SELL_PRICE = 3000
SIZE = 0.001
MARKET = "ETH-USD"
NUM_ORDERS_PER_SIDE_EACH_BLOCK = 1
STARTING_CLIENT_ID = randrange(0, 2**32 - 1)
# how many blocks to place it for
NUM_BLOCKS = 1000000
WAIT_BLOCKS = 5
MAX_LEN_ORDERS = 50
DYDX_MNEMONIC = config["taker_mnemonic"]
GTB_DELTA = 4
# how often to place these taker orders
# if we place too often the address will lose too much money to fees + spread
PLACE_INTERVAL = 300


# This presigns all the orders and puts it into a dictionary that is used to write later
def pre_signing_thread(client, ledger_client, market, subaccount, orders, lock):
    client_id = STARTING_CLIENT_ID
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
                    orders[current_block].append(
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
                client_id += NUM_ORDERS_PER_SIDE_EACH_BLOCK * 2
                # cant do it every block or the fees drain too quickly
                current_block += PLACE_INTERVAL

        time.sleep(0.2)


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
            task = None
            with lock:
                if current_block in orders:
                    logging.info(f"Placing orders for block: {current_block}")
                    task = asyncio.create_task(
                        place_orders(
                            ledger_client,
                            current_block,
                            orders[current_block],
                            batch_writer,
                        )
                    )
                    orders.pop(current_block)
            if task:
                await task
            previous_block = current_block
            num_blocks_placed += 1

        await asyncio.sleep(0.01)


async def main():
    batch_writer = BatchWriter(
        DATASET_ID, TABLE_ID, WORKER_COUNT, BATCH_SIZE, BATCH_TIMEOUT
    )
    batch_writer_task = asyncio.create_task(batch_writer.batch_writer_loop())
    await listen_to_block_stream_and_place_orders(batch_writer)
    await batch_writer_task


if __name__ == "__main__":
    handler = RotatingFileHandler(
        "place_taker_orders.log",
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
