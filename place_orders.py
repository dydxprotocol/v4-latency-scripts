"""

Script that places orders every block with a new client id each time
and writes the time that it was placed along with the order info to BigQuery

Usage: python place_orders.py
"""

import asyncio
import json
import logging
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler
from random import randrange

from dydx_v4_client import OrderFlags
from dydx_v4_client.indexer.rest.constants import OrderType
from dydx_v4_client.indexer.rest.indexer_client import IndexerClient
from dydx_v4_client.network import NodeConfig, insecure_channel
from dydx_v4_client.node.client import NodeClient
from dydx_v4_client.node.market import Market
from dydx_v4_client.wallet import Wallet, from_mnemonic
from v4_proto.dydxprotocol.clob.order_pb2 import Order

from bq_helpers import (
    create_table,
    BatchWriter,
    PLACE_ORDER_CLUSTERING_FIELDS,
    PLACE_ORDER_SCHEMA,
    PLACE_ORDER_TIME_PARTITIONING,
)

def load_config():
    # load config from config.json
    with open("config.json", "r") as config_file:
        return json.load(config_file)

config = load_config()
# Dataset configuration
DATASET_ID = "latency_experiments"
TABLE_ID = "long_running_two_sided_orders"

# Batch settings for BQ writes
BATCH_SIZE = 50
BATCH_TIMEOUT = 10
WORKER_COUNT = 1

# Constants on how to place orders
TIME_IN_FORCE = Order.TIME_IN_FORCE_POST_ONLY
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
        market: Market,
        address: str,
        current_block: int,
):
    client_id = STARTING_CLIENT_ID + NUM_ORDERS_PER_SIDE_EACH_BLOCK * 2 * current_block
    client_id = client_id % (2**32 - 1)
    orders = []


    for i in range(NUM_ORDERS_PER_SIDE_EACH_BLOCK):
        id_a = market.order_id(address, 0, client_id + i * 2, OrderFlags.SHORT_TERM)
        id_b = market.order_id(address, 0, client_id + i * 2 + 1, OrderFlags.SHORT_TERM)

        orders.append(
            market.order(
                id_a,
                OrderType.LIMIT,
                Order.Side.SIDE_BUY,
                size=SIZE,
                price=BUY_PRICE,
                time_in_force=TIME_IN_FORCE,
                reduce_only=False,
                good_til_block=current_block + GTB_DELTA,
            )
        )

        orders.append(
            market.order(
                id_b,
                OrderType.LIMIT,
                Order.Side.SIDE_SELL,
                size=SIZE,
                price=SELL_PRICE,
                time_in_force=TIME_IN_FORCE,
                reduce_only=False,
                good_til_block=current_block + GTB_DELTA,
            )
        )

    return orders


async def listen_to_block_stream_and_place_orders(batch_writer):
    # Setup clients to broadcast
    node_url = config['full_node_submission_address']
    valiator_address_field = f'grpc+http://{node_url}'
    node = await NodeClient.connect(
        NodeConfig(
            "dydx-mainnet-1",
            chaintoken_denom="adydx",
            usdc_denom="ibc/8E27BA2D5493AF5636760E354E46004562C46AB7EC0CC4C1CA14E9E20E2545B5",
            channel=insecure_channel(node_url),
        )
    )

    indexer = IndexerClient("https://indexer.dydx.trade")
    market_data = (await indexer.markets.get_perpetual_markets())['markets'][MARKET]
    market = Market(market_data)

    address = Wallet(from_mnemonic(DYDX_MNEMONIC), 0, 0).address
    wallet = await Wallet.from_mnemonic(node, DYDX_MNEMONIC, address)

    previous_block = 0
    num_blocks_placed = 0
    while num_blocks_placed < NUM_BLOCKS:
        current_block = await node.latest_block_height()

        if previous_block < current_block:
            logging.info(f"New block: {current_block}")
            orders = orders_at_height(market, address, current_block)

            logging.info(f"Placing orders:")
            tasks = []
            for order in orders:
                logging.info(f"  {order}")
                order_data = {
                    "sent_at": datetime.utcnow().isoformat("T") + "Z",
                    "uuid": str(randrange(2 ** 32 - 1)),
                    "validator_address": valiator_address_field,
                    "block": current_block,
                    "address": order.order_id.subaccount_id.owner,
                    "side": order.side,
                    "good_til_block": order.good_til_block,
                    "client_id": order.order_id.client_id,
                }
                await batch_writer.enqueue_data(order_data)
                tasks.append(node.place_order(wallet, order))

            resp = await asyncio.gather(*tasks)
            logging.info(f"Placed orders: {resp}")

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

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.INFO)
    logging.basicConfig(
        handlers=[handler, stdout_handler],
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    create_table(DATASET_ID, TABLE_ID, PLACE_ORDER_SCHEMA, PLACE_ORDER_TIME_PARTITIONING, PLACE_ORDER_CLUSTERING_FIELDS)
    asyncio.run(main())
