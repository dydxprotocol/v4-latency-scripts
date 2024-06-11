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

from v4_client_py.chain.aerial.tx import SigningCfg, Transaction
from v4_client_py.chain.aerial.client import LedgerClient, NetworkConfig
from v4_client_py.chain.aerial.wallet import LocalWallet
from v4_client_py.clients import CompositeClient, Subaccount
from v4_client_py.clients.constants import BECH32_PREFIX, Network
from v4_client_py.clients.helpers.chain_helpers import OrderSide
from v4_client_py.clients.helpers.chain_helpers import (
    Order_TimeInForce,
    calculate_side,
    calculate_quantums,
    calculate_subticks,
    ORDER_FLAGS_LONG_TERM,
    Order,
)

# The idea of this experiment is to see what is the lag between the remove and place messages when we replace orders

# Import BigQuery helpers
from bq_helpers import create_table, BatchWriter

# Dataset configuration
DATASET_ID = "latency_experiments"
TABLE_ID = "long_running_stateful_orders"

# Schema and partitioning
SCHEMA = [
    SchemaField("sent_at", "TIMESTAMP", mode="REQUIRED"),
    SchemaField("uuid", "STRING", mode="REQUIRED"),
    SchemaField("validator_address", "STRING", mode="REQUIRED"),
    SchemaField("block", "INT64", mode="REQUIRED"),
    SchemaField("address", "STRING", mode="REQUIRED"),
    SchemaField("side", "STRING", mode="REQUIRED"),
    SchemaField("good_til_block", "INT64", mode="REQUIRED"),
    SchemaField("client_id", "INT64", mode="REQUIRED"),
]
TIME_PARTITIONING = bigquery.TimePartitioning(field="sent_at")
CLUSTERING_FIELDS = ["validator_address"]
# Batch settings for BQ writes
BATCH_SIZE = 2
BATCH_TIMEOUT = 10
WORKER_COUNT = 1

# Loading mnemonic from config.json
with open("config.json", "r") as config_file:
    config = json.load(config_file)

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

# Logging setup
logging.basicConfig(
    filename=f"stateful_maker_orders.log",
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def place_order(client, block, msg_and_order, batch_writer):
    try:
        logging.info(f"Placing order: {msg_and_order[0]}")
        result = client.broadcast_tx(msg_and_order[0])
        logging.info(f"Placed order: {msg_and_order[1]}")
    except Exception as error:
        err_str = str(error)
        logging.error(f"Order failed: {msg_and_order[1]} {err_str}")
    return


async def place_orders(client, block, msg_and_orders, batch_writer):
    loop = asyncio.get_running_loop()
    for msg_and_order in msg_and_orders:
        logging.info(f"Logging order: {msg_and_order[0]}")
        # Record order information in BigQuery
        order = msg_and_order[1].order
        order_data = {
            "sent_at": datetime.utcnow().isoformat("T") + "Z",
            "uuid": str(randrange(2**32 - 1)),
            "validator_address": client._network_config.url,
            "block": block,
            "address": order.order_id.subaccount_id.owner,
            "side": order.side,
            "good_til_block": order.good_til_block,
            "client_id": order.order_id.client_id,
        }
        loop = asyncio.get_running_loop()
        # write to BQ
        await batch_writer.enqueue_data(order_data)
        # broadcast order
        loop.run_in_executor(
            None,
            place_order,
            client,
            block,
            msg_and_order,
            batch_writer,
        )


def precompute_order(
    client, ledger_client, market, subaccount, side, price, client_id, sequence
):
    clob_pair_id = market["clobPairId"]
    atomic_resolution = market["atomicResolution"]
    step_base_quantums = market["stepBaseQuantums"]
    quantum_conversion_exponent = market["quantumConversionExponent"]
    subticks_per_tick = market["subticksPerTick"]
    order_side = calculate_side(side)
    quantums = calculate_quantums(SIZE, atomic_resolution, step_base_quantums)
    subticks = calculate_subticks(
        price, atomic_resolution, quantum_conversion_exponent, subticks_per_tick
    )
    order_flags = ORDER_FLAGS_LONG_TERM
    gtbt = client.calculate_good_til_block_time(GTBT_DELTA)

    msg = client.validator_client.post.composer.compose_msg_place_order(
        address=subaccount.address,
        subaccount_number=subaccount.subaccount_number,
        client_id=client_id,
        clob_pair_id=clob_pair_id,
        order_flags=order_flags,
        good_til_block=0,
        good_til_block_time=gtbt,
        side=order_side,
        quantums=quantums,
        subticks=subticks,
        time_in_force=TIME_IN_FORCE,
        reduce_only=False,
        client_metadata=0,
        condition_type=Order.CONDITION_TYPE_UNSPECIFIED,
        conditional_order_trigger_subticks=0,
    )

    wallet = subaccount.wallet
    tx = Transaction()
    tx.add_message(msg)
    gas_limit = 0
    sender = wallet
    memo = None

    fee = ledger_client.estimate_fee_from_gas(gas_limit)
    account = ledger_client.query_account(sender.address())

    tx.seal(
        SigningCfg.direct(sender.public_key(), sequence),
        fee=fee,
        gas_limit=gas_limit,
        memo=memo,
    )
    tx.sign(sender.signer(), ledger_client.network_config.chain_id, account.number)
    tx.complete()
    return (tx, msg)


def get_markets_data(client, market):
    markets_response = client.indexer_client.markets.get_perpetual_markets(market)
    return markets_response.data["markets"][market]


async def listen_to_block_stream_and_place_orders(batch_writer):
    # Setup clients to broadcast
    wallet = LocalWallet.from_mnemonic(DYDX_MNEMONIC, BECH32_PREFIX)
    network_config = Network.config_network()
    # hardcode constants
    network_config.validator_config.grpc_endpoint = "dydx-grpc.polkachu.com:23890"
    network_config.validator_config.url = "https://dydx-grpc.polkachu.com:23890"
    network_config.validator_config.ssl_enabled = False
    client = CompositeClient(network_config)
    url = (
        "grpc+https://"
        if client.validator_client.post.config.ssl_enabled
        else "grpc+http://"
    ) + client.validator_client.post.config.grpc_endpoint
    network = NetworkConfig(
        client.validator_client.post.config.chain_id, 0, None, None, url, None
    )
    ledger_client = LedgerClient(network)

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
                account.sequence,
            )
        ]
        logging.info(f"Placing orders {num_blocks_placed}")
        current_block = client.get_current_block()
        asyncio.create_task(
            place_orders(
                ledger_client,
                current_block,
                orders,
                batch_writer,
            )
        )
        client_id += 1
        num_blocks_placed += 1
        # place orders every 15 seconds to avoid hitting the place order limit
        await asyncio.sleep(10)


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
