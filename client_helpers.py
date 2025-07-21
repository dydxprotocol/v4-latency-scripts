# Helpers to help with placing orders and database setup
import asyncio
import json
import logging
from datetime import datetime
from random import randrange

from v4_client_py.chain.aerial.client import LedgerClient, NetworkConfig
from v4_client_py.chain.aerial.tx import SigningCfg, Transaction
from v4_client_py.clients import CompositeClient
from v4_client_py.clients.constants import Network
from v4_client_py.clients.helpers.chain_helpers import (
    calculate_side,
    calculate_quantums,
    calculate_subticks,
    Order,
)


def load_config():
    # load config from config.json
    with open("config.json", "r") as config_file:
        return json.load(config_file)


def setup_clients(grpc_endpoint):
    # setup network and ledger client
    network_config = Network.config_network()
    # hardcode constants
    network_config.indexer_config.rest_endpoint = 'https://indexer.dydx.trade'
    network_config.validator_config.grpc_endpoint = grpc_endpoint
    network_config.validator_config.url = "https://" + grpc_endpoint
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
    return client, ledger_client


def get_current_block_with_retries(client: CompositeClient):
    for i in range(5):
        try:
            return client.get_current_block()
        except Exception as error:
            if i == 4:
                raise error
    raise Exception("Failed to get current block")


def get_markets_data(client, market):
    markets_response = client.indexer_client.markets.get_perpetual_markets(market)
    return markets_response.data["markets"][market]


def place_order(client, block, msg_and_order, batch_writer):
    # broadcast order
    try:
        logging.info(f"Placing order: {msg_and_order[0]}")
        result = client.broadcast_tx(msg_and_order[0])
        logging.info(f"Placed order: {msg_and_order[1]}")
    except Exception as error:
        err_str = str(error)
        logging.error(f"Order failed: {msg_and_order[1]} {err_str}")
        return err_str
    return None


def precompute_order(
        client,
        ledger_client,
        market,
        subaccount,
        side,
        price,
        client_id,
        good_til_block,
        good_til_block_time,
        size,
        order_flags,
        time_in_force,
        sequence_number,
        account_number,
):
    # precompute order and sign the order
    clob_pair_id = market["clobPairId"]
    atomic_resolution = market["atomicResolution"]
    step_base_quantums = market["stepBaseQuantums"]
    quantum_conversion_exponent = market["quantumConversionExponent"]
    subticks_per_tick = market["subticksPerTick"]
    order_side = calculate_side(side)
    quantums = calculate_quantums(size, atomic_resolution, step_base_quantums)
    subticks = calculate_subticks(
        price, atomic_resolution, quantum_conversion_exponent, subticks_per_tick
    )

    msg = client.validator_client.post.composer.compose_msg_place_order(
        address=subaccount.address,
        subaccount_number=subaccount.subaccount_number,
        client_id=client_id,
        clob_pair_id=clob_pair_id,
        order_flags=order_flags,
        good_til_block=good_til_block,
        good_til_block_time=good_til_block_time,
        side=order_side,
        quantums=quantums,
        subticks=subticks,
        time_in_force=time_in_force,
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

    tx.seal(
        SigningCfg.direct(sender.public_key(), sequence_number),
        fee=fee,
        gas_limit=gas_limit,
        memo=memo,
    )
    tx.sign(
        sender.signer(),
        ledger_client.network_config.chain_id,
        account_number
    )
    tx.complete()
    return tx, msg


async def place_orders(client, block, msg_and_orders, batch_writer):
    # spin a new event loop to write to BQ and broadcast orders concurrently
    loop = asyncio.get_running_loop()
    tasks = []
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
        # write to BQ
        await batch_writer.enqueue_data(order_data)
        # broadcast order
        tasks.append(loop.run_in_executor(
            None,
            place_order,
            client,
            block,
            msg_and_order,
            batch_writer,
        ))

    return await asyncio.gather(*tasks)