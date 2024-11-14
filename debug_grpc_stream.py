import argparse
import asyncio
import logging
from logging.handlers import RotatingFileHandler

import grpc
from google.protobuf.json_format import MessageToJson
from v4_proto.dydxprotocol.clob.query_pb2 import StreamOrderbookUpdatesRequest
from v4_proto.dydxprotocol.clob.query_pb2_grpc import QueryStub

# Dataset configuration
CLOB_PAIR_IDS = range(1000)  # Have to update if too many more pairs are added

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


async def main(server_address):
    async with grpc.aio.insecure_channel(server_address, GRPC_OPTIONS) as channel:
        try:
            stub = QueryStub(channel)
            request = StreamOrderbookUpdatesRequest(clob_pair_id=CLOB_PAIR_IDS)

            # Here, we initiate the streaming call and asynchronously iterate over the responses
            async for response in stub.StreamOrderbookUpdates(request):
                print(MessageToJson(response))

            print("Stream ended")
        except Exception as e:
            print(f"An error occurred: {e}")

    print("Done")


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

    # Make sure server_address is provided
    if not args.server_address:
        parser.print_help()
        exit(1)

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

    asyncio.run(main(args.server_address))
