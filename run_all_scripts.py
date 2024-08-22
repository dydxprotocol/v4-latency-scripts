"""Script to run all the different python scripts that listens to websocket, 
    grpc stream and restarts them if they fail for any reason

Usage: python run_all_scripts.py
"""

import json
import logging
import subprocess
import time
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler

from google.cloud import bigquery

# Set up the BigQuery client
client = bigquery.Client()


with open("config.json", "r") as config_file:
    config = json.load(config_file)

# Constants
PROJECT_ID = config["bigquery_project_id"]
FULL_NODE_ADDRESS_1 = config["full_node_address_1"]
FULL_NODE_ADDRESS_2 = config["full_node_address_2"]
FULL_NODE_ADDRESS_3 = config["full_node_address_3"]
CHECK_INTERVAL = 250  # Check every 250 seconds

SCRIPT_CONFIGS = {
    "websocket": {
        "script_name": "listen_to_websocket.py",
        "table_id": "indexer_stream.responses",
        "timestamp_column": "received_at",
        "filter": "",
        "args": [],
        "time_threshold": timedelta(seconds=90),
    },
    "grpc_stream "
    + FULL_NODE_ADDRESS_1: {
        "script_name": "listen_to_grpc_stream.py",
        "table_id": "full_node_stream.responses",
        "timestamp_column": "received_at",
        "filter": 'server_address = "{address}"'.format(address=FULL_NODE_ADDRESS_1),
        "args": ["--server_address", FULL_NODE_ADDRESS_1],
        "time_threshold": timedelta(seconds=90),
    },
    # "grpc_stream "
    # + FULL_NODE_ADDRESS_2: {
    #     "script_name": "listen_to_grpc_stream.py",
    #     "table_id": "full_node_stream.responses",
    #     "timestamp_column": "received_at",
    #     "filter": 'server_address = "{address}"'.format(address=FULL_NODE_ADDRESS_2),
    #     "args": ["--server_address", FULL_NODE_ADDRESS_2],
    #     "time_threshold": timedelta(seconds=90),
    # },
    "grpc_stream "
    + FULL_NODE_ADDRESS_3: {
        "script_name": "listen_to_grpc_stream.py",
        "table_id": "full_node_stream.responses",
        "timestamp_column": "received_at",
        "filter": 'server_address = "{address}"'.format(address=FULL_NODE_ADDRESS_3),
        "args": ["--server_address", FULL_NODE_ADDRESS_3],
        "time_threshold": timedelta(seconds=90),
    },
    "place_orders": {
        "script_name": "place_orders.py",
        "table_id": "latency_experiments.long_running_two_sided_orders",
        "timestamp_column": "sent_at",
        "filter": "",
        "args": [],
        "time_threshold": timedelta(seconds=90),
    },
    # "place_taker_orders": {
    #     "script_name": "place_taker_orders.py",
    #     "table_id": "latency_experiments.long_running_taker_orders",
    #     "timestamp_column": "sent_at",
    #     "filter": "",
    #     "args": [],
    #     "time_threshold": timedelta(seconds=180),
    # },
    "place_stateful_orders": {
        "script_name": "place_stateful_orders.py",
        "table_id": "latency_experiments.long_running_stateful_orders",
        "timestamp_column": "sent_at",
        "filter": "",
        "args": [],
        "time_threshold": timedelta(seconds=90),
    },
    # Add more scripts with their corresponding table IDs, timestamp columns, and filters here
}


def get_latest_timestamp(table_id, timestamp_column, filter_condition):
    filter_clause = f"WHERE TIMESTAMP_TRUNC({timestamp_column}, DAY) = TIMESTAMP(CURRENT_DATE()) AND {filter_condition}" if filter_condition else ""
    query = f"""
    SELECT MAX({timestamp_column}) as latest_timestamp
    FROM `{PROJECT_ID}.{table_id}`
    {filter_clause}
    """
    query_job = client.query(query)
    results = query_job.result()
    for row in results:
        if row["latest_timestamp"]:
            return row["latest_timestamp"].astimezone(timezone.utc).replace(tzinfo=None)
    return None


def start_script(script_name, args):
    logging.info("running " + script_name + " " + " ".join(args))
    return subprocess.Popen(["python", script_name] + args)


def check_and_restart_script(
    process,
    config_name,
    script_name,
    table_id,
    timestamp_column,
    filter_condition,
    args,
    time_threshold,
):
    latest_timestamp = get_latest_timestamp(
        table_id, timestamp_column, filter_condition
    )
    if latest_timestamp:
        current_time = datetime.utcnow().replace(tzinfo=None)
        if current_time - latest_timestamp > time_threshold:
            logging.info(
                f"Latest timestamp for table {table_id} for script {script_name} is {latest_timestamp}, restarting {config_name}..."
            )
            process.kill()
            process.wait()
            return start_script(script_name, args)
        else:
            logging.info(
                f"Latest timestamp for table {table_id} for script {script_name} is {latest_timestamp}, {config_name} is working fine."
            )
    else:
        logging.info(f"Failed to retrieve the latest timestamp for table {table_id}.")
    return process


def main():
    processes = {
        config: start_script(info["script_name"], info["args"])
        for config, info in SCRIPT_CONFIGS.items()
    }

    time.sleep(CHECK_INTERVAL)

    while True:
        for config, info in SCRIPT_CONFIGS.items():
            process = processes[config]
            script_name = info["script_name"]
            table_id = info["table_id"]
            timestamp_column = info["timestamp_column"]
            filter_condition = info.get("filter", "")
            args = info["args"]

            # Check if the process is still running
            if process.poll() is not None:
                logging.info(f"{script_name} ({config}) has stopped, restarting...")
                processes[config] = start_script(script_name, args)

            processes[config] = check_and_restart_script(
                process,
                config,
                script_name,
                table_id,
                timestamp_column,
                filter_condition,
                args,
                info["time_threshold"],
            )

        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    handler = RotatingFileHandler(
        "run_all_scripts.log",
        maxBytes=5 * 1024 * 1024,  # 5 MB
        backupCount=5
    )
    logging.basicConfig(
        handlers=[handler],
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    main()
