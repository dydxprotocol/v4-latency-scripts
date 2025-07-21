"""Script to run all the different python scripts that listens to websocket, 
    grpc stream and restarts them if they fail for any reason

Usage: python run_all_scripts.py
"""

import json
import logging
import signal
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from typing import Dict

from google.cloud import bigquery

# Set up the BigQuery client
client = bigquery.Client()


with open("config.json", "r") as config_file:
    config = json.load(config_file)

# Constants
PROJECT_ID = config["bigquery_project_id"]
CHECK_INTERVAL = 250  # Check every 250 seconds

SCRIPT_CONFIGS = {
    # "websocket": {
    #     "script_name": "listen_to_websocket.py",
    #     "table_id": "indexer_stream.responses",
    #     "timestamp_column": "received_at",
    #     "filter": "",
    #     "args": [],
    #     "time_threshold": timedelta(seconds=90),
    # },
    # "place_orders": {
        # "script_name": "place_orders.py",
        # "table_id": "latency_experiments.long_running_two_sided_orders",
        # "timestamp_column": "sent_at",
        # "filter": "",
        # "args": [],
        # "time_threshold": timedelta(seconds=90),
    # },
    # "place_taker_orders": {
    #     "script_name": "place_taker_orders.py",
    #     "table_id": "latency_experiments.long_running_taker_orders",
    #     "timestamp_column": "sent_at",
    #     "filter": "",
    #     "args": [],
    #     "time_threshold": timedelta(seconds=180),
    # },
    # "place_stateful_orders": {
        # "script_name": "place_stateful_orders.py",
        # "table_id": "latency_experiments.long_running_stateful_orders",
        # "timestamp_column": "sent_at",
        # "filter": "",
        # # "args": [],
        # "time_threshold": timedelta(seconds=90),
    # },
    # Add more scripts with their corresponding table IDs, timestamp columns, and filters here
}

# Include full node stream gRPC listeners
for addr in config["full_node_addresses"]:
    SCRIPT_CONFIGS[f"grpc_stream {addr}"] = {
        "script_name": "listen_to_grpc_stream.py",
        "table_id": "full_node_stream.responses",
        "timestamp_column": "received_at",
        "filter": f'server_address = "{addr}"',
        "args": ["--server_address", addr],
        "time_threshold": timedelta(seconds=90),
    }

for addr in config["indexer_addresses"]:
    SCRIPT_CONFIGS[f"indexer {addr}"] = {
        "script_name": "listen_to_websocket.py",
        "table_id": "indexer_stream_new.responses",
        "timestamp_column": "received_at",
        "filter": f'server_address = "{addr}"',
        "args": ["--indexer_url", addr],
        "time_threshold": timedelta(seconds=90),
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


def terminate_process(process: subprocess.Popen, pname: str):
    # Try to terminate the process
    logging.info(f"Terminating process {pname}...")
    process.terminate()


def force_kill_process(process: subprocess.Popen, pname: str):
    # Check if the process has terminated
    if process.poll() is None:
        # Process is still alive, so forcefully kill it
        logging.info(f"Forcefully killing process {pname}...")
        process.kill()

    process.wait()
    logging.info(f"Process {pname} has finished.")


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

    should_restart = False
    if latest_timestamp:
        current_time = datetime.utcnow().replace(tzinfo=None)
        if current_time - latest_timestamp > time_threshold:
            logging.info(
                f"Latest timestamp for table {table_id} for script {script_name} "
                f"is {latest_timestamp}, restarting {config_name}..."
            )
            should_restart = True
        else:
            logging.info(
                f"Latest timestamp for table {table_id} for script {script_name} "
                f"is {latest_timestamp}, {config_name} is working fine."
            )
    else:
        logging.info(f"Failed to retrieve the latest timestamp for table "
                     f"{table_id}, restarting {config_name}...")
        should_restart = True

    if should_restart:
        terminate_process(process, script_name)
        time.sleep(1)  # Wait for the process to terminate
        force_kill_process(process, script_name)
        return start_script(script_name, args)
    else:
        return process


def main():
    processes: Dict[str, subprocess.Popen] = {
        config: start_script(info["script_name"], info["args"])
        for config, info in SCRIPT_CONFIGS.items()
    }

    # Gracefully handle Ctrl+C
    def signal_handler(sig, frame):
        logging.info("Received termination signal, shutting down...")
        for pname, p in processes.items():
            terminate_process(p, pname)

        time.sleep(3)  # Wait for the processes to terminate

        for pname, p in processes.items():
            force_kill_process(p, pname)

        sys.exit(0)
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

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
            else:
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