import google.cloud.bigquery as bigquery
from datadog import initialize, api
import time
import json
import os
import logging


logging.basicConfig(
    filename="bigquery_to_dd.log",
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


with open("config.json", "r") as config_file:
    config = json.load(config_file)

START_TIMESTAMP = "2024-06-18 22:05:00"
PROJECT_ID = config["bigquery_project_id"]
# Configuration
QUERIES = [
    {
        "name": "short_term_order_latency",
        "query": """
                SELECT p.received_at
                    , TIMESTAMP_DIFF(p.received_at, s.sent_at, millisecond) AS latency
                    , p.server_address
                FROM `{project_id}.latency_experiments.long_running_two_sided_orders` s
                JOIN `{project_id}.full_node_stream.order_places` p
                   ON p.client_id = CAST(s.client_id AS STRING)
                  AND p.address = s.address
                  AND p.received_at > TIMESTAMP("{start_timestamp}")
                  AND p.received_at > TIMESTAMP(@timestamp)
                WHERE s.sent_at > TIMESTAMP("{start_timestamp}")
                  AND s.address = @maker_address
                  AND TIMESTAMP_TRUNC(s.sent_at, DAY) > TIMESTAMP(TIMESTAMP_ADD(CURRENT_DATE, INTERVAL -1 DAY))
                UNION ALL
                SELECT p.received_at
                    , TIMESTAMP_DIFF(p.received_at, s.sent_at, millisecond) AS latency
                    , "indexer" AS server_address
                FROM `{project_id}.latency_experiments.long_running_two_sided_orders` s
                JOIN `{project_id}.indexer_stream.received_orders_and_cancels` p
                    ON p.client_id = CAST(s.client_id AS STRING)
                   AND p.address = s.address
                   AND p.received_at > TIMESTAMP("{start_timestamp}")
                  AND p.received_at > TIMESTAMP(@timestamp)
                WHERE s.sent_at > TIMESTAMP("{start_timestamp}")
                  AND s.address = @maker_address
                  AND TIMESTAMP_TRUNC(s.sent_at, DAY) > TIMESTAMP(TIMESTAMP_ADD(CURRENT_DATE, INTERVAL -1 DAY))
                ORDER BY 1
        """.format(
            start_timestamp=START_TIMESTAMP,
            project_id=PROJECT_ID,
        ),
        "params": {"maker_address": config["maker_address"]},
        "metric_name": "bigquery.short_term_order_latency",
    },
    {
        "name": "stateful_order_latency",
        "query": """
            SELECT p.received_at
                , TIMESTAMP_DIFF(p.received_at, s.sent_at, millisecond) AS latency
                , p.server_address
            FROM `{project_id}.latency_experiments.long_running_stateful_orders` s
            JOIN `{project_id}.full_node_stream.order_places` p
                ON p.client_id = CAST(s.client_id AS STRING)
            AND p.address = s.address
            AND p.received_at > TIMESTAMP("{start_timestamp}")
            AND p.received_at > TIMESTAMP(@timestamp)
            WHERE s.sent_at > TIMESTAMP("{start_timestamp}")
            AND s.address = @stateful_address
            AND TIMESTAMP_TRUNC(s.sent_at, DAY) > TIMESTAMP(TIMESTAMP_ADD(CURRENT_DATE, INTERVAL -1 DAY))
            UNION ALL
            SELECT p.received_at
                , TIMESTAMP_DIFF(p.received_at, s.sent_at, millisecond) AS latency
                , "indexer" AS server_address
            FROM `{project_id}.latency_experiments.long_running_stateful_orders` s
            JOIN `{project_id}.indexer_stream.received_orders_and_cancels` p
                ON p.client_id = CAST(s.client_id AS STRING)
            AND p.address = s.address
            AND p.received_at > TIMESTAMP("{start_timestamp}")
            AND p.received_at > TIMESTAMP(@timestamp)
            WHERE s.sent_at > TIMESTAMP("{start_timestamp}")
            AND s.address = @stateful_address
            AND TIMESTAMP_TRUNC(s.sent_at, DAY) > TIMESTAMP(TIMESTAMP_ADD(CURRENT_DATE, INTERVAL -1 DAY))
            ORDER BY 1
        """.format(
            start_timestamp=START_TIMESTAMP,
            project_id=PROJECT_ID,
        ),
        "params": {"stateful_address": config["stateful_address"]},
        "metric_name": "bigquery.stateful_order_latency",
    },
]

DATADOG_API_KEY = config["dd_api_key"]
DATADOG_APP_KEY = config["dd_app_key"]
POLL_INTERVAL = 60  # 1 minute
STATE_FILE = "last_processed_timestamps.json"

# Initialize Datadog API
options = {"api_key": DATADOG_API_KEY, "app_key": DATADOG_APP_KEY}
initialize(**options)


def load_last_processed_timestamps():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return {query["name"]: START_TIMESTAMP for query in QUERIES}


def save_last_processed_timestamps(timestamps):
    with open(STATE_FILE, "w") as f:
        json.dump(timestamps, f)


def run_query(client, query, params, last_processed_timestamp):
    params["timestamp"] = last_processed_timestamp
    query_job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(key, "STRING", value)
                for key, value in params.items()
            ]
        ),
    )
    results = query_job.result()
    return results


def monitor():
    last_processed_timestamps = load_last_processed_timestamps()
    client = bigquery.Client(project=PROJECT_ID)

    while True:
        try:
            for query_info in QUERIES:
                query_name = query_info["name"]
                query = query_info["query"]
                params = query_info["params"]
                metric_name = query_info["metric_name"]
                last_processed_timestamp = last_processed_timestamps.get(
                    query_name, START_TIMESTAMP
                )
                results = run_query(client, query, params, last_processed_timestamp)
                points = []
                metrics = []
                latest_timestamp = last_processed_timestamp

                # loop through results
                for row in results:
                    timestamp = int(row["received_at"].timestamp())
                    points.append((timestamp, [row["latency"]]))
                    latest_timestamp = row["received_at"]
                    tags = [
                        f"server_address:{row['server_address']}",
                        f"service_name:v4-latency-scripts",
                        "environment:mainnet",
                    ]
                    metric = {
                        "metric": metric_name,
                        "points": [(timestamp, row["latency"])],
                        "tags": tags,
                    }
                    metrics.append(metric)

                if points:
                    tags = [
                        f"server_address:{row['server_address']}",
                        "service_name:v4-latency-scripts",
                        "environment:test",
                    ]
                    logging.info(f"Sending {len(points)} latency points to Datadog")
                    api.Distribution.send(metric=metric_name + "_dist", points=points, tags=tags)

                if metrics:
                    logging.info("sending_metrics")
                    api.Metric.send(metrics)

                last_processed_timestamps[query_name] = str(latest_timestamp)
            save_last_processed_timestamps(last_processed_timestamps)
            time.sleep(POLL_INTERVAL)
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    monitor()
