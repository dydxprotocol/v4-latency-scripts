# Overview
This repo contains various scripts to understand the latency of networks using the dYdX Chain software.
1. `listen_to_grpc_stream.py` - listens to a gRPC stream from a full node and writes and writes the received data to BigQuery.
2. `listen_to_websocket_stream.py` - listens to the indexer's websocket stream and writes and writes the received data to BigQuery.
3. `place_orders.py` - sends a new order to a public grpc node every block
4. `place_replacement_orders.py` - sends a replacement order to a public grpc node every block
5. `run_all_scripts.py` - runs the above scripts and queries BigQuery to ensure each of the scripts are still running

# Code setup
Starting from a manually created ec2 machine
```
sudo yum install git -y
# make sure to use personal access tokens (user/password no longer works)
git clone https://github.com/dydxprotocol/full-node-client.git
sudo yum install python3-pip -y
sudo pip3 install virtualenv
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

# Setup google cloud account
Add this to `~/.bashrc`. The [service account](https://cloud.google.com/iam/docs/service-accounts-create) should have "BigQuery Admin" access or similar.
- `export GOOGLE_APPLICATION_CREDENTIALS="/home/ec2-user/full-node-client/service-account.json"`

# Running it
```
# this runs all the scripts and restarts if necessary
python run_all_scripts.py
```
