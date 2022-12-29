#!/bin/bash

# Load dot env for sync-back app
. .env.sync_back

# Registers data processing service module to the python path
export PYTHONPATH="${PYTHONPATH}:${PWD}"

python "$PWD/data_processor/sync_back_app/main.py"
