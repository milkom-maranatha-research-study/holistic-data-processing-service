#!/bin/bash

TYPE="$1"

if [[ $TYPE =~ ^(locally)$ ]]; then
    # Load dot env for the sync-back app
    . .env

    # Overrides BACKEND_URL
    export BACKEND_URL=http://localhost:8080

    # Registers data processing service module to the python path
    export PYTHONPATH="${PYTHONPATH}:${PWD}"
fi

python "$PWD/data_processing/sync_back_app/main.py"
