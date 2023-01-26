#!/bin/bash

TYPE="$1"

if [[ $TYPE =~ ^(locally)$ ]]; then
    # Load dot env for the visualization app
    . .env

    # Overrides BACKEND_URL
    export BACKEND_URL=http://localhost:8080

    # Registers data processing service module to the python path
    export PYTHONPATH="${PYTHONPATH}:${PWD}"
fi

python "$PWD/data_processing/visualization_app/main.py"
