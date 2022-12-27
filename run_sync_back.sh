#!/bin/bash

RUN_LOCALLY="$1"


function run_sync_back {
    if [[ $RUN_LOCALLY =~ ^(locally)$ ]]; then
        . .env

        export PYTHONPATH="$PWD/data_sync_back"
    fi

    python "$PWD/data_sync_back/main.py"
}


# Main
run_sync_back
