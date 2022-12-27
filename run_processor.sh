#!/bin/bash

RUN_LOCALLY="$1"


function run_data_processor {
    if [[ $RUN_LOCALLY =~ ^(locally)$ ]]; then
        . .env

        export PYTHONPATH="$PWD/data_processor"
    fi

    echo $PYTHONPATH
    python "$PWD/data_processor/main.py"
}


# Main
run_data_processor
