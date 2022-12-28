#!/bin/bash

# Load dot env for sync-back app
. .env.sync_back

python "$PWD/data_processor/sync_back.py"
