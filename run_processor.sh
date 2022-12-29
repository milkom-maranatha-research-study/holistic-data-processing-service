#!/bin/bash

# Load dot env for the processor app
. .env

python "$PWD/data_processor/processor.py"
