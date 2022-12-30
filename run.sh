#!/bin/bash

docker-compose up therapist_processor && \
./run_aggregator.sh total_ther && \
docker-compose up interaction_processor && \
./run_aggregator.sh active_ther && \
docker-compose up active_ther_processor && \
docker-compose up rate_processor && \
docker-compose up sync_back