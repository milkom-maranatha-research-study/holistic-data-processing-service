#!/bin/bash

PERIOD_TYPE="$1"


function prepare_hadoop_requirements {
    echo "Preparing Hadoop requirements..."

    echo "Create folder structure to allocate input files in the 'namenode' service..."
    docker exec -it namenode bash hdfs dfs -mkdir -p /user/root

    echo "Copy 'data_aggregator_app.jar' file into the 'tmp' dir in the 'namenode' service..."
    docker cp data_aggregator_app.jar namenode:/tmp

    echo "Copy ${PERIOD_TYPE} input files into the 'tmp' dir in the 'namenode' service..."
    if [[ $PERIOD_TYPE =~ ^(weekly)$ ]]; then
        docker cp input/interaction/weekly namenode:/tmp/weekly-interaction/

    elif [[ $PERIOD_TYPE =~ ^(monthly)$ ]]; then
        docker cp input/interaction/monthly namenode:/tmp/monthly-interaction/

    elif [[ $PERIOD_TYPE =~ ^(yearly)$ ]]; then
        docker cp input/interaction/yearly namenode:/tmp/yearly-interaction/
    else
        echo "Invalid period type"
        exit 1
    fi

    echo "Create HDFS 'input' dir in the 'namenode' service"
    docker exec -it namenode bash hdfs dfs -mkdir /user/root/input

    echo "Copy 'tmp/input-${PERIOD_TYPE}-interaction/' files to the HDFS input dir..."
    if [[ $PERIOD_TYPE =~ ^(weekly)$ ]]; then
        docker exec -it namenode bash hdfs dfs -put tmp/weekly-interaction /user/root/input

    elif [[ $PERIOD_TYPE =~ ^(monthly)$ ]]; then
        docker exec -it namenode bash hdfs dfs -put tmp/monthly-interaction /user/root/input

    elif [[ $PERIOD_TYPE =~ ^(yearly)$ ]]; then
        docker exec -it namenode bash hdfs dfs -put tmp/yearly-interaction /user/root/input
    fi
}


function run_data_aggregator {
    echo "Starting to run 'data_aggregator' service..."

    echo "Run MR Job to aggregate active/inactive therapists on ${PERIOD_TYPE} period..."
    INPUT_PATH="input/${PERIOD_TYPE}-interaction/"
    OUTPUT_PATH="output/${PERIOD_TYPE}-interaction/"
    docker exec -it namenode hadoop jar tmp/data_aggregator_app.jar data.aggregator.app.TherapistAggregatorDriver "${INPUT_PATH}" "${OUTPUT_PATH}"

    echo "Export MR Job outputs..."
    docker exec -it namenode hadoop fs -getmerge "/user/root/${OUTPUT_PATH}" "${PERIOD_TYPE}-output.txt"
    docker exec -it namenode cat "${PERIOD_TYPE}-output.txt" > "${PERIOD_TYPE}-aggregate-output.txt"
}


function cleanup {
    echo "Clean up HDFS input/output..."
    docker exec -it namenode bash hdfs dfs -rm -r /user/root/input
    docker exec -it namenode bash hdfs dfs -rm -r /user/root/output

    echo "Clean up 'namenode' output file..."
    docker exec -it namenode rm -f "${PERIOD_TYPE}-output.txt"

    echo "Clean up 'tmp' files..."
    TMP_INPUT_PATH="/tmp/${PERIOD_TYPE}-interaction"
    docker exec -it namenode rm -rf "${TMP_INPUT_PATH}"
    docker exec -it namenode rm -f /tmp/data_aggregator_app.jar
}


# Main
prepare_hadoop_requirements
run_data_aggregator
cleanup
