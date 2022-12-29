#!/bin/bash


function prepare_therapist_hadoop_requirements {
    PERIOD_TYPE="$1"

    echo "Preparing ${PERIOD_TYPE} therapists Hadoop requirements..."

    echo "Create folder structure to allocate input files in the 'namenode' service..."
    docker exec -it namenode bash hdfs dfs -mkdir -p /user/root

    echo "Copy 'data_aggregator_app.jar' file into the 'tmp' dir in the 'namenode' service..."
    docker cp data_aggregator_app.jar namenode:/tmp

    echo "Copy ${PERIOD_TYPE} input files into the 'tmp' dir in the 'namenode' service..."
    if [[ $PERIOD_TYPE =~ ^(alltime)$ ]]; then
        docker cp input/interaction/alltime namenode:/tmp/alltime-interaction/

    elif [[ $PERIOD_TYPE =~ ^(weekly)$ ]]; then
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
    if [[ $PERIOD_TYPE =~ ^(alltime)$ ]]; then
        docker exec -it namenode bash hdfs dfs -put tmp/alltime-interaction /user/root/input

    elif [[ $PERIOD_TYPE =~ ^(weekly)$ ]]; then
        docker exec -it namenode bash hdfs dfs -put tmp/weekly-interaction /user/root/input

    elif [[ $PERIOD_TYPE =~ ^(monthly)$ ]]; then
        docker exec -it namenode bash hdfs dfs -put tmp/monthly-interaction /user/root/input

    elif [[ $PERIOD_TYPE =~ ^(yearly)$ ]]; then
        docker exec -it namenode bash hdfs dfs -put tmp/yearly-interaction /user/root/input
    fi
}


function run_therapist_aggregator {
    PERIOD_TYPE="$1"

    echo "Starting 'data_aggregator' service to calculate active/inactive therapists..."

    echo "Run MR Job on ${PERIOD_TYPE} period..."
    INPUT_PATH="input/${PERIOD_TYPE}-interaction/"
    OUTPUT_PATH="output/${PERIOD_TYPE}-interaction/"
    docker exec -it namenode hadoop jar tmp/data_aggregator_app.jar data.aggregator.app.ActiveTherapistAggregatorDriver "${INPUT_PATH}" "${OUTPUT_PATH}" "${PERIOD_TYPE}"

    echo "Export MR Job outputs..."

    # Create 'output' directory if it doesn't exists
    OUTPUT_ACTIVE_INACTIVE_AGGREGATE_PATH="output/active-inactive"
    mkdir -p "${OUTPUT_ACTIVE_INACTIVE_AGGREGATE_PATH}"

    docker exec -it namenode hadoop fs -getmerge "/user/root/${OUTPUT_PATH}" "active-inactive-${PERIOD_TYPE}-aggregate.csv"
    docker exec -it namenode cat "active-inactive-${PERIOD_TYPE}-aggregate.csv" > "${OUTPUT_ACTIVE_INACTIVE_AGGREGATE_PATH}/${PERIOD_TYPE}-aggregate.csv"
}


function cleanup_therapist_aggregator {
    PERIOD_TYPE="$1"

    echo "Clean up HDFS input/output..."
    docker exec -it namenode bash hdfs dfs -rm -r /user/root/input
    docker exec -it namenode bash hdfs dfs -rm -r /user/root/output

    echo "Clean up 'namenode' output file..."
    docker exec -it namenode rm -f "active-inactive-${PERIOD_TYPE}-aggregate.csv"

    echo "Clean up 'tmp' files..."
    TMP_INPUT_PATH="/tmp/${PERIOD_TYPE}-interaction"
    docker exec -it namenode rm -rf "${TMP_INPUT_PATH}"
    docker exec -it namenode rm -f /tmp/data_aggregator_app.jar
}


function therapist_aggregator_main {
    echo "MR Job - All-Time Aggregate..."
    prepare_therapist_hadoop_requirements "alltime"
    run_therapist_aggregator "alltime"
    cleanup_therapist_aggregator "alltime"

    echo "MR Job - Weekly Aggregate..."
    prepare_therapist_hadoop_requirements "weekly"
    run_therapist_aggregator "weekly"
    cleanup_therapist_aggregator "weekly"

    echo "MR Job - Monthly Aggregate..."
    prepare_therapist_hadoop_requirements "monthly"
    run_therapist_aggregator "monthly"
    cleanup_therapist_aggregator "monthly"

    echo "MR Job - Yearly Aggregate..."
    prepare_therapist_hadoop_requirements "yearly"
    run_therapist_aggregator "yearly"
    cleanup_therapist_aggregator "yearly"
}


# Main
therapist_aggregator_main
