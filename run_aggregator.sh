#!/bin/bash


# ===============================================================================================
# Number of therapists Aggregator
# ===============================================================================================


function prepare_therapist_hadoop_requirements {
    echo "Preparing therapists Hadoop requirements..."

    echo "Create folder structure to allocate input files in the 'namenode' service..."
    docker exec -it namenode bash hdfs dfs -mkdir -p /user/root

    echo "Copy 'data_aggregator_app.jar' file into the 'tmp' dir in the 'namenode' service..."
    docker cp data_aggregator_app.jar namenode:/tmp

    echo "Copy ${PERIOD_TYPE} input files into the 'tmp' dir in the 'namenode' service..."
    docker cp input/therapist namenode:/tmp/therapist/

    echo "Create HDFS 'input' dir in the 'namenode' service"
    docker exec -it namenode bash hdfs dfs -mkdir /user/root/input

    echo "Copy 'tmp/therapist/' files to the HDFS input dir..."
    docker exec -it namenode bash hdfs dfs -put tmp/therapist /user/root/input
}


function run_therapist_aggregator {
    AGGREGATE_TYPE="$1"

    if [[ $AGGREGATE_TYPE =~ ^(all)$ ]]; then
        echo "Starting 'data_aggregator' service to calculate number of therapists in NiceDay..."
    elif [[ $AGGREGATE_TYPE =~ ^(per-org)$ ]]; then
        echo "Starting 'data_aggregator' service to calculate number of therapists per Organization..."
    fi

    echo "Run MR Job..."
    INPUT_PATH="input/therapist/"
    OUTPUT_PATH="output/therapist/"
    docker exec -it namenode hadoop jar tmp/data_aggregator_app.jar data.aggregator.app.TherapistAggregatorDriver "${INPUT_PATH}" "${OUTPUT_PATH}" "${AGGREGATE_TYPE}"

    echo "Export MR Job outputs..."

    # Create 'output' directory if it doesn't exists
    OUTPUT_AGGREGATE_PATH="output/num_of_ther"
    mkdir -p "${OUTPUT_AGGREGATE_PATH}"

    docker exec -it namenode hadoop fs -getmerge "/user/root/${OUTPUT_PATH}" "${AGGREGATE_TYPE}-aggregate.csv"
    docker exec -it namenode cat "${AGGREGATE_TYPE}-aggregate.csv" > "${OUTPUT_AGGREGATE_PATH}/${AGGREGATE_TYPE}-aggregate.csv"
}


function cleanup_therapist_aggregator {
    AGGREGATE_TYPE="$1"

    echo "Clean up HDFS input/output..."
    docker exec -it namenode bash hdfs dfs -rm -r /user/root/input
    docker exec -it namenode bash hdfs dfs -rm -r /user/root/output

    echo "Clean up 'namenode' output file..."
    docker exec -it namenode rm -f "${AGGREGATE_TYPE}-aggregate.csv"

    echo "Clean up 'tmp' files..."
    TMP_INPUT_PATH="/tmp/therapist"
    docker exec -it namenode rm -rf "${TMP_INPUT_PATH}"
    docker exec -it namenode rm -f /tmp/data_aggregator_app.jar
}


function therapist_aggregator_main {
    echo "MR Job - Aggregate all therapist..."
    prepare_therapist_hadoop_requirements
    run_therapist_aggregator "all"
    cleanup_therapist_aggregator "all"

    echo "MR Job - Aggregate therapists per Organization..."
    prepare_therapist_hadoop_requirements
    run_therapist_aggregator "per-org"
    cleanup_therapist_aggregator "per-org"
}


# ===============================================================================================
# Active/Inactive Therapists Aggregator
# ===============================================================================================


function prepare_active_therapist_hadoop_requirements {
    PERIOD_TYPE="$1"

    echo "Preparing ${PERIOD_TYPE} active therapists Hadoop requirements..."

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


function run_active_therapist_aggregator {
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


function cleanup_active_therapist_aggregator {
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


function active_therapist_aggregator_main {
    echo "MR Job - All-Time Aggregate..."
    prepare_active_therapist_hadoop_requirements "alltime"
    run_active_therapist_aggregator "alltime"
    cleanup_active_therapist_aggregator "alltime"

    echo "MR Job - Weekly Aggregate..."
    prepare_active_therapist_hadoop_requirements "weekly"
    run_active_therapist_aggregator "weekly"
    cleanup_active_therapist_aggregator "weekly"

    echo "MR Job - Monthly Aggregate..."
    prepare_active_therapist_hadoop_requirements "monthly"
    run_active_therapist_aggregator "monthly"
    cleanup_active_therapist_aggregator "monthly"

    echo "MR Job - Yearly Aggregate..."
    prepare_active_therapist_hadoop_requirements "yearly"
    run_active_therapist_aggregator "yearly"
    cleanup_active_therapist_aggregator "yearly"
}


# ===============================================================================================
# Main Scripts
# ===============================================================================================

TYPE="$1"

if [[ $TYPE =~ ^(total_ther)$ ]]; then
    therapist_aggregator_main

elif [[ $TYPE =~ ^(active_ther)$ ]]; then
    active_therapist_aggregator_main

else
    echo "Invalid aggregate type."
    exit 1
fi
