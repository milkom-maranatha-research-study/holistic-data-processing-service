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
    INPUT_THERAPIST_PATH="input/therapist"
    INPUT_NAMENODE_THERAPIST_PATH="tmp/therapist"

    docker cp "${INPUT_THERAPIST_PATH}" namenode:/"${INPUT_NAMENODE_THERAPIST_PATH}/"

    echo "Create HDFS 'input' dir in the 'namenode' service"
    docker exec -it namenode bash hdfs dfs -mkdir /user/root/input

    echo "Copy '${INPUT_NAMENODE_THERAPIST_PATH}' files to the HDFS input dir..."
    docker exec -it namenode bash hdfs dfs -put "${INPUT_NAMENODE_THERAPIST_PATH}" /user/root/input
}


function run_therapist_aggregator {
    AGGREGATE_TYPE="$1"

    if [[ $AGGREGATE_TYPE =~ ^(all)$ ]]; then
        echo "Starting 'data_aggregator' service to calculate number of therapists..."
    elif [[ $AGGREGATE_TYPE =~ ^(per-org)$ ]]; then
        echo "Starting 'data_aggregator' service to calculate number of therapists per Organization..."
    fi

    echo "Run MR Job..."
    INPUT_PATH="input/therapist/"
    OUTPUT_PATH="output/therapist/"
    AGG_JAVA_CLASS="data.aggregator.app.TherapistAggregatorDriver"

    docker exec -it namenode hadoop jar tmp/data_aggregator_app.jar "${AGG_JAVA_CLASS}" "${INPUT_PATH}" "${OUTPUT_PATH}" "${AGGREGATE_TYPE}"

    echo "Export MR Job outputs..."
    OUTPUT_AGGREGATE_PATH="output/num-of-ther"
    OUTPUT_AGGREGATE_FILENAME="${AGGREGATE_TYPE}-aggregate.csv"

    # Create 'output' directory if it doesn't exists
    mkdir -p "${OUTPUT_AGGREGATE_PATH}"

    docker exec -it namenode hadoop fs -getmerge "/user/root/${OUTPUT_PATH}" "${OUTPUT_AGGREGATE_FILENAME}"
    docker exec -it namenode cat "${OUTPUT_AGGREGATE_FILENAME}" > "${OUTPUT_AGGREGATE_PATH}/${OUTPUT_AGGREGATE_FILENAME}"
}


function cleanup_therapist_aggregator {
    AGGREGATE_TYPE="$1"

    echo "Clean up HDFS input/output..."
    docker exec -it namenode bash hdfs dfs -rm -r /user/root/input
    docker exec -it namenode bash hdfs dfs -rm -r /user/root/output

    echo "Clean up 'namenode' output file..."
    NAMENODE_OUTPUT_FILENAME="${AGGREGATE_TYPE}-aggregate.csv"
    docker exec -it namenode rm -f "${NAMENODE_OUTPUT_FILENAME}"

    echo "Clean up 'tmp' files..."
    NAMENODE_TMP_INPUT_PATH="/tmp/therapist"
    docker exec -it namenode rm -rf "${NAMENODE_TMP_INPUT_PATH}"
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
    INPUT_DIR="$2"

    INPUT_PATH="input/interaction/${INPUT_DIR}"
    INPUT_NAMENODE_PATH="tmp/${PERIOD_TYPE}-interaction"

    echo "Preparing ${PERIOD_TYPE} active therapists Hadoop requirements..."

    echo "Create folder structure to allocate input files in the 'namenode' service..."
    docker exec -it namenode bash hdfs dfs -mkdir -p /user/root

    echo "Copy 'data_aggregator_app.jar' file into the 'tmp' dir in the 'namenode' service..."
    docker cp data_aggregator_app.jar namenode:/tmp

    echo "Copy ${PERIOD_TYPE} input files from ${INPUT_PATH} into the 'tmp' dir in the 'namenode' service..."
    docker cp "${INPUT_PATH}" namenode:/"${INPUT_NAMENODE_PATH}/"

    echo "Create HDFS 'input' dir in the 'namenode' service"
    docker exec -it namenode bash hdfs dfs -mkdir /user/root/input

    echo "Copy '${INPUT_NAMENODE_PATH}' files to the HDFS input dir..."
    docker exec -it namenode bash hdfs dfs -put "${INPUT_NAMENODE_PATH}" /user/root/input
}


function run_active_therapist_aggregator {
    PERIOD_TYPE="$1"
    AGGREGATE_TYPE="$2"
    OUTPUT_DIR="$3"

    echo "Run MR Job on ${PERIOD_TYPE} period..."

    INPUT_PATH="input/${PERIOD_TYPE}-interaction/"
    OUTPUT_PATH="output/${PERIOD_TYPE}-interaction/"

    if [[ $AGGREGATE_TYPE =~ ^(all|per-app)$ ]]; then
        AGG_JAVA_CLASS="data.aggregator.app.bynd.ActiveTherapistAggregatorDriver"

    elif [[ $AGGREGATE_TYPE =~ ^(per-org)$ ]]; then
        AGG_JAVA_CLASS="data.aggregator.app.byorg.ActiveTherapistAggregatorDriver"
    fi

    docker exec -it namenode hadoop jar tmp/data_aggregator_app.jar "${AGG_JAVA_CLASS}" "${INPUT_PATH}" "${OUTPUT_PATH}" "${PERIOD_TYPE}"

    echo "Export MR Job outputs..."
    OUTPUT_AGGREGATE_PATH="output/active-ther/${OUTPUT_DIR}"
    OUTPUT_AGGREGATE_FILENAME="active-ther-${PERIOD_TYPE}-aggregate.csv"

    # Create 'output' directory if it doesn't exists
    mkdir -p "${OUTPUT_AGGREGATE_PATH}"

    docker exec -it namenode hadoop fs -getmerge "/user/root/${OUTPUT_PATH}" "${OUTPUT_AGGREGATE_FILENAME}"
    docker exec -it namenode cat "${OUTPUT_AGGREGATE_FILENAME}" > "${OUTPUT_AGGREGATE_PATH}/${OUTPUT_AGGREGATE_FILENAME}"
}


function cleanup_active_therapist_aggregator {
    PERIOD_TYPE="$1"

    echo "Clean up HDFS input/output..."
    docker exec -it namenode bash hdfs dfs -rm -r /user/root/input
    docker exec -it namenode bash hdfs dfs -rm -r /user/root/output

    echo "Clean up 'namenode' output file..."
    NAMENODE_OUTPUT_FILENAME="active-ther-${PERIOD_TYPE}-aggregate.csv"
    docker exec -it namenode rm -f "${NAMENODE_OUTPUT_FILENAME}"

    echo "Clean up 'tmp' files..."
    INPUT_NAMENODE_PATH="tmp/${PERIOD_TYPE}-interaction"
    docker exec -it namenode rm -rf "${INPUT_NAMENODE_PATH}"
    docker exec -it namenode rm -f /tmp/data_aggregator_app.jar
}


function active_therapist_aggregator_main {
    echo "MR Job - All-time Aggregate..."
    prepare_active_therapist_hadoop_requirements "alltime" "alltime"
    run_active_therapist_aggregator "alltime" "all" "alltime"
    cleanup_active_therapist_aggregator "alltime"

    echo "MR Job - Weekly Aggregate per NiceDay application..."
    prepare_active_therapist_hadoop_requirements "weekly" "by-app/weekly"
    run_active_therapist_aggregator "weekly" "per-app" "by-app/weekly"
    cleanup_active_therapist_aggregator "weekly"

    echo "MR Job - Monthly Aggregate per NiceDay application..."
    prepare_active_therapist_hadoop_requirements "monthly" "by-app/monthly"
    run_active_therapist_aggregator "monthly" "per-app" "by-app/monthly"
    cleanup_active_therapist_aggregator "monthly"

    echo "MR Job - Yearly Aggregate per NiceDay application..."
    prepare_active_therapist_hadoop_requirements "yearly" "by-app/yearly"
    run_active_therapist_aggregator "yearly" "per-app" "by-app/yearly"
    cleanup_active_therapist_aggregator "yearly"

    echo "MR Job - Weekly Aggregate per Organization..."
    prepare_active_therapist_hadoop_requirements "weekly" "by-org/weekly"
    run_active_therapist_aggregator "weekly" "per-org" "by-org/weekly"
    cleanup_active_therapist_aggregator "weekly"

    echo "MR Job - Monthly Aggregate per Organization..."
    prepare_active_therapist_hadoop_requirements "monthly" "by-org/monthly"
    run_active_therapist_aggregator "monthly" "per-org" "by-org/monthly"
    cleanup_active_therapist_aggregator "monthly"

    echo "MR Job - Yearly Aggregate per Organization..."
    prepare_active_therapist_hadoop_requirements "yearly" "by-org/yearly"
    run_active_therapist_aggregator "yearly" "per-org" "by-org/yearly"
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
