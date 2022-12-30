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
    INPUT_ORG_THERAPIST_PATH="input/org-therapist"
    INPUT_NAMENODE_ORG_THERAPIST_PATH="tmp/org-therapist"

    docker cp "${INPUT_ORG_THERAPIST_PATH}" namenode:/"${INPUT_NAMENODE_ORG_THERAPIST_PATH}/"

    echo "Create HDFS 'input' dir in the 'namenode' service"
    docker exec -it namenode bash hdfs dfs -mkdir /user/root/input

    echo "Copy '${INPUT_NAMENODE_ORG_THERAPIST_PATH}' files to the HDFS input dir..."
    docker exec -it namenode bash hdfs dfs -put "${INPUT_NAMENODE_ORG_THERAPIST_PATH}" /user/root/input
}


function run_therapist_aggregator {
    AGGREGATE_TYPE="$1"

    if [[ $AGGREGATE_TYPE =~ ^(all)$ ]]; then
        echo "Starting 'data_aggregator' service to calculate number of therapists..."
    elif [[ $AGGREGATE_TYPE =~ ^(per-org)$ ]]; then
        echo "Starting 'data_aggregator' service to calculate number of therapists per Organization..."
    fi

    echo "Run MR Job..."
    INPUT_PATH="input/org-therapist/"
    OUTPUT_PATH="output/org-therapist/"
    AGG_JAVA_CLASS="data.aggregator.app.byorg.TherapistAggregatorDriver"

    docker exec -it namenode hadoop jar tmp/data_aggregator_app.jar "${AGG_JAVA_CLASS}" "${INPUT_PATH}" "${OUTPUT_PATH}" "${AGGREGATE_TYPE}"

    echo "Export MR Job outputs..."
    OUTPUT_AGGREGATE_PATH="output/org/num-of-ther"
    OUTPUT_AGGREGATE_FILENAME="org-${AGGREGATE_TYPE}-aggregate.csv"

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
    NAMENODE_OUTPUT_FILENAME="org-${AGGREGATE_TYPE}-aggregate.csv"
    docker exec -it namenode rm -f "${NAMENODE_OUTPUT_FILENAME}"

    echo "Clean up 'tmp' files..."
    NAMENODE_TMP_INPUT_PATH="/tmp/org-therapist"
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

    echo "Preparing ${PERIOD_TYPE} active therapists Hadoop requirements..."

    echo "Create folder structure to allocate input files in the 'namenode' service..."
    docker exec -it namenode bash hdfs dfs -mkdir -p /user/root

    echo "Copy 'data_aggregator_app.jar' file into the 'tmp' dir in the 'namenode' service..."
    docker cp data_aggregator_app.jar namenode:/tmp

    echo "Copy ${PERIOD_TYPE} input files into the 'tmp' dir in the 'namenode' service..."
    INPUT_ORG_INTERACTION_PATH="input/org-interaction"
    INPUT_NAMENODE_ORG_INTERACTION_PATH="tmp/${PERIOD_TYPE}-org-interaction"
    docker cp "${INPUT_ORG_INTERACTION_PATH}/${PERIOD_TYPE}" namenode:/"${INPUT_NAMENODE_ORG_INTERACTION_PATH}/"

    echo "Create HDFS 'input' dir in the 'namenode' service"
    docker exec -it namenode bash hdfs dfs -mkdir /user/root/input

    echo "Copy '${INPUT_NAMENODE_ORG_INTERACTION_PATH}' files to the HDFS input dir..."
    docker exec -it namenode bash hdfs dfs -put "${INPUT_NAMENODE_ORG_INTERACTION_PATH}" /user/root/input
}


function run_active_therapist_aggregator {
    PERIOD_TYPE="$1"

    echo "Starting 'data_aggregator' service to calculate active/inactive therapists..."

    echo "Run MR Job on ${PERIOD_TYPE} period..."
    INPUT_PATH="input/${PERIOD_TYPE}-org-interaction/"
    OUTPUT_PATH="output/${PERIOD_TYPE}-org-interaction/"
    AGG_JAVA_CLASS="data.aggregator.app.byorg.ActiveTherapistAggregatorDriver"

    docker exec -it namenode hadoop jar tmp/data_aggregator_app.jar "${AGG_JAVA_CLASS}" "${INPUT_PATH}" "${OUTPUT_PATH}" "${PERIOD_TYPE}"

    echo "Export MR Job outputs..."
    OUTPUT_AGGREGATE_PATH="output/org/active-ther-aggregate"
    OUTPUT_AGGREGATE_FILENAME="org-active-ther-${PERIOD_TYPE}-aggregate.csv"

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
    NAMENODE_OUTPUT_FILENAME="org-active-ther-${PERIOD_TYPE}-aggregate.csv"
    docker exec -it namenode rm -f "${NAMENODE_OUTPUT_FILENAME}"

    echo "Clean up 'tmp' files..."
    NAMENODE_INPUT_ORG_INTERACTION_PATH="/tmp/${PERIOD_TYPE}-org-interaction"
    docker exec -it namenode rm -rf "${NAMENODE_INPUT_ORG_INTERACTION_PATH}"
    docker exec -it namenode rm -f /tmp/data_aggregator_app.jar
}


function active_therapist_aggregator_main {
    echo "MR Job - All-time Aggregate..."
    prepare_active_therapist_hadoop_requirements "alltime"
    run_active_therapist_aggregator "alltime"
    cleanup_active_therapist_aggregator "alltime"

    echo "MR Job - Weekly Aggregate per Organization..."
    prepare_active_therapist_hadoop_requirements "weekly"
    run_active_therapist_aggregator "weekly"
    cleanup_active_therapist_aggregator "weekly"

    echo "MR Job - Monthly Aggregate per Organization..."
    prepare_active_therapist_hadoop_requirements "monthly"
    run_active_therapist_aggregator "monthly"
    cleanup_active_therapist_aggregator "monthly"

    echo "MR Job - Yearly Aggregate per Organization..."
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
