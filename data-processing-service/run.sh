#!/bin/bash


function ping_service {
    HOST=$1
    PORT=$2
    TIMEOUT=$3

    until nc -w $TIMEOUT -z $HOST $PORT; do
        echo "Connection to ${HOST}:${PORT} was failed"
        sleep 1
    done

    echo "${HOST} is ready to be used!"
}


function run_dps_service {
    echo "Waiting for 'namenode' to be ready..."
    ping_service "namenode" "9870" "60"

    echo "Waiting for 'datanode1' to be ready..."
    ping_service "datanode1" "9864" "60"

    echo "Waiting for 'nodemanager1' to be ready..."
    ping_service "nodemanager1" "8042" "60"

    echo "Waiting for 'resourcemanager' to be ready..."
    ping_service "resourcemanager" "8088" "60"

    echo "Starting to run 'dps' service..."
    # $HADOOP_HOME/bin/hadoop jar $JAR_FILEPATH $CLASS_TO_RUN $PARAMS

    # TODO: Remove while-loop once DPS Jar is ready!
    while true; do
        sleep 1
    done
}


# Main
run_dps_service
