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


function run_nodemanager {
    echo "Waiting for 'namenode' to be ready..."
    ping_service "namenode" "9870" "60"

    echo "Waiting for 'datanode1' to be ready..."
    ping_service "datanode1" "9864" "60"

    echo "Starting to run 'nodemanager' service..."
    $HADOOP_HOME/bin/yarn --config $HADOOP_CONF_DIR nodemanager
}


# Main
run_nodemanager
