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


function run_datanode {
    echo "Waiting for 'namenode' to be ready..."
    ping_service "namenode" "9870" "60"

    datadir=`echo $HDFS_CONF_dfs_datanode_data_dir | perl -pe 's#file://##'`
    if [ ! -d $datadir ]; then
        echo "Datanode data directory not found: $datadir"
        exit 2
    fi

    $HADOOP_HOME/bin/hdfs --config $HADOOP_CONF_DIR datanode
}


# Main
run_datanode
