#!/bin/bash

if [ "$SPARK_WORKLOAD" == "master" ]; then
  echo "Starting Spark master..."
  /opt/spark/sbin/start-master.sh
fi

if [ "$SPARK_WORKLOAD" == "worker" ]; then
  echo "Starting Spark worker..."
  /opt/spark/sbin/start-worker.sh $SPARK_MASTER
fi

service ssh start

tail -f /dev/null
