#!/bin/bash

SCRIPTDIR=$(dirname "$0")
BASEDIR=$(dirname "$SCRIPTDIR")
LOGDIR=/srv/log/animated-octopus

# Config
TOPIC=test
THREADS=1
CONSUMER_GROUP=yt
ZOOKEEPER=d-lhr1-docker-001.zanox.com

JAR_FILE="/app/target/highlevelconsumer.jar"

if [ "$zanox_stage" == "live" ]; then
    zanox_stage="production"
elif [ "$zanox_stage" == "dev" ]; then
    zanox_stage="development"
fi

if [ ! -d "$LOGDIR" ]; then
  mkdir -p $LOGDIR
fi

exec java -cp $JAR_FILE com.zanox.kafka.highlevelconsumer.App $ZOOKEEPER $CONSUMER_GROUP $TOPIC $THREADS 2>> $LOGDIR/main.log 1>> $LOGDIR/info.log
