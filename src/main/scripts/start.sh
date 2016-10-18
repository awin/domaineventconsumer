#!/bin/bash

SCRIPTDIR=$(dirname "$0")
BASEDIR=$(dirname "$SCRIPTDIR")
LOGDIR=/srv/log/highlevelconsumer

JAR_FILE="/app/target/highlevelconsumer-*.jar"

if [ "$zanox_stage" == "live" ]; then
    zanox_stage="production"
elif [ "$zanox_stage" == "dev" ]; then
    zanox_stage="development"
fi

if [ ! -d "$LOGDIR" ]; then
  mkdir -p $LOGDIR
fi

exec java -D"spring.profiles.active=$zanox_stage" -jar $JAR_FILE 2>> $LOGDIR/main.log 1>> $LOGDIR/info.log
