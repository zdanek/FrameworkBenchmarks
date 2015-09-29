#!/bin/bash

fw_depends java8 vertx

# load java environment variables
#source $IROOT/java8.installed

#sed -i 's|host: \x27.*\x27|host: \x27'"${DBHOST}"'\x27|g' app.groovy

export JAVA_OPTS="-server -XX:+UseNUMA -XX:+UseParallelGC -XX:+AggressiveOpts"
${VERTX_HOME}/bin/vertx run app.groovy &
