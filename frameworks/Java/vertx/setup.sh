#!/bin/bash

fw_depends java8 maven

# load java environment variables
#source $IROOT/java8.installed

#sed -i 's|host: \x27.*\x27|host: \x27'"${DBHOST}"'\x27|g' app.groovy

export JAVA_OPTS="-server -XX:+UseNUMA -XX:+UseParallelGC -XX:+AggressiveOpts"
mvn clean package
java -jar target/hellovertx-0.0.1-SNAPSHOT-jar-with-dependencies.jar &
#${VERTX_HOME}/bin/vertx run app.groovy &
