#!/bin/bash

fw_depends java8 maven

# load java environment variables
#source $IROOT/java8.installed

sed -i 's|\"host\", \".*\"|\"host\", \"'"${DBHOST}"'\"|g' src/main/java/App.java

export JAVA_OPTS="-server -XX:+UseNUMA -XX:+UseParallelGC -XX:+AggressiveOpts"
mvn clean package
java -jar target/hellovertx-0.0.1-SNAPSHOT-jar-with-dependencies.jar &
