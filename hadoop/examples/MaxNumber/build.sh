#!/bin/bash

export HADS=/usr/local/hadoop/share/hadoop/
export CLASSPATH=${HADS}common/hadoop-common-2.8.5.jar:${HADS}mapreduce/hadoop-mapreduce-client-core-2.8.5.jar:${HADS}common/lib/commons-cli-1.2.jar

javac -classpath $CLASSPATH -d bin/ src/*.java

cd bin

jar -cvf MaxNumber.jar .

cd ..

cp -f bin/MaxNumber.jar .
