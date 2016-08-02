#!/usr/bin/env bash

# Set environment variables here.

# This script sets variables multiple times over the course of starting an hbase process,
# so try to keep things idempotent unless you want to take an even deeper look
# into the startup scripts (bin/hbase, etc.)

# The java implementation to use.  Java 1.6 required.
# export JAVA_HOME=/usr/java/jdk1.6.0/
. ~/.bash_profile

# Extra Java CLASSPATH elements.  Optional. 
 
export ESTORM_CLASSPATH=$ESTORM_HOME/classes:$STORM_HOME/conf 
 
for f in  $STORM_HOME/storm-*.jar $STORM_HOME/lib/*.jar ; do 
    ESTORM_CLASSPATH=${ESTORM_CLASSPATH}:$f; 
done


export ESTORM_HEAPSIZE=1000

if [ "$STORM_PID_DIR" = "" ]; then
export STORM_PID_DIR=$STORM_HOME/logs
fi
if [ "$STORM_LOG_DIR" = "" ]; then
export STORM_LOG_DIR=$STORM_HOME/logs
fi

export STORM_PID_DIR=$STORM_PID_DIR
export STORM_LOG_DIR="$STORM_LOG_DIR"
