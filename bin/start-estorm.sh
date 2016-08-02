#!/usr/bin/env bash
#

# Modelled after $ESTORM_HOME/bin/start-estorm.sh.

# Start hadoop hbase daemons.
# Run this on master node.
usage="Usage: start-estorm.sh"

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin">/dev/null; pwd`

. "$bin"/estorm-config.sh

# start ESTORM daemons
errCode=$?
if [ $errCode -ne 0 ]
then
  exit $errCode
fi


if [ "$1" = "autorestart" ]
then
  commandToRun="autorestart"
else
  commandToRun="start"
fi



"$bin"/estorm-daemons.sh --config "${ESTORM_CONF_DIR}" --hosts "${ESTORM_MASTERS}" $commandToRun master


