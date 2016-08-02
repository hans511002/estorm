#!/usr/bin/env bash
#
# Modelled after $ESTORM_HOME/bin/stop-estorm.sh.

# Stop storm estorm daemons.  Run this on master node.

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin">/dev/null; pwd`

. "$bin"/estorm-config.sh
. "$bin"/estorm-common.sh

# variables needed for stop command
if [ "$ESTORM_LOG_DIR" = "" ]; then
  export ESTORM_LOG_DIR="$ESTORM_HOME/logs"
fi
mkdir -p "$ESTORM_LOG_DIR"

if [ "$ESTORM_IDENT_STRING" = "" ]; then
  export ESTORM_IDENT_STRING="$USER"
fi

export ESTORM_LOG_PREFIX=hbase-$ESTORM_IDENT_STRING-master-$HOSTNAME
export ESTORM_LOGFILE=$ESTORM_LOG_PREFIX.log
logout=$ESTORM_LOG_DIR/$ESTORM_LOG_PREFIX.out
loglog="${ESTORM_LOG_DIR}/${ESTORM_LOGFILE}"
pid=${ESTORM_PID_DIR:-/tmp}/estorm-$ESTORM_IDENT_STRING-master.pid

echo -n stopping ESTORM
echo "`date` Stopping estorm (via master)" >> $loglog

nohup nice -n ${ESTORM_NICENESS:-0} "$ESTORM_HOME"/bin/estorm \
   --config "${ESTORM_CONF_DIR}" \
   master stop "$@" > "$logout" 2>&1 < /dev/null &

waitForProcessEnd `cat $pid` 'stop-master-command'

rm -f $pid
