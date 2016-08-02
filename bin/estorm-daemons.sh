#!/usr/bin/env bash
#
#
# Run a estorm command on all master hosts.
# Modelled after $ESTORM_HOME/bin/estorm-daemons.sh

usage="Usage: estorm-daemons.sh [--config <estorm-confdir>] \
 [--hosts serversfile] [start|stop] command args..."

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin">/dev/null; pwd`

. $bin/hbase-config.sh

remote_cmd="cd ${ESTORM_HOME}; $bin/estorm-daemon.sh --config ${ESTORM_CONF_DIR} $@"
args="--hosts ${ESTORM_SERVERS} --config ${ESTORM_CONF_DIR} $remote_cmd"

command=$2
case $command in
  (masters|servers)
    exec "$bin/master.sh" $args
    ;;
  (*)
   # exec "$bin/servers.sh" $args
    ;;
esac

