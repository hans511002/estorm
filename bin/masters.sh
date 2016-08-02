#!/usr/bin/env bash
#
# Run a shell command on all master hosts.
#
# Environment Variables
#
#   ESTORM_MASTERS File naming remote hosts.
#     Default is ${HBASE_CONF_DIR}/masters
#   ESTORM_CONF_DIR  Alternate ESTORM conf dir. Default is ${ESTORM_HOME}/conf.
#   ESTORM_SLAVE_SLEEP Seconds to sleep between spawning remote commands.
#   ESTORM_SSH_OPTS Options passed to ssh when running remote commands.
#

usage="Usage: $0 [--config <estorm-confdir>] command..."

# if no args specified, show usage
if [ $# -le 0 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin">/dev/null; pwd`

. "$bin"/estorm-config.sh

# If the master backup file is specified in the command line,
# then it takes precedence over the definition in
# estorm-env.sh. Save it here.
HOSTLIST=$ESTORM_MASTERS

if [ "$HOSTLIST" = "" ]; then
  if [ "$ESTORM_MASTERS" = "" ]; then
    export HOSTLIST="${ESTORM_CONF_DIR}/masters"
  else
    export HOSTLIST="${ESTORM_MASTERS}"
  fi
fi


args=${@// /\\ }
args=${args/master-backup/master}

if [ -f $HOSTLIST ]; then
  for emaster in `cat "$HOSTLIST"`; do
   ssh $ESTORM_SSH_OPTS $emaster $"$args  " 2>&1 | sed "s/^/$emaster: /" &
   if [ "$ESTORM_SLAVE_SLEEP" != "" ]; then
     sleep $ESTORM_SLAVE_SLEEP
   fi
  done
fi

wait
