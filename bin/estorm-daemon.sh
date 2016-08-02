#!/usr/bin/env bash

# Runs a estorm command as a daemon.
#
# Environment Variables
#
#   ESTORM_CONF_DIR   Alternate estorm conf dir. Default is ${ESTORM_HOME}/conf.
#   ESTORM_LOG_DIR    Where log files are stored.  PWD by default.
#   ESTORM_PID_DIR    The pid files are stored. /tmp by default.
#   ESTORM_IDENT_STRING   A string representing this instance of hadoop. $USER by default
#   ESTORM_NICENESS The scheduling priority for daemons. Defaults to 0.
#   ESTORM_STOP_TIMEOUT  Time, in seconds, after which we kill -9 the server if it has not stopped.
#                        Default 1200 seconds.
#
# Modelled after $HADOOP_HOME/bin/hadoop-daemon.sh

usage="Usage: estorm-daemon.sh [--config <conf-dir>]\
 (start|stop|restart|autorestart) <estorm-command> \
 <args...>"

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin">/dev/null; pwd`

. "$bin"/estorm-config.sh
. "$bin"/estorm-common.sh

# get arguments
startStop=$1
shift

command=$1
shift

estorm_rotate_log ()
{
    log=$1;
    num=5;
    if [ -n "$2" ]; then
    num=$2
    fi
    if [ -f "$log" ]; then # rotate logs
    while [ $num -gt 1 ]; do
        prev=`expr $num - 1`
        [ -f "$log.$prev" ] && mv -f "$log.$prev" "$log.$num"
        num=$prev
    done
    mv -f "$log" "$log.$num";
    fi
}

cleanZNode() {
  if [ -f $ESTORM_ZNODE_FILE ]; then
    if [ "$command" = "master" ]; then
      $bin/estorm master clear > /dev/null 2>&1
    else
        $bin/estorm clean --cleanZk  > /dev/null 2>&1
    fi
    rm $ESTORM_ZNODE_FILE
  fi
}

check_before_start(){
    #ckeck if the process is not running
    mkdir -p "$ESTORM_PID_DIR"
    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo $command running as process `cat $pid`.  Stop it first.
        exit 1
      fi
    fi
}

wait_until_done ()
{
    p=$1
    cnt=${ESTORM_SLAVE_TIMEOUT:-300}
    origcnt=$cnt
    while kill -0 $p > /dev/null 2>&1; do
      if [ $cnt -gt 1 ]; then
        cnt=`expr $cnt - 1`
        sleep 1
      else
        echo "Process did not complete after $origcnt seconds, killing."
        kill -9 $p
        exit 1
      fi
    done
    return 0
}

# get log directory
if [ "$ESTORM_LOG_DIR" = "" ]; then
  export ESTORM_LOG_DIR="$ESTORM_HOME/logs"
fi
mkdir -p "$ESTORM_LOG_DIR"

if [ "$ESTORM_PID_DIR" = "" ]; then
  ESTORM_PID_DIR=/tmp
fi

if [ "$ESTORM_IDENT_STRING" = "" ]; then
  export ESTORM_IDENT_STRING="$USER"
fi

# Some variables
# Work out java location so can print version into log.
if [ "$JAVA_HOME" != "" ]; then
  #echo "run java in $JAVA_HOME"
  JAVA_HOME=$JAVA_HOME
fi
if [ "$JAVA_HOME" = "" ]; then
  echo "Error: JAVA_HOME is not set."
  exit 1
fi

JAVA=$JAVA_HOME/bin/java
export ESTORM_LOG_PREFIX=estorm-$ESTORM_IDENT_STRING-$command-$HOSTNAME
export ESTORM_LOGFILE=$ESTORM_LOG_PREFIX.log
export ESTORM_ROOT_LOGGER=${ESTORM_ROOT_LOGGER:-"INFO,RFA"}
export ESTORM_SECURITY_LOGGER=${ESTORM_SECURITY_LOGGER:-"INFO,RFAS"}
logout=$ESTORM_LOG_DIR/$ESTORM_LOG_PREFIX.out
loggc=$ESTORM_LOG_DIR/$ESTORM_LOG_PREFIX.gc
loglog="${ESTORM_LOG_DIR}/${ESTORM_LOGFILE}"
pid=$ESTORM_PID_DIR/estorm-$ESTORM_IDENT_STRING-$command.pid
export ESTORM_ZNODE_FILE=$ESTORM_PID_DIR/estorm-$ESTORM_IDENT_STRING-$command.znode
export ESTORM_START_FILE=$ESTORM_PID_DIR/estorm-$ESTORM_IDENT_STRING-$command.autorestart

if [ -n "$SERVER_GC_OPTS" ]; then
  export SERVER_GC_OPTS=${SERVER_GC_OPTS/"-Xloggc:<FILE-PATH>"/"-Xloggc:${loggc}"}
fi

# Set default scheduling priority
if [ "$ESTORM_NICENESS" = "" ]; then
    export ESTORM_NICENESS=0
fi

thiscmd=$0
args=$@

case $startStop in

(start)
    check_before_start
    estorm_rotate_log $logout
    estorm_rotate_log $loggc
    echo starting $command, logging to $logout
    nohup $thiscmd --config "${ESTORM_CONF_DIR}" internal_start $command $args < /dev/null > ${logout} 2>&1  &
    sleep 1; head "${logout}"
  ;;

(autorestart)
    check_before_start
    estorm_rotate_log $logout
    estorm_rotate_log $loggc
    nohup $thiscmd --config "${ESTORM_CONF_DIR}" internal_autorestart $command $args < /dev/null > ${logout} 2>&1  &
  ;;

(internal_start)
    # Add to the command log file vital stats on our environment.
    echo "`date` Starting $command on `hostname`" >> $loglog
    echo "`ulimit -a`" >> $loglog 2>&1
    nice -n $ESTORM_NICENESS "$ESTORM_HOME"/bin/estorm \
        --config "${ESTORM_CONF_DIR}" \
        $command "$@" start >> "$logout" 2>&1 &
    echo $! > $pid
    wait
    cleanZNode
  ;;

(internal_autorestart)
    touch "$ESTORM_START_FILE"
    #keep starting the command until asked to stop. Reloop on software crash
    while true
      do
        lastLaunchDate=`date +%s`
        $thiscmd --config "${ESTORM_CONF_DIR}" internal_start $command $args

        #if the file does not exist it means that it was not stopped properly by the stop command
        if [ ! -f "$ESTORM_START_FILE" ]; then
          exit 1
        fi

        #if the cluster is being stopped then do not restart it again.
        zparent=`$bin/estorm org.apache.hadoop.estorm.util.HBaseConfTool zookeeper.znode.parent`
        if [ "$zparent" == "null" ]; then zparent="/estorm"; fi
        zshutdown=`$bin/estorm org.apache.hadoop.estorm.util.HBaseConfTool zookeeper.znode.state`
        if [ "$zshutdown" == "null" ]; then zshutdown="shutdown"; fi
        zFullShutdown=$zparent/$zshutdown
        $bin/estorm zkcli stat $zFullShutdown 2>&1 | grep "Node does not exist"  1>/dev/null 2>&1
        #grep returns 0 if it found something, 1 otherwise
        if [ $? -eq 0 ]; then
          exit 1
        fi

        #If ZooKeeper cannot be found, then do not restart
        $bin/estorm zkcli stat $zFullShutdown 2>&1 | grep Exception | grep ConnectionLoss  1>/dev/null 2>&1
        if [ $? -eq 0 ]; then
          exit 1
        fi

        #if it was launched less than 5 minutes ago, then wait for 5 minutes before starting it again.
        curDate=`date +%s`
        limitDate=`expr $lastLaunchDate + 300`
        if [ $limitDate -gt $curDate ]; then
          sleep 300
        fi
      done
    ;;

(stop)
    rm -f "$ESTORM_START_FILE"
    if [ -f $pid ]; then
      pidToKill=`cat $pid`
      # kill -0 == see if the PID exists
      if kill -0 $pidToKill > /dev/null 2>&1; then
        echo -n stopping $command
        echo "`date` Terminating $command" >> $loglog
        kill $pidToKill > /dev/null 2>&1
        waitForProcessEnd $pidToKill $command
        rm $pid
      else
        retval=$?
        echo no $command to stop because kill -0 of pid $pidToKill failed with status $retval
      fi
    else
      echo no $command to stop because no pid file $pid
    fi
  ;;

(restart)
    # stop the command
    $thiscmd --config "${ESTORM_CONF_DIR}" stop $command $args &
    wait_until_done $!
    # wait a user-specified sleep period
    sp=${ESTORM_RESTART_SLEEP:-3}
    if [ $sp -gt 0 ]; then
      sleep $sp
    fi
    # start the command
    $thiscmd --config "${ESTORM_CONF_DIR}" start $command $args &
    wait_until_done $!
  ;;

(*)
  echo $usage
  exit 1
  ;;
esac
