#!/usr/bin/env bash
# Modelled after $ESTORM_HOME/bin/estorm-env.sh.

# resolve links - "${BASH_SOURCE-$0}" may be a softlink

this="${BASH_SOURCE-$0}"
while [ -h "$this" ]; do
  ls=`ls -ld "$this"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '.*/.*' > /dev/null; then
    this="$link"
  else
    this=`dirname "$this"`/"$link"
  fi
done


# convert relative path to absolute path
bin=`dirname "$this"`
script=`basename "$this"`
bin=`cd "$bin">/dev/null; pwd`
this="$bin/$script"

# the root of the estorm installation
if [ -z "$ESTORM_HOME" ]; then
  export ESTORM_HOME=`dirname "$this"`/..
fi

#check to see if the conf dir or estorm home are given as an optional arguments
while [ $# -gt 1 ]
do
  if [ "--config" = "$1" ]
  then
    shift
    confdir=$1
    shift
    ESTORM_CONF_DIR=$confdir
  elif [ "--hosts" = "$1" ]
  then
    shift
    hosts=$1
    shift
    ESTORM_MASTERS=$hosts
  else
    # Presume we are at end of options and break
    break
  fi
done

# Allow alternate estorm conf dir location.
ESTORM_CONF_DIR="${ESTORM_CONF_DIR:-$ESTORM_HOME/conf}"
# List of estorm   masters.
ESTORM_MASTERS="${ESTORM_MASTERS:-$ESTORM_CONF_DIR/masters}"


echo ${ESTORM_CONF_DIR}/estorm-env.sh  $ESTORM_ENV_INIT   

if [ -z "$ESTORM_ENV_INIT" ] && [ -f "${ESTORM_CONF_DIR}/estorm-env.sh" ]; then
  . "${ESTORM_CONF_DIR}/estorm-env.sh"
  export ESTORM_ENV_INIT="true"
fi

if [ -z "$JAVA_HOME" ]; then
  for candidate in \
    /usr/lib/jvm/java-6-sun \
    /usr/lib/jvm/java-1.6.0-sun-1.6.0.*/jre \
    /usr/lib/jvm/java-1.6.0-sun-1.6.0.* \
    /usr/lib/j2sdk1.6-sun \
    /usr/java/jdk1.6* \
    /usr/java/jre1.6* \
    /Library/Java/Home ; do
    if [ -e $candidate/bin/java ]; then
      export JAVA_HOME=$candidate
      break
    fi
  done
  # if we didn't set it
  if [ -z "$JAVA_HOME" ]; then
    cat 1>&2 <<EOF
+======================================================================+
|      Error: JAVA_HOME is not set and Java could not be found         |
+----------------------------------------------------------------------+
| Please download the latest Sun JDK from the Sun Java web site        |
|       > http://java.sun.com/javase/downloads/ <                      |
|                                                                      |
| estorm requires Java 1.6 or later.                                    |
| NOTE: This script will find Sun Java whether you install using the   |
|       binary or the RPM based installer.                             |
+======================================================================+
EOF
    exit 1
  fi
fi
