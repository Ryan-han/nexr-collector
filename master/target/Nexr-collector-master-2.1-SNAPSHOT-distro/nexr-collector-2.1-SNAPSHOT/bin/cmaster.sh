#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

if [ $# -le 0 ]; then
  echo "Usage: cmaster.sh (start|stop) [<catalina-args...>]"
  exit 1
fi

actionCmd=$1
shift

this="${BASH_SOURCE-$0}"

BASEDIR=`dirname ${this}`
BASEDIR=`cd ${BASEDIR}/..;pwd`

COLLECTOR_HOME=$BASEDIR
source ${COLLECTOR_HOME}/bin/collector-env.sh

LIB=${COLLECTOR_HOME}/lib
if [ "$LOG_DIR" == "" ]; then
    LOG_DIR=${COLLECTOR_HOME}/logs
fi

if [ "$CONF_HOME" == "" ]; then
    CONF_HOME=$COLLECTOR_HOME
fi
CLASS_PATH=""
COLLECTOR_CONF=$CONF_HOME/cmaster
# prepend conf dir to classpath
if [ -n "$COLLECTOR_CONF" ]; then
  CLASS_PATH="$COLLECTOR_CONF:$CLASS_PATH"
fi


CLASS_PATH=${CLASS_PATH}:${LIB}/'*'

if [ "$MONITOR_PID_DIR" = "" ]; then
  MONITOR_PID_DIR=/tmp
fi
log=$MONITOR_PID_DIR/cmaster.out
pid=$MONITOR_PID_DIR/cmaster.pid
STOP_TIMEOUT=${STOP_TIMEOUT:-3}

JAVA_OPT="-Xms2048m -Xmx4096m"

case $actionCmd in

  (start)
    [ -w "$MONITOR_PID_DIR" ] ||  mkdir -p "$MONITOR_PID_DIR"

    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo $command running as process `cat $pid`.  Stop it first.
        exit 1
      fi
    fi

    echo starting $command logging to $log
    java ${JAVA_OPT} -cp ${CLASS_PATH} -Dlog.dir=${LOG_DIR} com.nexr.master.services.CMasterService ${actionCmd} > "$log" 2>&1 < /dev/null &

    echo $! > $pid
    TARGET_PID=`cat $pid`
    echo starting as process $TARGET_PID 
    ;;
  (stop)

    if [ -f $pid ]; then
      TARGET_PID=`cat $pid`
      if kill -0 $TARGET_PID > /dev/null 2>&1; then
        echo stopping $command
        kill $TARGET_PID
        sleep $STOP_TIMEOUT
        if kill -0 $TARGET_PID > /dev/null 2>&1; then
          echo "$command did not stop gracefully after $STOP_TIMEOUT seconds: killing with kill -9"
          kill -9 $TARGET_PID
        fi
      else
        echo no $command to stop
      fi
      rm -f $pid
    else
      echo no $command to stop
    fi
    ;;

  (*)
    echo $usage
    exit 1
    ;;

esac
