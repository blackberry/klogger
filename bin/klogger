#!/bin/bash
#
#  /etc/init.d/klogger
#
# Starts the klogger daemon
#
# chkconfig: - 95 5
# description: Collects logs, and writes them to Kafka.
### BEGIN INIT INFO
# Provides:       klogger
# Required-Start: $local_fs $remote_fs $network
# Required-Stop:  $local_fs $remote_fs $network
# Default-Start:
# Default-Stop:
# Description:    Start the klogger service
### END INIT INFO

setup() {
  if [ "x$CONFIGDIR" == "x" ]
  then
    CONFIGDIR=/opt/klogger/config
  fi
  . $CONFIGDIR/klogger-env.sh

  PROG="klogger"

  OS=unknown

  if [ -e "/etc/SuSE-release" ]
  then
    OS=suse
  elif [ -e "/etc/redhat-release" ]
  then
    OS=redhat
  else
    echo "Could not determine OS."
  fi

  # Source function library.
  [ "$OS" == "redhat" ] && . /etc/init.d/functions
  [ "$OS" == "suse"   ] && . /etc/rc.status

  RETVAL=0
}

start() {
  setup

  # Check if klogger is already running
  if [ -f $PIDFILE ]
  then
    PID=`head -1 $PIDFILE`
    if [ -e /proc/$PID ]
    then
      echo "$PROG is already running (PID $PID)"
      return 1
    else
      rm -f $PIDFILE
    fi
  fi

  echo -n $"Starting $PROG: "
  . $CONFIGDIR/klogger-env.sh

  nohup $JAVA $JAVA_OPTS -cp $CLASSPATH com.blackberry.bdp.klogger.KLogger >$LOGDIR/server.out &

  RETVAL=$?
  PID=$!

  if [ $RETVAL -eq 0 ]
  then
    [ "$OS" == "redhat"  ] && success
    [ "$OS" == "suse"    ] && echo -n $rc_done
    [ "$OS" == "unknown" ] && echo -n "... done"
    echo $PID > $PIDFILE
  else
    failure
  fi
  echo
}

stop() {
  setup

  echo -n $"Stopping $PROG: "

  if [ -f $PIDFILE ]
  then
    PID=`head -1 $PIDFILE`
    if [ -e /proc/$PID ]
    then
      kill $PID
      for i in `seq 1 60`
      do
        sleep 1

        if [ ! -e /proc/$PID ]
        then
          rm -f $PIDFILE
          [ "$OS" == "redhat"  ] && success
          [ "$OS" == "suse"    ] && echo -n $rc_done
          [ "$OS" == "unknown" ] && echo -n "... done"
          RETVAL=0
          break
        fi
      done

      if [ -e /proc/$PID ]
      then
        echo -n "Trying kill -9 "
        kill -9 $PID

        for i in `seq 1 60`
        do
          sleep 1

          if [ ! -e /proc/$PID ]
          then
            rm -f $PIDFILE
            [ "$OS" == "redhat"  ] && success
            [ "$OS" == "suse"    ] && echo -n $rc_done
            [ "$OS" == "unknown" ] && echo -n "... done"
            RETVAL=0
            break
          fi
        done
      fi

      if [ -e /proc/$PID ]
      then
        echo "Could not kill "
        [ "$OS" == "redhat"  ] && failure
        [ "$OS" == "suse"    ] && echo -n $rc_failed
        [ "$OS" == "unknown" ] && echo -n "... failed"
        RETVAL=1
      fi

    else
      echo -n "PID $PID is not running "
      rm -f $PIDFILE
      [ "$OS" == "redhat"  ] && success
      [ "$OS" == "suse"    ] && echo -n $rc_done
      [ "$OS" == "unknown" ] && echo -n "... done"
      RETVAL=0
    fi

  else
    echo -n "Could not find $PIDFILE"
    [ "$OS" == "redhat"  ] && failure
    [ "$OS" == "suse"    ] && echo -n $rc_failed
    [ "$OS" == "unknown" ] && echo -n "... failed"
    RETVAL=1

  fi

  echo
}

restart() {
  stop
  start
}

_status() {
  setup
  status -p $PIDFILE $PROG
}

# make functions available under su
export -f setup
export -f start
export -f stop
export -f restart
export -f _status

setup
case "$1" in
start)
  if [ "x$KLOGGER_USER" != "x" ]
  then
    su $KLOGGER_USER -c start
  else
    start
  fi
  ;;
stop)
  if [ "x$KLOGGER_USER" != "x" ]
  then
    su $KLOGGER_USER -c stop
  else
    stop
  fi
  ;;
restart)
  if [ "x$KLOGGER_USER" != "x" ]
  then
    su $KLOGGER_USER -c restart
  else
    restart
  fi
  ;;
status)
  if [ "x$KLOGGER_USER" != "x" ]
  then
    su $KLOGGER_USER -c _status
  else
    _status
  fi
  ;;
*)
  echo $"Usage: $0 {start|stop|restart|status}"
  exit 1
esac

exit $?
