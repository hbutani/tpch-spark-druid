#!/usr/bin/env bash

processPid() {
  if [ -z "$1" ]
  then
    echo "Process name argument  not provided"
     exit -1
  fi
  ps -ef | grep $1 | grep -v grep | awk '{print $2}'
}

BROKERPID=`processPid broker`
COORDPID=`processPid coordinator`
HISTPID=`processPid historical`
OVERLORDPID=`processPid overlord`

kill -9 $OVERLORDPID $COORDPID $HISTPID $BROKERPID


