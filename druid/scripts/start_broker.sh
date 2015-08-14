#!/usr/bin/env bash

source jvm.config

java $JAVA_OPTIONS -classpath config/_common:config/broker:lib/* io.druid.cli.Main server broker &
