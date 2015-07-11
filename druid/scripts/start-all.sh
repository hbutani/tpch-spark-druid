#!/usr/bin/env bash

#JAVA_OPTIONS="-server
#-Xmx6g
#-Xms6g
#-XX:NewSize=1g
#-XX:MaxNewSize=1g
#-XX:MaxDirectMemorySize=9g
#-XX:+UseConcMarkSweepGC
#-XX:+PrintGCDetails
#-XX:+PrintGCTimeStamps
#-XX:+HeapDumpOnOutOfMemoryError
#-Duser.timezone=UTC
#-Dfile.encoding=UTF-8
#-Dcom.sun.management.jmxremote.port=17071
#-Dcom.sun.management.jmxremote.authenticate=false
#-Dcom.sun.management.jmxremote.ssl=false"

java -Xmx256m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -classpath config/_common:config/coordinator:lib/* io.druid.cli.Main server coordinator &

java -Xmx256m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -classpath config/_common:config/broker:lib/* io.druid.cli.Main server broker &

java -Xmx2g -Duser.timezone=UTC -Dfile.encoding=UTF-8 -classpath config/_common:config/historical:lib/* io.druid.cli.Main server historical &

java -Xmx256m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -classpath config/_common:config/overlord:lib/* io.druid.cli.Main server overlord &
