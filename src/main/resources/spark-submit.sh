#!/bin/bash
export SPARK_HOME=/usr/hdp/current/spark2-client
export PYTHONPATH=/usr/bin/python2.7
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.65-3.b17.el7.x86_64/jre
export PATH=$PYTHONPATH:$SPARK_HOME/bin:$PATH:$JAVA_HOME/bin
export SPARK_CLASSPATH=/usr/hdp/current/hbase-client/lib/hbase-common.jar:/usr/hdp/current/hbase-client/lib/hbase-client.jar:/usr/hdp/current/hbase-client/lib/hbase-server.jar:/usr/hdp/current/hbase-client/lib/hbase-protocol.jar:/usr/hdp/current/hbase-client/lib/guava-12.0.1.jar

$SPARK_HOME/bin/spark-submit \
--master yarn \
--deploy-mode client \
--driver-java-options "-Dlog4j.configuration=file:/export/home/adlakproc/Ludo/HbaseTest/log4j.properties" \
--files /etc/hbase/conf/hbase-site.xml \
--class com.natixis.df.Main \
HbaseWriteTest-assembly-1.0.jar $@
