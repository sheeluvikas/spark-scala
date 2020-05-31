#!/bin/bash

export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop

SPARK_APP_JAR=$(ls /Users/vikas/GitRepo/spark-scala/target/spark-scala-*-jar-with-dependencies.jar | tail -1)
HIVE_FILE=/usr/local/Cellar/hive/3.1.2/libexec/conf/hive-site.xml
/usr/local/bin/spark-submit \
--files=${HIVE_FILE} \
--master yarn \
--deploy-mode cluster \
--class org.example.SparkApp ${SPARK_APP_JAR}