#!/bin/bash

########################
# Application name : Spark app
# @Author          : Vikas
# @git url         : git@github.com:sheeluvikas/spark-scala.git

# for standalone mode :
# --master spark://192.168.43.204:7077 \
# to run the above standalone mode, make sure we have start-master.sh run
#
########################
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop

SPARK_APP_JAR=$(ls /Users/vikas/GitRepo/spark-scala/target/spark-scala-*-jar-with-dependencies.jar | tail -1)
HIVE_FILE=/usr/local/Cellar/hive/3.1.2/libexec/conf/hive-site.xml
/usr/local/bin/spark-submit \
--files=${HIVE_FILE} \
--master yarn \
--conf "spark.executor.extraJavaOptions=-DBUCKET_INPUT_PATH=/app/data/ -DBUCKET_OUTPUT_PATH=/app/hive/file" \
--conf "spark.driver.extraJavaOptions=-DBUCKET_INPUT_PATH=/app/data/ -DBUCKET_OUTPUT_PATH=/app/hive/file" \
--deploy-mode cluster \
--class com.example.app.SparkApp ${SPARK_APP_JAR}