# Spark Application

This application shows the examples of creating spark application,
how to read or write into avro, or parquet format using Spark.
I also shows to create a spark application which can be run on spark-submit,
read and writes the file from and to hdfs location.

## Getting Started

Please find below steps to run the SparkApp in cluster or client mode.

### Prerequisites

Hadoop, hive should be installed in the machine.
MySql should be running.
Hadoop services should be running

### Installing

Install the jar spark-scala-1.0-SNAPSHOT-jar-with-dependencies.jar.
copy the script file - spark_app.sh to your location.

use Java version 1.8
Use Scala version 2.12

### Steps to run the application : 

#### Step1 : running hadoop services.
```
$ cd /usr/local/cellar/hadoop/3.2.1/libexec/sbin
$ ./start-all.sh
```

# TBContinued
### And coding style tests

Explain what these tests test and why

```
Give an example
```

## Deployment

Add additional notes about how to deploy this on a live system

## Built With

## Contributing

## Versioning

## Authors

* **Vikas Katiyar** - *Initial work* - (https://github.com/sheeluvikas)

## License

#---------------------------------------------------
### Rest equivalent to run the job in google cloud : 
POST /v1/projects/single-nebula-319205/regions/us-central1/jobs:submit/
{
  "projectId": "single-nebula-319205",
  "job": {
    "placement": {
      "clusterName": "cluster-27e7"
    },
    "statusHistory": [],
    "reference": {
      "jobId": "job-2eb9a59a",
      "projectId": "single-nebula-319205"
    },
    "sparkJob": {
      "mainJarFileUri": "gs://nebula_2244/libs/spark-scala-1.0-SNAPSHOT-jar-with-dependencies.jar",
      "properties": {
        "spark.executor.extraJavaOptions": "-DBUCKET_INPUT_PATH=gs://nebula_2244/data/ -DBUCKET_OUTPUT_PATH=gs://nebula_2244/output",
        "spark.driver.extraJavaOptions": "-DBUCKET_INPUT_PATH=gs://nebula_2244/data/ -DBUCKET_OUTPUT_PATH=gs://nebula_2244/output"
      }
    }
  }
}