# Spark Application

This application shows the examples of creating spark application,
how to read or write into avro, or parquet format using Spark.
I also shows to create a spark application which can be run on spark-submit,
read and writes the file from and to hdfs location.

## Getting Started

Please find below steps to run the SparkApp in clustor or client mode.

### Prerequisites

Hadoop, hive should be installed in the machine.
MySql should be running.
Hadoop services should be running

### Installing

Install the jar spark-scala-1.0-SNAPSHOT-jar-with-dependencies.jar.
copy the script file - spark_app.sh to your location.

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

* [Dropwizard](http://www.dropwizard.io/1.0.2/docs/) - The web framework used
* [Maven](https://maven.apache.org/) - Dependency Management
* [ROME](https://rometools.github.io/rome/) - Used to generate RSS Feeds

## Contributing

Please read [CONTRIBUTING.md](https://gist.github.com/PurpleBooth/b24679402957c63ec426) for details on our code of conduct, and the process for submitting pull requests to us.

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/your/project/tags). 

## Authors

* **Billie Thompson** - *Initial work* - [PurpleBooth](https://github.com/PurpleBooth)

See also the list of [contributors](https://github.com/your/project/contributors) who participated in this project.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

* Hat tip to anyone whose code was used
* Inspiration
* etc
