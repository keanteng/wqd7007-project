# WQD7007 Big Data Management Project

This repository contains the working files for the project for the course Big Data Management. Primarily, Docker will be used to run the whole pipeline. The project is divided into three parts: Data Ingestion, Data Processing, Modeling and Data Visualization. Additionally, a Pub-Sub implementation using Kafka is also explored. The data used in this project is the NYC Taxi Trip data from Kaggle.

## Title
Uncovering Patterns in Urban Transportation: A Big Data Analysis of NYC Taxi Trips

## Using This Repository

1. Clone the repository to your local machine.

```bash
git clone https://github.com/keanteng/wqd7007-project
```

2. Change the directory to the folder that you want to work in.
```bash
# to hdfs folder
cd hdfs

# to the hive folder
cd hive

# to the spark folder
cd spark

# to the kafka folder
cd kafka

# change back to the root folder
cd ..
```

> Note: Remember to delete the containers and images after you are done with the project to free up space on your local machine.

## Architecture

| Stage | Implementation | Description |
|-------|----------------|-------------|
| Data Ingestion | HDFS | Data is ingested from the source and stored in HDFS. MapReduce is also performed in this stage to filter the data. |
| Data Processing | Hive | Data is processed using Hive. The data is cleaned and transformed into a format suitable for analysis. |
| Data Modeling | Spark | Data is modeled using Spark. Machine learning algorithms are applied to the data to uncover patterns and insights. |
| Data Visualization | Power BI | Data is visualized using Power BI. The data is presented in a way that is easy to understand and interpret. |
| Advanced Implementation | Python | Compare the data processing and modeling performance of Apache pipeline with the traditional Python pipeline. |
| Pub-Sub | Kafka | Taxi Data is published to a Kafka topic for real-time processing. The data is consumed by a Spark Streaming application for real-time analysis. In this project, we make it to a completed taxi trip notification. |

## Demonstration

![Demo](kafka/kafka-demo.gif)

Watch on [Youtube](https://youtu.be/LTH3W15_tcE).


## Source of Data
[New York City Taxi](https://www.kaggle.com/datasets/anandaramg/taxi-trip-data-nyc/code)