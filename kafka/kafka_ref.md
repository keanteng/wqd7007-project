# Kafka Reference

In this section we will make use of Kafka to simulate a streaming data pipeline where we use the existing taxi data to create a producer to broadcast the completed taxi trip to the consumer end. This is a sample message at the consumer end:

```
Taxi Ride Summary:
-----------------
Your 1.2 mile journey took 4.7 minutes.
Picked up at 12:30 AM from zone 74 to zone 168.
Fare: $6.00
Tip: $0.00
Total amount: $7.30 paid with Cash.

Thank you for choosing our taxi service!
```

## Demonstration
Check out the [video](https://youtu.be/LTH3W15_tcE) below to see how the producer and consumer work together to create a streaming data pipeline.

![Demo](demo-kafka.gif)

## Configuration

Make sure you have docker installed and running. Also, make sure you have the `docker-compose.yml` in your directory. Then copy the `producer.py` and `consumer.py` files to the same directory as the `docker-compose.yml` file. Also install `python-kafka` in your local environment. You can do this by running the following command:

```bash
py -3.12 -m pip install kafka-python
```

## Docker Compose

```bash
# start the docker container
docker-compose up -d

# on the terminal start the producer
py -3.12 producer.py

# on another terminal start the consumer
py -3.12 consumer.py
```