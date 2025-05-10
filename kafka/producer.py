import csv
import json
import time
from kafka import KafkaProducer

# Configure the Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Path to your CSV file
csv_path = 'data.csv'  # Update with your actual path

def read_and_send_csv_row():
    with open(csv_path, 'r') as file:
        csv_reader = csv.DictReader(file)
        # Just send the first row for testing
        for row in csv_reader:
            # Send the row to Kafka topic 'taxi-data'
            producer.send('taxi-data-2', value=row)
            print(f"Sent: {row}")
            producer.flush()
            # If you want to send only one row, break the loop
            break

if __name__ == "__main__":
    # Wait for Kafka to be fully up
    time.sleep(10)  
    read_and_send_csv_row()
    print("Message sent successfully")