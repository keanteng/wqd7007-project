import json
import datetime
from kafka import KafkaConsumer

# Create a Kafka consumer
consumer = KafkaConsumer(
    'taxi-data-2',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def format_money(amount):
    """Format amount as currency with 2 decimal places"""
    return f"${float(amount):.2f}"

def format_human_readable_message(data):
    """Convert raw taxi data into a human-readable message"""
    
    # Format pickup and dropoff time to be more readable
    try:
        pickup_time = datetime.datetime.strptime(data['pickup_time'], '%Y-%m-%d %H:%M:%S')
        pickup_time_str = pickup_time.strftime('%I:%M %p')
    except:
        pickup_time_str = data['pickup_time']
        
    # Format monetary values
    fare = format_money(data['fare_amount'])
    tip = format_money(data['tip_amount'])
    total = format_money(data['total_amount'])
    
    # Determine payment method
    payment_types = {
        '1': 'Credit Card', 
        '2': 'Cash',
        '3': 'No Charge',
        '4': 'Dispute'
    }
    payment_method = payment_types.get(data['payment_type'], 'Unknown')
    
    # Create a human-readable message
    message = f"""
Taxi Ride Summary:
-----------------
Your {data['trip_distance']} mile journey took {float(data['trip_duration_minutes']):.1f} minutes.
Picked up at {pickup_time_str} from zone {data['pu_location']} to zone {data['do_location']}.
Fare: {fare}
Tip: {tip}
Total amount: {total} paid with {payment_method}.

Thank you for choosing our taxi service!
"""
    return message

# Consume messages
print("Starting consumer, waiting for messages...")
for message in consumer:
    # Get the raw taxi data
    taxi_data = message.value
    print("Received raw data:", json.dumps(taxi_data, indent=2))
    
    # Format into a user-friendly message
    formatted_message = format_human_readable_message(taxi_data)
    print("\nFormatted Message:")
    print(formatted_message)
    print("-" * 50)