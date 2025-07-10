# Import necessary packages
import time, json
from kafka import KafkaConsumer

# Create a Kafka consumer that listens to the topic
consumer = KafkaConsumer(
    "weather",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="weather-group-v2",
    value_deserializer=lambda b: json.loads(b.decode())
)

print("→ Listening for weather messages…")

# Continuously read messages from Kafka
for record in consumer:
    try:
        # Get the actual message content
        d = record.value
        
        # Format the timestamp from the message into a readable string
        t = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(d["ts"]))
        
        # Print the weather information in a nice readable format
        print(f"[{t} | {d['location']}] {d['temperature_C']}°C, "
              f"{d['wind_kph']} kph, {d['precip_mm']} mm → {d['recommendation']}")
    
    except Exception as e:
        # Skip the bad message if anything goes wrong
        print("Skipping bad message:", e)