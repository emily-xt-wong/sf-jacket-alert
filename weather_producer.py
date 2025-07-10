# Import necessary packages
import time, json, requests
from kafka import KafkaProducer

# Configuration
API_KEY = "c276a08e05da927b1b5a40ed7e1a6d61"
BASE_URL = "http://api.weatherstack.com/current" 
LOCATION = "San Francisco" 
UNITS = "m"
POLL_INTERVAL = 60
KAFKA_SERVER  = "localhost:9092"
TOPIC = "weather"

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Fetch current weather data for a specified city
def fetch_current(city):
    params = {
        "access_key": API_KEY, 
        "query": city, 
        "units": UNITS
    }
    resp = requests.get(BASE_URL, params=params, timeout=10)  # Send request to the weather API
    data = resp.json()  # Parse JSON response
    if "error" in data:
        raise RuntimeError(f"API error {data['error']['code']}: {data['error']['info']}") 
    return data["current"]  # Return the 'current' section of weather data

# Determine jacket recommendation based on weather conditions
def classify_jacket(temp_c, wind_kph, precip_mm):
    # Recommend raincoat if precipitation is detected
    if precip_mm > 0:
        return "Raincoat"
    # Recommend based on temperature ranges
    if temp_c <= 0:
        level = "Heavy Jacket"
    elif temp_c <= 10:
        level = "Medium Jacket"
    elif temp_c <= 18:
        level = "Light Jacket"
    else:
        return "No Jacket"
    # Adjust recommendation for high wind speeds
    if wind_kph >= 25:
        return {
            "Light Jacket":  "Medium Jacket",
            "Medium Jacket": "Heavy Jacket",
            "Heavy Jacket":  "Heavy Jacket"
        }[level]
    return level

# Main execution logic
if __name__ == "__main__":
    print(f"→ Producing to Kafka topic '{TOPIC}' every {POLL_INTERVAL}s")
    while True:
        try:
            # Retrieve current weather data
            cw = fetch_current(LOCATION)
            
            # Generate jacket recommendation
            rec = classify_jacket(
                temp_c   = cw["temperature"],
                wind_kph = cw["wind_speed"],
                precip_mm= cw["precip"]
            )
            
            # Prepare message payload
            msg = {
                "ts": int(time.time()),
                "location": LOCATION,
                "temperature_C":  cw["temperature"],
                "wind_kph": cw["wind_speed"],
                "precip_mm": cw["precip"],
                "recommendation": rec
            }
            
            # Send message to Kafka topic
            producer.send(TOPIC, msg)
            producer.flush()  # Ensure all buffered records are sent
            print("Sent:", msg)
        
        except Exception as e:
            # Handle any errors
            print("Error:", e)
        
        # Wait for the next polling interval
        time.sleep(POLL_INTERVAL)