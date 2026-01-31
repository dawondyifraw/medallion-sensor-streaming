import json
import time
import random
import random
from datetime import datetime, timedelta
from datetime import datetime
from kafka import KafkaProducer

KAFKA_BROKER = "localhost:9092"
TOPIC = 'sensor-data-medallion'
# IKDB/Den Bosch: previous project interest around the city center
LAT_RANGE = (51.683, 51.733)
LON_RANGE = (5.283, 5.383)

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Sensor streaming: replace with your own logic as needed.
def generate_sensor_data():
    sensor_ids = [f"sensor_{i}" for i in range(1, 6)]
    start_time = datetime.now() - timedelta(hours=1)
    end_time = datetime.now()
    interval_minutes = 5

    data = []
    current_time = start_time
    while current_time <= end_time:
        for sensor_id in sensor_ids:
            temperature = round(random.uniform(20.0, 35.0), 2)
            humidity = round(random.uniform(40.0, 80.0), 2)
            data.append({
                "sensor_id": sensor_id,
                "timestamp": current_time.strftime('%Y-%m-%d %H:%M:%S'),
                "temperature": temperature,
                "humidity": humidity
            })
        current_time += timedelta(minutes=interval_minutes)

    return data

if __name__ == "__main__":
    print(f"Producing to topic: {TOPIC}...")
    while True:
        data = generate_sensor_data()
        producer.send(TOPIC, value=data)
        print("Sent:", data)
        time.sleep(1)  # every second
