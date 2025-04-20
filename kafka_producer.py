from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = 'sensor_data'

def generate_data():
    return {
        'device_id': random.randint(1, 5),
        'temperature': round(random.uniform(20.0, 35.0), 2),
        'humidity': round(random.uniform(30.0, 60.0), 2),
        'timestamp': int(time.time())
    }

while True:
    data = generate_data()
    producer.send(topic, data)
    print(f"Sent: {data}")
    time.sleep(1)
