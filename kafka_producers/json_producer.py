from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def generate_ad_impression():
    return {
        "ad_creative_id": "ad123",
        "user_id": "user123",
        "timestamp": int(time.time()),
        "website": "example.com"
    }

while True:
    ad_impression = generate_ad_impression()
    producer.send('ad_impressions', ad_impression)
    time.sleep(1)
