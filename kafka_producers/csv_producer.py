from kafka import KafkaProducer
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: v.encode('utf-8'))

def generate_click_conversion():
    return f"user123,campaign123,{int(time.time())},click"

while True:
    click_conversion = generate_click_conversion()
    producer.send('clicks_conversions', click_conversion)
    time.sleep(1)
