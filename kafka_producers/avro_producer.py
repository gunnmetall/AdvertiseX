from kafka import KafkaProducer
import avro.schema
import avro.io
import io
import time

schema_path = "path/to/schema.avsc"
schema = avro.schema.parse(open(schema_path).read())
producer = KafkaProducer(bootstrap_servers='localhost:9092')

def generate_bid_request():
    return {
        "user_id": "user123",
        "auction_id": "auction123",
        "ad_targeting_criteria": "criteria123"
    }

while True:
    bid_request = generate_bid_request()
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer = avro.io.DatumWriter(schema)
    writer.write(bid_request, encoder)
    raw_bytes = bytes_writer.getvalue()
    producer.send('bid_requests', raw_bytes)
    time.sleep(1)
