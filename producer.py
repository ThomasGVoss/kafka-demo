from confluent_kafka import Producer
import json
import random
import time
 
KAFKA_BROKERS = "localhost:9092"
TOPIC = "position-event"
 
producer = Producer({
    "bootstrap.servers": KAFKA_BROKERS,
    "client.id": "python-example-producer",
})
 
 
def delivery_report(err, message):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Delivered to {message.topic()} "
            f"[partition {message.partition()}] "
            f"at offset {message.offset()}"
        )
 
 
while True:
    event = {
        "eventType": "gps",
        "deviceId": f"GPS-{random.randint(1000, 9999)}",
        "latitude": round(random.uniform(-90, 90), 6),
        "longitude": round(random.uniform(-180, 180), 6),
        "timestamp": time.time(),
    }
 
    producer.produce(
        topic=TOPIC,
        key=event["deviceId"],
        value=json.dumps(event),
        callback=delivery_report,
    )
    producer.poll(0)
 
    # Wait a random 1–10 seconds before producing the next GPS event.
    time.sleep(random.uniform(1, 10))