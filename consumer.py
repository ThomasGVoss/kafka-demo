from confluent_kafka import Consumer, Producer
import json
import time

KAFKA_BROKERS = "localhost:9092"
TOPIC = "position-event"
FILTER_TOPIC = "position-filter-triggered"

consumer = Consumer({
    "bootstrap.servers": KAFKA_BROKERS,
    "group.id": "python-example-consumer",
    "auto.offset.reset": "earliest",
})
consumer.subscribe([TOPIC])

producer = Producer({
    "bootstrap.servers": KAFKA_BROKERS,
    "client.id": "python-example-filter-producer",
})


def delivery_report(err, message):
    if err is not None:
        print(f"Filter event delivery failed: {err}")


def is_wanted(device_id):
    try:
        prefix, number = device_id.split("-", 1)
        return prefix == "GPS" and 9000 <= int(number) <= 9999
    except (AttributeError, ValueError):
        return False


try:
    while True:
        message = consumer.poll(1.0)
        if message is None:
            continue
        if message.error():
            print(f"Consumer error: {message.error()}")
            continue

        try:
            event = json.loads(message.value())
        except (TypeError, json.JSONDecodeError):
            print(f"Skipping invalid message at offset {message.offset()}")
            continue

        if not is_wanted(event.get("deviceId")):
            continue

        print(
            f"[partition {message.partition()} offset {message.offset()}] "
            f"{event['deviceId']}: lat={event['latitude']}, "
            f"lon={event['longitude']}, ts={event['timestamp']}"
        )

        filter_event = {
            "eventType": "filterTriggered",
            "filter": "deviceId GPS-9000..GPS-9999",
            "deviceId": event["deviceId"],
            "sourceTopic": message.topic(),
            "sourcePartition": message.partition(),
            "sourceOffset": message.offset(),
            "timestamp": time.time(),
        }
        producer.produce(
            topic=FILTER_TOPIC,
            key=event["deviceId"],
            value=json.dumps(filter_event),
            callback=delivery_report,
        )
        producer.poll(0)
except KeyboardInterrupt:
    pass
finally:
    producer.flush()
    consumer.close()
