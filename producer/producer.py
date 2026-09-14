import json
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer


# ----------------------------
# Kafka configuration
# ----------------------------

KAFKA_BROKER = "localhost:9092"
TOPIC = "raw-telemetry"

producer = Producer(
    {
        "bootstrap.servers": KAFKA_BROKER,
        "client.id": "telemetry-producer",
    }
)


# ----------------------------
# Simulated devices
# ----------------------------

DEVICES = [
    "device_001",
    "device_002",
    "device_003",
    "device_004",
    "device_005",
]


def generate_event(device_id: str) -> dict:
    """
    Generate one synthetic telemetry event.
    """

    event = {
        "event_id": str(uuid.uuid4()),
        "device_id": device_id,
        "event_time": datetime.now(timezone.utc).isoformat(),
        "temperature": round(random.gauss(70, 3), 2),
        "vibration": round(max(0, random.gauss(0.30, 0.05)), 3),
        "voltage": round(random.gauss(230, 2), 2),
    }

    return event


def delivery_report(err, msg):
    """
    Called by the Kafka client when Kafka confirms whether
    a message was successfully delivered.
    """

    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Delivered to topic={msg.topic()} "
            f"partition={msg.partition()} "
            f"offset={msg.offset()}"
        )


def main():
    print("Starting telemetry producer...")
    print(f"Kafka broker: {KAFKA_BROKER}")
    print(f"Topic: {TOPIC}")
    print("Press Ctrl+C to stop.\n")

    try:
        while True:
            device_id = random.choice(DEVICES)

            event = generate_event(device_id)

            event_json = json.dumps(event)

            producer.produce(
                topic=TOPIC,
                key=device_id,
                value=event_json,
                callback=delivery_report,
            )

            producer.poll(0)

            print(f"Produced: {event_json}")

            time.sleep(0.5)

    except KeyboardInterrupt:
        print("\nStopping producer...")

    finally:
        producer.flush()
        print("Producer stopped.")


if __name__ == "__main__":
    main()