import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "raw-telemetry")

EVENTS_PER_SECOND = float(os.getenv("EVENTS_PER_SECOND", "2"))

ANOMALY_PROBABILITY = float(
    os.getenv("ANOMALY_PROBABILITY", "0.03")
)


producer = Producer(
    {
        "bootstrap.servers": KAFKA_BROKER,
        "client.id": "telemetry-producer",
    }
)


# ---------------------------------------------------------
# Device profiles
# ---------------------------------------------------------

DEVICE_PROFILES = {
    "device_001": {
        "temperature": 65,
        "vibration": 0.25,
        "voltage": 230,
    },
    "device_002": {
        "temperature": 68,
        "vibration": 0.30,
        "voltage": 229,
    },
    "device_003": {
        "temperature": 72,
        "vibration": 0.35,
        "voltage": 231,
    },
    "device_004": {
        "temperature": 66,
        "vibration": 0.28,
        "voltage": 230,
    },
    "device_005": {
        "temperature": 70,
        "vibration": 0.32,
        "voltage": 228,
    },
}


def generate_event(device_id: str) -> dict:
    profile = DEVICE_PROFILES[device_id]

    is_anomaly = random.random() < ANOMALY_PROBABILITY

    temperature = random.gauss(
        profile["temperature"],
        2,
    )

    vibration = random.gauss(
        profile["vibration"],
        0.04,
    )

    voltage = random.gauss(
        profile["voltage"],
        1.5,
    )

    # Inject abnormal behaviour
    if is_anomaly:
        anomaly_type = random.choice(
            [
                "temperature_spike",
                "vibration_spike",
                "voltage_drop",
            ]
        )

        if anomaly_type == "temperature_spike":
            temperature += random.uniform(20, 35)

        elif anomaly_type == "vibration_spike":
            vibration += random.uniform(1.0, 2.0)

        elif anomaly_type == "voltage_drop":
            voltage -= random.uniform(25, 50)

    else:
        anomaly_type = None

    return {
        "event_id": str(uuid.uuid4()),
        "device_id": device_id,
        "event_time": datetime.now(timezone.utc).isoformat(),
        "temperature": round(temperature, 2),
        "vibration": round(max(vibration, 0), 3),
        "voltage": round(voltage, 2),
        "is_injected_anomaly": is_anomaly,
        "anomaly_type": anomaly_type,
    }


def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        print(
            f"✅ delivered "
            f"partition={msg.partition()} "
            f"offset={msg.offset()}"
        )


def main():

    sleep_interval = 1 / EVENTS_PER_SECOND

    print("Starting telemetry producer")
    print(f"Broker: {KAFKA_BROKER}")
    print(f"Topic: {TOPIC}")
    print(f"Rate: {EVENTS_PER_SECOND} events/sec")
    print(f"Anomaly probability: {ANOMALY_PROBABILITY}")
    print()

    try:

        while True:

            device_id = random.choice(
                list(DEVICE_PROFILES.keys())
            )

            event = generate_event(device_id)

            producer.produce(
                topic=TOPIC,
                key=device_id,
                value=json.dumps(event),
                callback=delivery_report,
            )

            producer.poll(0)

            anomaly_marker = (
                " ⚠️ ANOMALY"
                if event["is_injected_anomaly"]
                else ""
            )

            print(
                f"{device_id} | "
                f"T={event['temperature']} | "
                f"Vib={event['vibration']} | "
                f"Volt={event['voltage']}"
                f"{anomaly_marker}"
            )

            time.sleep(sleep_interval)

    except KeyboardInterrupt:
        print("\nStopping producer...")

    finally:
        producer.flush()
        print("Producer stopped.")


if __name__ == "__main__":
    main()