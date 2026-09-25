import argparse
import json
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path


PRODUCTS = [
    ("prod_001", "eletronicos", 199.90),
    ("prod_002", "eletronicos", 89.90),
    ("prod_003", "livros", 59.90),
    ("prod_004", "casa", 129.90),
    ("prod_005", "moda", 79.90),
]

EVENT_TYPES = [
    "click",
    "add_to_cart",
    "purchase",
    "shipment_update",
]

EVENT_WEIGHTS = [55, 25, 12, 8]

SHIPPING_STATUS = [
    "processing",
    "shipped",
    "in_transit",
    "delivered",
    "delayed",
]


def iso_utc(dt):
    return (
        dt.astimezone(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def generate_event():
    event_type = random.choices(
        EVENT_TYPES,
        weights=EVENT_WEIGHTS,
        k=1
    )[0]

    now = datetime.now(timezone.utc)

    # Alguns eventos são atrasados de propósito.
    # Depois usaremos isso para demonstrar watermarks no Flink.
    if random.random() < 0.20:
        event_time = now - timedelta(
            seconds=random.uniform(1, 4)
        )
    else:
        event_time = now

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_time": iso_utc(event_time),
        "user_id": f"user_{random.randint(1, 50):03d}",
        "session_id": f"session_{random.randint(1, 30):03d}",
        "product_id": None,
        "category": None,
        "quantity": None,
        "unit_price": None,
        "order_id": None,
        "shipping_status": None,
    }

    if event_type == "shipment_update":
        event["order_id"] = f"order_{random.randint(1, 500):04d}"
        event["shipping_status"] = random.choice(SHIPPING_STATUS)
        return event

    product_id, category, price = random.choice(PRODUCTS)

    event["product_id"] = product_id
    event["category"] = category

    if event_type == "click":
        return event

    event["quantity"] = random.randint(1, 3)
    event["unit_price"] = price

    if event_type == "purchase":
        event["order_id"] = f"order_{random.randint(1, 500):04d}"

    return event


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--output",
        default="/data/events.log"
    )

    parser.add_argument(
        "--interval",
        type=float,
        default=1.0
    )

    args = parser.parse_args()

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.touch(exist_ok=True)

    print(
        f"[generator] gravando eventos em {output}",
        flush=True
    )

    with output.open(
        "a",
        encoding="utf-8",
        buffering=1
    ) as file:

        while True:
            event = generate_event()

            line = json.dumps(
                event,
                ensure_ascii=False,
                separators=(",", ":")
            )

            file.write(line + "\n")
            file.flush()

            print(line, flush=True)

            time.sleep(args.interval)


if __name__ == "__main__":
    main()