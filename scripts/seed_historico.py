import argparse
import json
import random
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


def generate_event(day):
    event_type = random.choices(
        EVENT_TYPES,
        weights=EVENT_WEIGHTS,
        k=1
    )[0]

    midnight = datetime(
        day.year,
        day.month,
        day.day,
        tzinfo=timezone.utc
    )

    event_time = midnight + timedelta(
        seconds=random.randint(0, 86399),
        milliseconds=random.randint(0, 999)
    )

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
        event["order_id"] = (
            f"order_{random.randint(1, 500):04d}"
        )
        event["shipping_status"] = random.choice(
            SHIPPING_STATUS
        )
        return event

    product_id, category, price = random.choice(PRODUCTS)

    event["product_id"] = product_id
    event["category"] = category

    if event_type == "click":
        return event

    event["quantity"] = random.randint(1, 3)
    event["unit_price"] = price

    if event_type == "purchase":
        event["order_id"] = (
            f"order_{random.randint(1, 500):04d}"
        )

    return event


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--date",
        required=True
    )

    parser.add_argument(
        "--count",
        type=int,
        default=1000
    )

    parser.add_argument(
        "--output",
        required=True
    )

    args = parser.parse_args()

    day = datetime.strptime(
        args.date,
        "%Y-%m-%d"
    ).date()

    output = Path(args.output)
    output.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    random.seed(42)

    with output.open(
        "w",
        encoding="utf-8"
    ) as file:

        for _ in range(args.count):
            event = generate_event(day)

            file.write(
                json.dumps(
                    event,
                    ensure_ascii=False,
                    separators=(",", ":")
                )
                + "\n"
            )

    print(
        f"[historico] {args.count} eventos "
        f"gerados para {args.date} em {output}"
    )


if __name__ == "__main__":
    main()