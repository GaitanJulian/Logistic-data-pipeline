import os
import random
from datetime import datetime, timedelta
import csv
from pathlib import Path

RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"

CITIES = ["Bogota", "Medellin", "Cali", "Bucaramanga", "Barranquilla"]
STATES = ["CREATED", "IN_TRANSIT", "DELIVERED", "CANCELLED"]


def random_date(start: datetime, end: datetime) -> datetime:
    """Fecha aleatoria entre start y end."""
    delta = end - start
    seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=seconds)


def generate_shipments_csv(num_rows: int = 500) -> str:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    today_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = RAW_DIR / f"shipments_{today_str}.csv"

    start_date = datetime.now() - timedelta(days=60)
    end_date = datetime.now()

    with open(filename, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "shipment_id",
                "customer_id",
                "origin_city",
                "destination_city",
                "created_at",
                "delivered_at",
                "status",
                "weight_kg",
                "price",
            ]
        )

        for i in range(num_rows):
            shipment_id = i + 1
            customer_id = random.randint(1, 100)
            origin = random.choice(CITIES)
            dest = random.choice([c for c in CITIES if c != origin])

            created_at = random_date(start_date, end_date)
            # 70% entregados
            if random.random() < 0.7:
                delivered_at = created_at + timedelta(hours=random.randint(12, 96))
                status = "DELIVERED"
            else:
                delivered_at = ""
                status = random.choice(["CREATED", "IN_TRANSIT", "CANCELLED"])

            weight = round(random.uniform(0.2, 20.0), 2)
            price = round(5000 + weight * random.uniform(3000, 8000), 2)

            writer.writerow(
                [
                    shipment_id,
                    customer_id,
                    origin,
                    dest,
                    created_at.isoformat(),
                    delivered_at if delivered_at == "" else delivered_at.isoformat(),
                    status,
                    weight,
                    price,
                ]
            )

    print(f"Generated file: {filename}")
    return str(filename)


if __name__ == "__main__":
    generate_shipments_csv()
