from pathlib import Path
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from .db import get_engine


def upsert_dimensions(df: pd.DataFrame, engine: Engine):
    # dim_customer
    customers = df[["customer_id"]].drop_duplicates()
    customers.to_sql(
        "dim_customer",
        engine,
        if_exists="append",
        index=False,
        method="multi",
    )

    # dim_city
    cities = pd.DataFrame(
        {
            "name": pd.unique(
                pd.concat([df["origin_city"], df["destination_city"]])
            )
        }
    )
    # Insert temporal, luego resolvemos ids con subqueries en fact
    cities.to_sql(
        "dim_city",
        engine,
        if_exists="append",
        index=False,
        method="multi",
    )


def load_fact_shipments(df: pd.DataFrame, engine: Engine):
    # Construimos SQL que busque city_id según nombre
    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(
                text(
                    """
                    INSERT INTO fact_shipment (
                        shipment_id, customer_id, origin_city_id, destination_city_id,
                        created_at, delivered_at, status, weight_kg, price,
                        delivery_time_hours, is_delayed
                    )
                    VALUES (
                        :shipment_id,
                        :customer_id,
                        (SELECT city_id FROM dim_city WHERE name = :origin LIMIT 1),
                        (SELECT city_id FROM dim_city WHERE name = :dest LIMIT 1),
                        :created_at,
                        :delivered_at,
                        :status,
                        :weight_kg,
                        :price,
                        :delivery_time_hours,
                        :is_delayed
                    )
                    ON CONFLICT (shipment_id) DO NOTHING
                    """
                ),
                {
                    "shipment_id": int(row["shipment_id"]),
                    "customer_id": int(row["customer_id"]),
                    "origin": row["origin_city"],
                    "dest": row["destination_city"],
                    "created_at": row["created_at"].to_pydatetime(),
                    "delivered_at": row["delivered_at"].to_pydatetime()
                    if pd.notnull(row["delivered_at"])
                    else None,
                    "status": row["status"],
                    "weight_kg": float(row["weight_kg"]),
                    "price": float(row["price"]),
                    "delivery_time_hours": float(row["delivery_time_hours"])
                    if pd.notnull(row["delivery_time_hours"])
                    else None,
                    "is_delayed": bool(row["is_delayed"]),
                },
            )


def log_etl_run(
    engine: Engine,
    source_file: str,
    rows_read: int,
    rows_loaded: int,
    rows_error: int,
):
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO etl_log (source_file, rows_read, rows_loaded, rows_error)
                VALUES (:source_file, :rows_read, :rows_loaded, :rows_error)
                """
            ),
            {
                "source_file": source_file,
                "rows_read": rows_read,
                "rows_loaded": rows_loaded,
                "rows_error": rows_error,
            },
        )
