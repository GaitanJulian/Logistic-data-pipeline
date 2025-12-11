import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

# -------------------------------------------------------------------
# 1) Asegurar esquema (dimensiones, hechos y log) si no existen
# -------------------------------------------------------------------

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS dim_city (
    city_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    segment TEXT,
    created_at TIMESTAMP WITHOUT TIME ZONE
);

CREATE TABLE IF NOT EXISTS fact_shipment (
    shipment_id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES dim_customer(customer_id),
    origin_city_id INTEGER NOT NULL REFERENCES dim_city(city_id),
    destination_city_id INTEGER NOT NULL REFERENCES dim_city(city_id),
    created_at TIMESTAMP WITHOUT TIME ZONE,
    delivered_at TIMESTAMP WITHOUT TIME ZONE,
    status TEXT,
    weight_kg NUMERIC(10,2),
    price NUMERIC(12,2),
    delivery_time_hours NUMERIC(10,2),
    is_delayed BOOLEAN
);

CREATE TABLE IF NOT EXISTS etl_log (
    id SERIAL PRIMARY KEY,
    source_file TEXT NOT NULL,
    rows_read INTEGER NOT NULL,
    rows_loaded INTEGER NOT NULL,
    rows_error INTEGER NOT NULL,
    run_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);
"""


def ensure_schema(engine: Engine) -> None:
    """
    Crea las tablas si no existen (idempotente).
    """
    statements = [
        s.strip()
        for s in SCHEMA_SQL.strip().split(";")
        if s.strip()
    ]
    with engine.begin() as conn:
        for stmt in statements:
            conn.execute(text(stmt))


# -------------------------------------------------------------------
# 2) Carga de dimensiones
# -------------------------------------------------------------------

def upsert_dimensions(df: pd.DataFrame, engine: Engine) -> None:
    """
    Carga dimensiones.
    Para este proyecto de demo:
    - Truncamos fact_shipment, dim_city y dim_customer en cada corrida.
    - Generamos un nombre y segmento sintético para el cliente.
    """
    ensure_schema(engine)

    # Asegurarnos de que las columnas mínimas existan
    required_cols = [
        "shipment_id",
        "customer_id",
        "origin_city",
        "destination_city",
        "created_at",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas en el DataFrame transformado: {missing}")

    # 1) Clientes: usamos customer_id y generamos nombre/segmento sintético
    customers_base = df[["customer_id", "created_at"]].copy()
    customers_agg = (
        customers_base
        .groupby("customer_id", as_index=False)
        .agg({"created_at": "min"})
        .rename(columns={"created_at": "created_at"})
    )

    customers_agg["name"] = customers_agg["customer_id"].apply(
        lambda cid: f"Customer {cid}"
    )
    customers_agg["segment"] = "STANDARD"

    customers = customers_agg[
        ["customer_id", "name", "segment", "created_at"]
    ]

    # 2) Ciudades: a partir de origin_city y destination_city
    all_cities = pd.concat(
        [df["origin_city"], df["destination_city"]],
        ignore_index=True,
    ).dropna()

    cities = pd.DataFrame({"name": all_cities.unique()})

    # 3) Truncar dimensiones y hechos antes de cargar
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE fact_shipment RESTART IDENTITY CASCADE;"))
        conn.execute(text("TRUNCATE TABLE dim_city RESTART IDENTITY CASCADE;"))
        conn.execute(text("TRUNCATE TABLE dim_customer RESTART IDENTITY CASCADE;"))

    # 4) Insertar dimensiones
    customers.to_sql(
        "dim_customer",
        engine,
        if_exists="append",
        index=False,
    )

    cities.to_sql(
        "dim_city",
        engine,
        if_exists="append",
        index=False,
    )


# -------------------------------------------------------------------
# 3) Carga de tabla de hechos
# -------------------------------------------------------------------

def load_fact_shipments(df: pd.DataFrame, engine: Engine) -> None:
    """
    Inserta filas en fact_shipment resolviendo las foreign keys
    contra dim_city (origin/destination) y manejando NaT / NaN como NULL.
    """
    import math
    import pandas as pd

    ensure_schema(engine)

    insert_sql = text(
        """
        INSERT INTO fact_shipment (
            shipment_id,
            customer_id,
            origin_city_id,
            destination_city_id,
            created_at,
            delivered_at,
            status,
            weight_kg,
            price,
            delivery_time_hours,
            is_delayed
        )
        VALUES (
            :shipment_id,
            :customer_id,
            (SELECT city_id FROM dim_city WHERE name = :origin_city LIMIT 1),
            (SELECT city_id FROM dim_city WHERE name = :destination_city LIMIT 1),
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
    )

    def to_ts(value):
        # NaT -> None, Timestamp -> datetime, otros -> None
        if pd.isna(value):
            return None
        # Si es Timestamp de pandas, convertir a datetime nativo
        if hasattr(value, "to_pydatetime"):
            return value.to_pydatetime()
        return value

    def to_num(value):
        # NaN -> None
        if value is None:
            return None
        try:
            if isinstance(value, float) and math.isnan(value):
                return None
        except TypeError:
            pass
        return float(value)

    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(
                insert_sql,
                {
                    "shipment_id": int(row["shipment_id"]),
                    "customer_id": int(row["customer_id"]),
                    "origin_city": row["origin_city"],
                    "destination_city": row["destination_city"],
                    "created_at": to_ts(row["created_at"]),
                    "delivered_at": to_ts(row["delivered_at"]),
                    "status": row["status"],
                    "weight_kg": to_num(row["weight_kg"]),
                    "price": to_num(row["price"]),
                    "delivery_time_hours": to_num(row["delivery_time_hours"]),
                    "is_delayed": bool(row["is_delayed"]),
                },
            )

# -------------------------------------------------------------------
# 4) Log de la corrida ETL
# -------------------------------------------------------------------

def log_etl_run(
    engine: Engine,
    source_file: str,
    rows_read: int,
    rows_loaded: int,
    rows_error: int,
) -> None:
    ensure_schema(engine)

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO etl_log (
                    source_file,
                    rows_read,
                    rows_loaded,
                    rows_error
                )
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
