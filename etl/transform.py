from pathlib import Path
import pandas as pd
from .config import RAW_DIR, PROCESSED_DIR


def find_latest_raw_file() -> Path | None:
    files = sorted(RAW_DIR.glob("shipments_*.csv"))
    return files[-1] if files else None


def transform_shipments(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)

    # Convertir fechas
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df["delivered_at"] = pd.to_datetime(df["delivered_at"], errors="coerce")

    # Calcular tiempo de entrega en horas
    df["delivery_time_hours"] = (
        (df["delivered_at"] - df["created_at"]).dt.total_seconds() / 3600
    )

    # Regla simple de retraso: > 48 horas y entregado
    df["is_delayed"] = (df["status"] == "DELIVERED") & (
        df["delivery_time_hours"] > 48
    )

    # Eliminar filas sin created_at
    df = df.dropna(subset=["created_at"])

    # Guardar versión procesada opcional
    out_file = PROCESSED_DIR / f"processed_{csv_path.name}"
    df.to_csv(out_file, index=False)

    return df
