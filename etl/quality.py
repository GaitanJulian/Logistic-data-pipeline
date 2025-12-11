import pandas as pd
from typing import Dict, Any


def compute_quality_report(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Calcula métricas básicas de calidad de datos sobre el DataFrame transformado.
    """
    report: Dict[str, Any] = {}

    # Tamaño general
    report["row_count"] = len(df)

    # Nulos por columna
    report["null_counts"] = df.isna().sum().to_dict()

    # Duplicados de shipment_id (si existe la columna)
    if "shipment_id" in df.columns:
        report["duplicate_shipment_id"] = int(df["shipment_id"].duplicated().sum())
    else:
        report["duplicate_shipment_id"] = None

    # Valores negativos en métricas numéricas
    negative_values: Dict[str, int] = {}
    for col in ["weight_kg", "price", "delivery_time_hours"]:
        if col in df.columns:
            negative_values[col] = int((df[col] < 0).sum())
    report["negative_values"] = negative_values

    # Filas con problemas en entrega
    invalid_delivery_rows = 0
    if {"status", "delivered_at", "delivery_time_hours"}.issubset(df.columns):
        cond_invalid = (
            (df["status"] == "DELIVERED") & df["delivered_at"].isna()
        ) | (df["delivery_time_hours"] < 0)
        invalid_delivery_rows = int(cond_invalid.sum())
    report["invalid_delivery_rows"] = invalid_delivery_rows

    return report


def print_quality_report(report: Dict[str, Any]) -> None:
    """
    Imprime un resumen legible del reporte de calidad.
    """
    print("\n=== DATA QUALITY REPORT ===")
    print(f"Total rows: {report['row_count']}")
    print(f"Duplicate shipment_id: {report.get('duplicate_shipment_id')}")

    print("\nNulls by column:")
    for col, cnt in report["null_counts"].items():
        print(f"  - {col}: {cnt}")

    print("\nNegative values:")
    for col, cnt in report["negative_values"].items():
        print(f"  - {col}: {cnt}")

    print(f"\nInvalid delivery rows: {report.get('invalid_delivery_rows')}")
    print("=== END OF QUALITY REPORT ===\n")
