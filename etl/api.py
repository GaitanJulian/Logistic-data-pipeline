from fastapi import FastAPI
from etl.db import get_engine
from sqlalchemy import text
from etl.generate_raw_data import generate_shipments_csv
from etl.transform import transform_shipments
from etl.load import upsert_dimensions, load_fact_shipments, log_etl_run
from etl.quality import compute_quality_report

from pathlib import Path

app = FastAPI(title="Logistic ETL API")


@app.get("/health")
def health():
    engine = get_engine()
    return {"status": "ok" if engine is not None else "db_unreachable"}


@app.get("/etl/runs/latest")
def latest_run():
    engine = get_engine()
    if engine is None:
        return {"error": "Database unreachable"}

    with engine.connect() as conn:
        result = conn.execute(
            text(
                """
                SELECT id, source_file, rows_read, rows_loaded, rows_error, run_at
                FROM etl_log
                ORDER BY run_at DESC
                LIMIT 1
                """
            )
        )
        row = result.mappings().first()
        if row is None:
            return {"message": "No ETL runs yet"}
        return dict(row)


@app.post("/etl/run")
def run_etl_once(num_rows: int = 200):
    """
    Ejecuta el ETL una vez, generando datos sintéticos y cargándolos.
    """
    engine = get_engine()
    if engine is None:
        return {"error": "Database unreachable"}

    csv_path_str = generate_shipments_csv(num_rows=num_rows)
    csv_path = Path(csv_path_str)
    df = transform_shipments(csv_path)

    rows_read = len(df)
    rows_loaded = 0
    rows_error = rows_read

    upsert_dimensions(df, engine)
    try:
        load_fact_shipments(df, engine)
        rows_loaded = len(df)
        rows_error = 0
    except Exception:
        rows_loaded = 0
        rows_error = rows_read

    log_etl_run(
        engine,
        source_file=csv_path.name,
        rows_read=rows_read,
        rows_loaded=rows_loaded,
        rows_error=rows_error,
    )

    return {
        "message": "ETL run completed",
        "rows_read": rows_read,
        "rows_loaded": rows_loaded,
        "rows_error": rows_error,
    }


@app.get("/etl/quality/sample")
def quality_sample(num_rows: int = 200):
    """
    Genera un lote de datos sintéticos, calcula el reporte de calidad
    pero NO lo carga en la base de datos.
    """
    csv_path_str = generate_shipments_csv(num_rows=num_rows)
    csv_path = Path(csv_path_str)
    df = transform_shipments(csv_path)

    report = compute_quality_report(df)
    return report
