from etl.generate_raw_data import generate_shipments_csv
from etl.transform import transform_shipments
from etl.load import upsert_dimensions, load_fact_shipments, log_etl_run
from etl.db import get_engine
from etl.quality import compute_quality_report, print_quality_report
from pathlib import Path

def main():
    # 1) Generar un nuevo archivo raw
    csv_path_str = generate_shipments_csv(num_rows=500)
    print(f"Using raw file: {csv_path_str}")

    csv_path = Path(csv_path_str)

    # 2) Transform
    df = transform_shipments(csv_path)
    print(f"Transformed rows: {len(df)}")

    # 2.1) Data Quality (solo sobre el DataFrame en memoria)
    quality_report = compute_quality_report(df)
    print_quality_report(quality_report)

    engine = get_engine()

    rows_read = len(df)
    rows_loaded = 0
    rows_error = rows_read

    if engine is None:
        print("Skipping database loads due to unreachable Postgres.")
    else:
        # 3) Load dimensiones
        print(">> Loading dimensions...")
        upsert_dimensions(df, engine)
        print(">> Dimensions loaded.")

        # 4) Load fact
        print(">> Loading fact_shipment...")
        try:
            load_fact_shipments(df, engine)
            rows_loaded = len(df)
            rows_error = 0
            print(">> fact_shipment loaded.")
        except Exception as e:
            print(f"Error loading fact shipments: {e}")
            rows_loaded = 0
            rows_error = len(df)

        # 5) Log
        print(">> Writing ETL log entry...")
        log_etl_run(
            engine,
            source_file=csv_path.name,
            rows_read=rows_read,
            rows_loaded=rows_loaded,
            rows_error=rows_error,
        )

    print("ETL run completed.")



if __name__ == "__main__":
    main()
