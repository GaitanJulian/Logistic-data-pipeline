# Logistic Data Pipeline

A production-grade ETL pipeline designed to simulate, transform, and load logistics shipment data into a dimensional warehouse using PostgreSQL. The system includes automated data generation, cleansing, dimensional modeling, fact loading, audit logging, data quality validation, and optional orchestration with cron jobs or Airflow.

## 🚚 Overview

This project demonstrates a fully functional backend ETL workflow suitable for data engineering portfolios or real logistic scenarios. It features:

- Automated raw CSV generation  
- Transformation and standardization  
- Dimension upserts and fact table loading  
- ETL audit logging  
- Data Quality Reports  
- Dockerized PostgreSQL warehouse  
- Optional API exposure for metrics  
- Optional BI dashboard integration (Metabase/Superset)

---

## 📁 Project Structure

```
logistic-data-pipeline/
│── data/
│   ├── raw/               # Generated CSV files
│   └── processed/         # Cleaned and transformed data
│── etl/
│   ├── generate_raw_data.py
│   ├── transform.py
│   ├── load.py
│   ├── quality.py         # Data quality validation
│   ├── api.py             # Optional FastAPI service
│   └── db.py
│── dashboard/
│   ├── docker-compose.yml # Superset or Metabase
│── run_pipeline.py        # Main entry point
│── docker-compose.yml     # Postgres warehouse
```

---

## 🏗️ Architecture

### 🔸 1. **Extract**
Randomized logistic shipment records containing:
- customer_id, customer_name, segment  
- shipment timestamps  
- origin/destination cities  
- weight, price, status  

### 🔸 2. **Transform**
Data cleansing:
- Timestamp normalization  
- Derived delivery time  
- Delay detection  
- Null handling  
- Data type conversions  

### 🔸 3. **Load**
- `dim_customer` (upsert)  
- `dim_city` (upsert)  
- `fact_shipment` (insert)  
- `etl_log` (audit)  

---

## 🧪 Data Quality

The pipeline generates a quality report checking:
- Null values  
- Duplicate keys  
- Impossible delivery times  
- Negative weights or prices  
- Unknown cities or customers  

Example output:

```
QUALITY REPORT — 2025-12-11
Missing values: 0
Duplicate shipment_id: 0
Invalid delivery_time_hours: 3 rows
Rows with NaT delivered_at: 2 rows
```

---

## ⏱️ Automation (Cron or Airflow)

### Example cron job (Linux / WSL)

```
0 * * * * /usr/bin/python3 /path/run_pipeline.py >> /var/log/etl.log 2>&1
```

## 📊 Dashboard (Metabase)

A BI layer is provided for:
- On-time delivery rate  
- Revenue per city  
- Shipment volume heatmaps  
- Customer segmentation analysis  

Run:

```
cd dashboard
docker compose up -d
```

Access at:

```
http://localhost:3000
```

---

## 🌐 Optional API

A FastAPI microservice exposes:
- Data quality status  
- Recent ETL runs  
- Summary metrics  
- Shipment volume trends  

Start the API:

```
uvicorn etl.api:app --reload --port 8000
```

---

## 🐳 Running the Project

### 1. Start PostgreSQL

```
docker compose up -d
```

### 2. Run the ETL

```
python run_pipeline.py
```

### 3. View logs

```
docker exec -it logistic-data-pipeline-db-1 psql -U etl_user -d logistics_dw
SELECT * FROM etl_log;
```

---

## 📝 Author

Developed by **Julián Andrés Gaitán Hernández**  
Data & Backend Engineer • Portfolio Project

---

## 📄 License

MIT License.
