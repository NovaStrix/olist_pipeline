# Olist E-Commerce ETL Pipeline

**A modular Data Engineering pipeline leveraging PySpark, Airflow, and PostgreSQL to transform raw e-commerce data into an analytical Star Schema.**

> [!WARNING]
> **Airflow Orchestration Status:** The Airflow DAG (`ecom_pipeline.py`) is currently a work-in-progress and is not yet fully functional. Due to system RAM limitations, running the full Airflow stack (Webserver, Scheduler, and Workers) alongside PySpark may lead to performance issues or crashes. It is highly recommended to run the scripts manually as described in the **Low RAM Mode** section below.

## System Architecture

This project implements a standard ETL (Extract, Transform, Load) pattern. It is designed to be orchestrated by Apache Airflow, but the core logic is decoupled into standalone Python scripts for flexibility and independent testing.

### Data Flow

`Kaggle API` → **Extract** (CSV) → **Clean** (Parquet) → **Transform** (Star Schema) → **Load** (PostgreSQL)

| Stage | Script | Technology | Key Operation |
| :--- | :--- | :--- | :--- |
| **1. Extract** | `src/extract.py` | PySpark, KaggleHub | Downloads raw CSVs & performs null audits. |
| **2. Clean** | `src/cleaning.py` | PySpark | Enforces strict schemas & casts types. |
| **3. Transform** | `src/transform.py` | PySpark | Models data into Facts & Dimensions. |
| **4. Load** | `src/load_to_dwh.py` | Pandas, SQLAlchemy | Uploads modeled data to Supabase/Postgres. |

## Deep Dive: Core Modules

### 1. Extraction (`src/extract.py`)
Responsible for ingesting the "Brazilian E-Commerce Public Dataset by Olist" directly from Kaggle.
* **Logic:** Clears the staging area (`data/raw`) to ensure idempotency.
* **Normalization:** Renames files (e.g., stripping `_dataset.csv` suffixes) to standardize table names.
* **Audit:** Automatically runs a null-check across all columns to log data quality issues immediately upon ingestion.

### 2. Cleaning (`src/cleaning.py`)
The "Gatekeeper" module. It converts raw string data into typed formats optimized for analysis.
* **Type Casting:** Converts price strings to `Doubles`, counts to `Integers`, and dates to `Timestamps`.
* **Filtering:** Removes records lacking essential primary keys (e.g., `geolocation` entries without lat/long).
* **Format Switch:** Output is saved as **Parquet** (gzip compressed) in `data/processed`, significantly reducing I/O overhead.

### 3. Transformation (`src/transform.py`)
Implements Dimensional Modeling to prepare the data for Business Intelligence tools.
* **Fact Tables:** `fact_orders` (includes `is_delivered` flag), `fact_reviews` (adds `has_review` flag), `fact_payments`.
* **Dimension Tables:** `dim_customers`, `dim_products`, `dim_sellers`.
* **Output:** Tables are stored in `data/output` ready for final loading.

### 4. Loading (`src/load_to_dwh.py`)
The final bridge to the Data Warehouse (Supabase).
* **Credentials:** Loads securely from `.env`.
* **Strategy:** Uses `if_exists='replace'`, effectively treating the DWH as a mirror of the pipeline's latest state.
* **Hybrid Approach:** Uses Spark for reading Parquet files, but converts to **Pandas** to utilize SQLAlchemy's robust database connectors.

## Orchestration (Airflow)

The pipeline is defined in `dags/ecom_pipeline.py`.
* **DAG ID:** `olist_spark_pipeline`
* **Operators:** Uses `SparkSubmitOperator` to launch tasks.
* **Concurrency:** Set to `max_active_runs=1` to prevent resource contention.
* **Retry Policy:** Configured with 3 retries and a 5-minute delay.

## Developer Notes

### Memory Constraints (RAM)
The pipeline relies heavily on PySpark. Even in local mode, Spark allocates a JVM heap.
* **Bottleneck:** The `load_to_dwh.py` script uses `.toPandas()`. This collects all distributed data into the driver's memory. On systems with <8GB RAM, this may cause an OOM (Out of Memory) crash.
* **Workaround:** Run the scripts sequentially via Python directly (Low RAM Mode).

### Path Dependencies
A standard directory structure (`/opt/airflow/data/...`) is expected by the scripts. If running locally (outside Docker), ensure you manually create `data/raw`, `data/processed`, and `data/output`.

### Database Connectivity
Requires a `.env` file in the root directory:

```env
user=postgres
password=your_password
host=db.your-supabase-url.com
port=5432
dbname=postgres