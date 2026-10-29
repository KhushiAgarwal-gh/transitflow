# 🚍 TransitFlow — Public Transport Data Pipeline

**Python | SQL | PySpark | Databricks | Delta Lake**

End-to-end data engineering project: ingest, clean and transform public transport data
(trip records, passenger counts, route info) using a **Bronze → Silver → Gold** medallion
architecture, then analyse route-wise ridership, passenger trends and trip delays with SQL.

## Highlights
- Pipeline for trip records, passenger counts and route information
- Bronze / Silver / Gold layers for organised, consistent data
- SQL analytics: route-wise ridership, passenger trends (7-day moving avg, WoW), trip delays (avg, p90, % delayed), peak hours, data-quality checks
- Runs **locally** (PySpark) and on **Databricks** (notebooks in `databricks/`)

## Structure
```
transitflow/
├── data/raw/              # routes.csv, trips.csv, passenger_counts.csv (generated, intentionally dirty)
├── src/
│   ├── generate_data.py   # sample data generator
│   ├── bronze.py          # raw ingestion + metadata
│   ├── silver.py          # cleaning / validation
│   ├── gold.py            # aggregates
│   ├── spark_utils.py     # Spark session + Delta helpers
│   └── run_pipeline.py    # runs everything + prints SQL reports
├── sql/                   # analytics queries (01..05)
├── databricks/            # Databricks notebooks (bronze, silver, gold, SQL)
├── tests/                 # pytest unit tests for silver logic
└── docs/architecture.md
```

## Run locally
```bash
pip install -r requirements.txt     # needs Java 8/11/17
python -m src.run_pipeline          # generate data -> bronze -> silver -> gold -> SQL reports
pytest -q
```
Output tables are written to `lakehouse/{bronze,silver,gold}`.
Set `TRANSITFLOW_FORMAT=parquet` to skip Delta.

## Run on Databricks
1. Upload the 3 CSVs from `data/raw/` to a Volume (`/Volumes/main/transitflow/raw`).
2. Import `databricks/*.py` as notebooks, run `01 → 02 → 03`.
3. Use `04_analytics_queries.sql` for Databricks SQL dashboards.

## Data quality issues handled (Silver)
| Issue in raw data | Fix |
|---|---|
| Duplicate trip records | Window `row_number()` – keep latest ingested |
| Lowercase route IDs | `upper(trim())` |
| Missing `actual_departure` | Kept as NULL, excluded from delay metrics |
| Negative passenger counts | Filtered out |
| Missing alighted count | Filled with boarded count |
| Trips with unknown route | Moved to `_rejected_trips` |

## Possible extensions
Structured Streaming / Auto Loader, Delta `MERGE` for incremental loads, Databricks Workflows scheduling, Power BI / Databricks SQL dashboard.
