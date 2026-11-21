# SkyPoints ETL Pipeline

---
*README drafted with help from AI tools.*

An end-to-end ETL pipeline for the **SkyPoints** airline loyalty program, built on **Databricks** with **PySpark** and **Delta Lake**.

## Problem

SkyPoints maintains millions of members globally. Member data arrives daily from multiple countries in different file formats (CSV, JSON) with inconsistent schemas and date formats. This pipeline:

1. **Ingests** raw member files into a bronze landing layer
2. **Standardizes** schemas, computes derived columns (Age, Stale Member flag), and loads a silver staging layer
3. **Splits** members into per-country gold tables using a "latest record wins" merge strategy
4. **Parses** semi-structured JSON redemption feeds into a flattened, queryable table
5. **Validates** data quality — mandatory fields, key uniqueness, date integrity

## Architecture

```
Raw Files (CSV/JSON)
        │
        ▼
┌─────────────────┐
│   Bronze Layer   │  Raw ingestion, all strings, source metadata
│  (Landing Table) │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Silver Layer   │  Standardized schema, typed columns,
│ (Staging Table)  │  derived: Age, Stale_Member
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│    Gold Layer    │  Per-country Delta tables (Table_USA, Table_IND, Table_AUS, ...)
│ (Country Tables) │  MERGE with "latest record wins"
└─────────────────┘

JSON Redemption Feed ──▶ Gold: Redemptions table (flattened) ──▶ JOIN on Member_ID
```

## Data Challenges

The source files intentionally have inconsistent formats that the pipeline handles:

| Source | Date Format        | Columns              | Issues                        |
|--------|--------------------|----------------------|-------------------------------|
| USA    | `MMDDYYYY` (no sep)| 5 cols, no DOB       | Ambiguous date parsing        |
| IND    | `M/D/YYYY`         | 7 cols, has DOB      | Extra column (Ind/Corp)       |
| AUS    | `YYYY-MM-DD`       | 6 cols, has DOB      | NULL DOB, invalid date (month=13) |

## Repo Structure

```
skypoints-etl/
├── README.md
├── notebooks/
│   ├── 01_ddl_setup.py              # Create Bronze/Silver/Gold Delta tables
│   ├── 02_ingestion_raw.py          # Ingest CSVs into Bronze
│   ├── 03_staging_transformations.py # Standardize → Silver + derived columns
│   ├── 04_country_split.py          # Merge into per-country Gold tables
│   ├── 05_json_redemption_feed.py   # Parse & flatten JSON feed
│   └── 06_data_validations.py       # Data quality checks
├── src/
│   ├── schemas.py                   # Schema definitions & column mappings
│   ├── transformations.py           # Transformation functions
│   ├── validations.py               # Validation logic
│   └── utils.py                     # Helpers (date parsing, etc.)
├── tests/
│   ├── test_transformations.py
│   └── test_validations.py
├── data/
│   └── sample/                      # Sample source files
│       ├── USA.csv
│       ├── IND.csv
│       ├── AUS.csv
│       └── redemptions.json
└── ddl/
    └── create_tables.sql            # Standalone DDL reference
```

## Tech Stack

- **Databricks** (workspace + notebooks)
- **PySpark** (distributed processing)
- **Delta Lake** (ACID transactions, MERGE, schema evolution)
- **Spark SQL** (transformations & queries)
- **pytest** (unit tests)

## How to Run

1. Clone this repo into your Databricks workspace via Repos
2. Upload sample data to DBFS or a Unity Catalog volume
3. Run notebooks in order: `01` → `02` → `03` → `04` → `05` → `06`

## Design Decisions

- **All-strings bronze layer**: Raw data is stored exactly as received; type casting happens in the silver layer so we never lose data on ingestion.
- **Delta Lake MERGE for country split**: Handles the "latest record wins" rule atomically — if a member moves from IND to USA, the old country table row is soft-deleted and the new one is inserted.
- **Computed columns in silver**: Age and Stale_Member are derived at the staging layer so every downstream consumer gets them for free.
- **Validation-first approach**: Data quality checks run before any transformation, quarantining bad records rather than silently dropping them.
