# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Raw Ingestion (CSV → Bronze)
# MAGIC Reads country CSV files, maps columns to the unified bronze schema,
# MAGIC and writes to the `bronze_members` Delta table.
# MAGIC 
# MAGIC All values are kept as **strings** — no type casting or transformations here.

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/skypoints-etl")

from src.transformations import ingest_csv_to_bronze, ingest_all_countries

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC Update these paths to match where you uploaded the sample data files.

# COMMAND ----------

# Path to the uploaded sample data (adjust to your DBFS or Volume path)
DATA_BASE_PATH = "/Workspace/Repos/skypoints-etl/data/sample"

file_paths = {
    "USA": f"{DATA_BASE_PATH}/USA.csv",
    "IND": f"{DATA_BASE_PATH}/IND.csv",
    "AUS": f"{DATA_BASE_PATH}/AUS.csv",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Each Country File

# COMMAND ----------

bronze_df = ingest_all_countries(spark, file_paths)

print(f"Total records ingested: {bronze_df.count()}")
bronze_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Schema

# COMMAND ----------

bronze_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Bronze Delta Table

# COMMAND ----------

(
    bronze_df.write
    .format("delta")
    .mode("append")
    .saveAsTable("skypoints.bronze_members")
)

print("✓ Bronze table loaded successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Bronze Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT source_country, COUNT(*) as record_count
# MAGIC FROM skypoints.bronze_members
# MAGIC GROUP BY source_country
# MAGIC ORDER BY source_country;
