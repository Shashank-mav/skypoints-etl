# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 02 — Raw Ingestion (CSV → Bronze)
# MAGIC Reading country CSV files, maps columns to the bronze schema,
# MAGIC and write to `bronze_members` Delta table.

# COMMAND ----------

# repo root to path to  find our src/ modules
import sys
sys.path.insert(0, "/Workspace/Repos/Shashank/skypoints-etl")

from src.transformations import ingest_csv_to_bronze, ingest_all_countries

# COMMAND ----------


DATA_BASE_PATH = "/Volumes/workspace/skypoints/sample_data"

file_paths = {
    "USA": f"{DATA_BASE_PATH}/USA.csv",
    "IND": f"{DATA_BASE_PATH}/IND.csv",
    "AUS": f"{DATA_BASE_PATH}/AUS.csv",
}

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_catalog()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingestting Each Country File

# COMMAND ----------

bronze_df = ingest_all_countries(spark, file_paths)

print(f"no. of records ingested: {bronze_df.count()}")
bronze_df.show(truncate=False)

# COMMAND ----------

bronze_df.printSchema()

# COMMAND ----------

(
    bronze_df.write
    .format("delta")
    .mode("append")
    .saveAsTable("skypoints.bronze_members")
)

print("Bronze table completed success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT source_country, COUNT(*) as record_count
# MAGIC FROM skypoints.bronze_members
# MAGIC GROUP BY source_country
# MAGIC ORDER BY source_country;