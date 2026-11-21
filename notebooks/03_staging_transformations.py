# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Staging Transformations (Bronze → Silver)
# MAGIC 
# MAGIC Transforms raw bronze data into the typed silver staging layer:
# MAGIC 1. Parses dates using per-country format rules
# MAGIC 2. Cleans NULL strings and invalid values
# MAGIC 3. Computes derived columns: **Age** (from DOB) and **Stale_Member** (flight > 90 days ago)

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/skypoints-etl")

from src.transformations import transform_bronze_to_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze Data

# COMMAND ----------

bronze_df = spark.read.table("skypoints.bronze_members")
print(f"Bronze records: {bronze_df.count()}")
bronze_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform to Silver

# COMMAND ----------

silver_df = transform_bronze_to_silver(bronze_df)

print(f"Silver records: {silver_df.count()}")
silver_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect Derived Columns

# COMMAND ----------

silver_df.select(
    "member_id", "member_name", "country",
    "date_of_birth", "age",
    "last_flight_date", "stale_member"
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect Date Parsing Results
# MAGIC Check that dates were parsed correctly for each country format.

# COMMAND ----------

silver_df.select(
    "member_id", "source_country",
    "enrollment_date", "last_flight_date", "date_of_birth"
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver Delta Table

# COMMAND ----------

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("skypoints.silver_members")
)

print("✓ Silver table loaded successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Silver Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT source_country, COUNT(*) as records,
# MAGIC        SUM(CASE WHEN stale_member = TRUE THEN 1 ELSE 0 END) as stale_count,
# MAGIC        SUM(CASE WHEN age IS NOT NULL THEN 1 ELSE 0 END) as has_age
# MAGIC FROM skypoints.silver_members
# MAGIC GROUP BY source_country
# MAGIC ORDER BY source_country;
