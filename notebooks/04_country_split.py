# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Country Split (Silver → Gold)
# MAGIC 
# MAGIC Splits validated silver members into per-country gold Delta tables
# MAGIC using **MERGE** (upsert) with the **"latest record wins"** rule.
# MAGIC 
# MAGIC Also handles members who moved countries: removes them from the
# MAGIC old country table and inserts into the new one.

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/skypoints-etl")

from src.transformations import merge_to_country_table, remove_moved_members

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Silver Data

# COMMAND ----------

silver_df = spark.read.table("skypoints.silver_members")
print(f"Silver records to process: {silver_df.count()}")

silver_df.select("member_id", "member_name", "country").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merge into Country Tables
# MAGIC 
# MAGIC For each country:
# MAGIC 1. Filter silver for that country's members
# MAGIC 2. MERGE into the gold table (insert new, update if newer record exists)
# MAGIC 3. Clean up members who moved away

# COMMAND ----------

country_tables = {
    "USA": "skypoints.gold_members_usa",
    "IND": "skypoints.gold_members_ind",
    "AUS": "skypoints.gold_members_aus",
}

for country_code, table_name in country_tables.items():
    print(f"\nProcessing {country_code} → {table_name}")
    
    # Step 1: Merge new/updated records
    merge_to_country_table(spark, silver_df, table_name, country_code)
    
    # Step 2: Remove members who moved to a different country
    remove_moved_members(spark, silver_df, country_code, table_name)
    
    count = spark.read.table(table_name).count()
    print(f"  ✓ {table_name}: {count} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Gold Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'USA' as country, COUNT(*) as records FROM skypoints.gold_members_usa
# MAGIC UNION ALL
# MAGIC SELECT 'IND', COUNT(*) FROM skypoints.gold_members_ind
# MAGIC UNION ALL
# MAGIC SELECT 'AUS', COUNT(*) FROM skypoints.gold_members_aus;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect USA Gold Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM skypoints.gold_members_usa;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect IND Gold Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM skypoints.gold_members_ind;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect AUS Gold Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM skypoints.gold_members_aus;

# COMMAND ----------

# MAGIC %md
# MAGIC ## "Latest Record Wins" — How It Works
# MAGIC 
# MAGIC The MERGE uses `load_timestamp` to determine which record is newer:
# MAGIC ```sql
# MAGIC MERGE INTO gold_members_usa AS target
# MAGIC USING source_members AS source
# MAGIC ON target.member_id = source.member_id
# MAGIC WHEN MATCHED AND source.load_timestamp > target.load_timestamp THEN
# MAGIC     UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT *
# MAGIC ```
# MAGIC 
# MAGIC For **country changes** (e.g., member moves from IND to USA):
# MAGIC 1. The new country table gets the INSERT via MERGE
# MAGIC 2. `remove_moved_members()` deletes the member from the old country table
# MAGIC 
# MAGIC This ensures each member exists in exactly one country table at any time.
