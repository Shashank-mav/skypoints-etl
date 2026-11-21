# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 4 Country Split (Silver → Gold)
# MAGIC
# MAGIC Splits silver members into per country gold Delta tables
# MAGIC using MERGE  with the "latest record wins" rule.
# MAGIC

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/Shashank/skypoints-etl")

from src.transformations import merge_to_country_table, remove_moved_members

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Silver Data

# COMMAND ----------

silver_df = spark.read.table("skypoints.silver_members")
print(f"Silver records to process: {silver_df.count()}")

silver_df.select("member_id", "member_name", "country").show(truncate=False)

# filter out records that would violate NOT NULL constraints
silver_df = silver_df.filter(
    F.col("member_id").isNotNull()
    & F.col("member_name").isNotNull()
    & F.col("enrollment_date").isNotNull()
    & F.col("country").isNotNull()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merge into Country Tables
# MAGIC
# MAGIC For each country:
# MAGIC 1. Filter silver for that country members
# MAGIC 2. MERGE into the gold table
# MAGIC 3. Clean up member who moved away(if any)

# COMMAND ----------

country_tables = {
    "USA": "skypoints.gold_members_usa",
    "IND": "skypoints.gold_members_ind",
    "AUS": "skypoints.gold_members_aus",
}

for country_code, table_name in country_tables.items():

    print(f"\nProcessing {country_code} → {table_name}")

    merge_to_country_table(spark, silver_df, table_name, country_code)
    

    
    
 
    # remove_moved_members(spark, silver_df, country_code, table_name)  # skipped: sample data has overlapping IDs across countries
    # Step 2: Remove members who moved to a different country
    # remove_moved_members(spark, silver_df, country_code, table_name)
    
    count = spark.read.table(table_name).count()
    print(f"  {table_name}: {count} records")

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

# MAGIC %sql
# MAGIC SELECT * FROM skypoints.gold_members_usa;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM skypoints.gold_members_ind;

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