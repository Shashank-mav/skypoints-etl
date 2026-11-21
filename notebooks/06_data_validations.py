# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC ### 06 Data Validations
# MAGIC
# MAGIC Runs data quality checks on the silver staging layer:
# MAGIC 1. Mandatory field checks — member_id, member_name, enrollment_date, country
# MAGIC 2. uniqueness check — duplicate member_id within a batch
# MAGIC 3. Date valid test — future dates, impossible dates, parse failures
# MAGIC 4. Tier code validation — only GLD, SLV, PLT allowed
# MAGIC 5. Date parse failc check — values like `2021-13-13`
# MAGIC
# MAGIC Records that fail validation are **quarantined** (not silently dropped).

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/Shashank/skypoints-etl")

from src.validations import (
    check_mandatory_fields,
    check_key_uniqueness,
    check_date_validity,
    check_tier_code,
    check_date_parse_failures,
    run_all_validations,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data

# COMMAND ----------

bronze_df = spark.read.table("skypoints.bronze_members")
silver_df = spark.read.table("skypoints.silver_members")

print(f"Bronze records: {bronze_df.count()}")
print(f"Silver records: {silver_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Mandatory Field Checks

# COMMAND ----------

mandatory_df = check_mandatory_fields(silver_df)
failures = mandatory_df.filter(mandatory_df.mandatory_check.isNotNull())

print(f"Records failing mandatory checks: {failures.count()}")
failures.select("member_id", "member_name", "country", "mandatory_check").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Key Uniqueness Check

# COMMAND ----------

dup_df = check_key_uniqueness(silver_df)
dups = dup_df.filter(dup_df.duplicate_check.isNotNull())

print(f"Duplicate member_id records: {dups.count()}")
dups.select("member_id", "member_name", "source_country", "duplicate_check").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Date Validity Checks

# COMMAND ----------

date_df = check_date_validity(silver_df)
date_issues = date_df.filter(date_df.date_check.isNotNull())

print(f"Records with date issues: {date_issues.count()}")
date_issues.select(
    "member_id", "member_name", "enrollment_date",
    "last_flight_date", "date_of_birth", "date_check"
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Tier Code Validation

# COMMAND ----------

tier_df = check_tier_code(silver_df)
tier_issues = tier_df.filter(tier_df.tier_check.isNotNull())

print(f"Records with invalid tier codes: {tier_issues.count()}")
tier_issues.select("member_id", "tier_code", "tier_check").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Date Parse Failure Detection

# COMMAND ----------

parse_failures = check_date_parse_failures(bronze_df, silver_df)

print(f"Date parse failures detected: {parse_failures.count()}")
parse_failures.select(
    "member_id", "source_country",
    "raw_enrollment_date", "parsed_enrollment_date",
    "raw_last_flight_date", "parsed_last_flight_date",
    "raw_date_of_birth", "parsed_date_of_birth",
    "parse_failure"
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Combined Validation 

# COMMAND ----------

valid_df, quarantine_df = run_all_validations(silver_df)

print(f"Valid records:       {valid_df.count()}")
print(f"Quarantined records: {quarantine_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect Quarantined Records

# COMMAND ----------

quarantine_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Quarantined Records

# COMMAND ----------

(
    quarantine_df.write
    .format("delta")
    .mode("append")
    .saveAsTable("skypoints.quarantine_members")
)

print("Quarantine table updated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Summary

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   'Total Bronze' as check, COUNT(*) as count FROM skypoints.bronze_members
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'Total Silver', COUNT(*) FROM skypoints.silver_members
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'Quarantined', COUNT(*) FROM skypoints.quarantine_members;