# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 01 — DDL Setup
# MAGIC Creating the database and Delta tables:
# MAGIC - Bronze: Raw landing table
# MAGIC - Silver: Staging table (with derived columns)
# MAGIC - Gold: country tables + Redemption
# MAGIC - Quarantine: Records that fails validation

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS skypoints;
# MAGIC USE skypoints;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze —> Raw (Landing) Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS skypoints.bronze_members (
# MAGIC     member_id       STRING,
# MAGIC     member_name     STRING,
# MAGIC     date_of_birth   STRING,
# MAGIC     tier_code       STRING,
# MAGIC     enrollment_date STRING,
# MAGIC     last_flight_date STRING,
# MAGIC     agent_name      STRING,
# MAGIC     state           STRING,
# MAGIC     country         STRING,
# MAGIC     post_code       STRING,
# MAGIC     is_active       STRING,
# MAGIC     individual_or_corporate STRING,
# MAGIC     source_file     STRING,
# MAGIC     source_country  STRING,
# MAGIC     load_timestamp  TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Bronze landing table — raw member data, all strings, no transformations';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver -> Staging Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS skypoints.silver_members (
# MAGIC     member_id       STRING      NOT NULL,
# MAGIC     member_name     STRING      NOT NULL,
# MAGIC     date_of_birth   DATE,
# MAGIC     tier_code       STRING,
# MAGIC     enrollment_date DATE        NOT NULL,
# MAGIC     last_flight_date DATE,
# MAGIC     agent_name      STRING,
# MAGIC     state           STRING,
# MAGIC     country         STRING      NOT NULL,
# MAGIC     post_code       STRING,
# MAGIC     is_active       STRING,
# MAGIC     individual_or_corporate STRING,
# MAGIC     age             INT         COMMENT 'Computed from DOB',
# MAGIC     stale_member    BOOLEAN     COMMENT 'TRUE if days since last_flight_date > 90',
# MAGIC     source_file     STRING,
# MAGIC     source_country  STRING,
# MAGIC     load_timestamp  TIMESTAMP,
# MAGIC     processed_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Silver staging table — typed, standardized, with derived columns';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold -> Country Tables

# COMMAND ----------

# Helper to generate gold table DDL for each country
countries = {
    "usa": "USA members",
    "ind": "India members",
    "aus": "Australia members",
}

for code, description in countries.items():
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS skypoints.gold_members_{code} (
            member_id       STRING      NOT NULL,
            member_name     STRING      NOT NULL,
            date_of_birth   DATE,
            tier_code       STRING,
            enrollment_date DATE        NOT NULL,
            last_flight_date DATE,
            agent_name      STRING,
            state           STRING,
            country         STRING      NOT NULL,
            post_code       STRING,
            is_active       STRING,
            individual_or_corporate STRING,
            age             INT,
            stale_member    BOOLEAN,
            source_file     STRING,
            load_timestamp  TIMESTAMP,
            updated_timestamp TIMESTAMP
        )
        USING DELTA
        COMMENT 'Gold table — {description}'
    """)
    print(f"Created thee skypoints.gold_members_{code}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold -> Redemptions Table 
# MAGIC > as asked in assignennt - "Parse the semi-structured JSON redemption feed into a flattened, queryable table, and describe how you would join it back to the member profile data."

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS skypoints.gold_redemptions (
# MAGIC     member_id       STRING      NOT NULL,
# MAGIC     feed_date       DATE        NOT NULL,
# MAGIC     txn_id          STRING      NOT NULL,
# MAGIC     txn_date        DATE,
# MAGIC     partner         STRING,
# MAGIC     miles_redeemed  BIGINT,
# MAGIC     status          STRING,
# MAGIC     source_file     STRING,
# MAGIC     load_timestamp  TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Gold tables ';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quarantine Table
# MAGIC > as asked in assignmet - "Create the necessary data validations — mandatory field checks, key-column uniqueness, and any checks you would add to catch the kind of data issues visible in the sample data above."

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS skypoints.quarantine_members (
# MAGIC     member_id       STRING,
# MAGIC     member_name     STRING,
# MAGIC     raw_record      STRING      COMMENT 'Full original record as a string',
# MAGIC     validation_errors STRING    COMMENT 'Comma-separated list of failed checks',
# MAGIC     source_file     STRING,
# MAGIC     source_country  STRING,
# MAGIC     quarantined_at  TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Records that failed data validation checks';

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN skypoints;