# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 5 — JSON Redemption Feed
# MAGIC
# MAGIC Parses the semi structured JSON redemption into  flattened,
# MAGIC queryable Delta table. Each nested transaction becomes own row.
# MAGIC
# MAGIC ## Input Format
# MAGIC ```json
# MAGIC {
# MAGIC   "member_id": "223457",
# MAGIC   "feed_date": "20240115",
# MAGIC   "redemptions": [
# MAGIC     {"txn_id": "RX10091", "txn_date": "20240110", "partner": "AeroLink", "miles_redeemed": 12000, "status": "COMPLETED"}
# MAGIC   ]
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ## Output
# MAGIC One row per transaction with `member_id`, `feed_date`, and all transaction fields.

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/Shashank/skypoints-etl")

from src.transformations import parse_redemption_json

# COMMAND ----------

JSON_PATH = "/Volumes/workspace/skypoints/sample_data/redemptions.json"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse + Flatten JSON

# COMMAND ----------

redemptions_df = parse_redemption_json(spark, JSON_PATH)

print(f"Total transactions: {redemptions_df.count()}")
redemptions_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema

# COMMAND ----------

redemptions_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Gold Redemptions Table

# COMMAND ----------

(
    redemptions_df.write
    .format("delta")
    .mode("append")
    .saveAsTable("skypoints.gold_redemptions")
)

print("Redemptions table loaded done")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM skypoints.gold_redemptions ORDER BY member_id, txn_date;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Joining Redemptions Back to Member Profile
# MAGIC
# MAGIC The redemptions table joins to any member table on `member_id`:
# MAGIC
# MAGIC ```sql
# MAGIC -- Example: Total miles redeemed per member with their profile info
# MAGIC SELECT
# MAGIC     m.member_id,
# MAGIC     m.member_name,
# MAGIC     m.tier_code,
# MAGIC     m.country,
# MAGIC     SUM(r.miles_redeemed) AS total_miles_redeemed,
# MAGIC     COUNT(r.txn_id) AS total_transactions
# MAGIC FROM skypoints.silver_members m
# MAGIC INNER JOIN skypoints.gold_redemptions r
# MAGIC     ON m.member_id = r.member_id
# MAGIC GROUP BY m.member_id, m.member_name, m.tier_code, m.country
# MAGIC ORDER BY total_miles_redeemed DESC;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join Example

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     m.member_id,
# MAGIC     m.member_name,
# MAGIC     m.tier_code,
# MAGIC     m.country,
# MAGIC     SUM(r.miles_redeemed) AS total_miles_redeemed,
# MAGIC     COUNT(r.txn_id) AS total_transactions,
# MAGIC     COLLECT_LIST(r.partner) AS partners_used
# MAGIC FROM skypoints.silver_members m
# MAGIC INNER JOIN skypoints.gold_redemptions r
# MAGIC     ON m.member_id = r.member_id
# MAGIC GROUP BY m.member_id, m.member_name, m.tier_code, m.country
# MAGIC ORDER BY total_miles_redeemed DESC;