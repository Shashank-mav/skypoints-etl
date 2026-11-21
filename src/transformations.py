"""
Transformation functions for SkyPoints ETL.

Covers:
  - Bronze ingestion (CSV → raw Delta)
  - Silver staging (standardize types, add derived columns)
  - Gold country split (MERGE / upsert per country)
  - JSON redemption feed parsing
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, ArrayType
)

from src.schemas import COLUMN_MAPPINGS, DATE_FORMATS, BRONZE_COLUMNS
from src.utils import (
    parse_date_column,
    compute_age,
    compute_stale_member,
    add_metadata_columns,
)


# =============================================================================
# BRONZE — Raw Ingestion
# =============================================================================

def ingest_csv_to_bronze(
    spark: SparkSession,
    file_path: str,
    country_code: str,
) -> DataFrame:
    """
    Read a country CSV file and map it to the unified bronze schema.

    All values are kept as strings. Columns not present in the source
    file are filled with NULL. Metadata columns are added.

    Args:
        spark: Active SparkSession
        file_path: Path to the CSV file
        country_code: Country code (USA, IND, AUS) — must exist in COLUMN_MAPPINGS

    Returns:
        DataFrame matching the bronze schema
    """
    if country_code not in COLUMN_MAPPINGS:
        raise ValueError(
            f"Unknown country code '{country_code}'. "
            f"Expected one of: {list(COLUMN_MAPPINGS.keys())}"
        )

    mapping = COLUMN_MAPPINGS[country_code]

    # Read CSV — all as strings to preserve raw data
    df = spark.read.option("header", "true").csv(file_path)

    # Rename source columns to unified names
    for src_col, tgt_col in mapping.items():
        if src_col in df.columns:
            df = df.withColumnRenamed(src_col, tgt_col)

    # Country is implied by the file, not always a column in the data
    if "country" not in df.columns:
        df = df.withColumn("country", F.lit(country_code))

    # Ensure all bronze columns exist (fill missing with NULL)
    for col_name in BRONZE_COLUMNS:
        if col_name in ("source_file", "source_country", "load_timestamp"):
            continue  # added by metadata function
        if col_name not in df.columns:
            df = df.withColumn(col_name, F.lit(None).cast(StringType()))

    # Add metadata
    file_name = file_path.split("/")[-1]
    df = add_metadata_columns(df, file_name, country_code)

    # Select columns in consistent order
    return df.select(BRONZE_COLUMNS)


def ingest_all_countries(
    spark: SparkSession,
    file_paths: dict,
) -> DataFrame:
    """
    Ingest multiple country files and union them into one bronze DataFrame.

    Args:
        spark: Active SparkSession
        file_paths: Dict of {country_code: file_path}

    Returns:
        Unioned bronze DataFrame
    """
    dfs = []
    for country_code, path in file_paths.items():
        df = ingest_csv_to_bronze(spark, path, country_code)
        dfs.append(df)

    result = dfs[0]
    for df in dfs[1:]:
        result = result.unionByName(df)

    return result


# =============================================================================
# SILVER — Staging Transformations
# =============================================================================

def transform_bronze_to_silver(df: DataFrame) -> DataFrame:
    """
    Transform bronze data into the silver staging layer.

    Steps:
      1. Parse date columns per country using the correct format
      2. Handle NULL and invalid values (e.g. 'NULL' string → actual null)
      3. Compute derived columns: age, stale_member
      4. Add processed_timestamp

    Args:
        df: Bronze DataFrame

    Returns:
        Silver DataFrame with proper types and derived columns
    """
    # --- Step 1: Clean string NULLs ---
    # Some files use the literal string 'NULL' instead of actual nulls
    for col_name in df.columns:
        df = df.withColumn(
            col_name,
            F.when(
                F.upper(F.trim(F.col(col_name))) == "NULL", None
            ).otherwise(F.col(col_name))
        )

    # --- Step 2: Parse dates per country ---
    # We process each country's rows with their specific date formats
    result_dfs = []
    countries = [row.source_country for row in df.select("source_country").distinct().collect()]

    for country in countries:
        country_df = df.filter(F.col("source_country") == country)
        formats = DATE_FORMATS.get(country, {})

        for date_col, fmt in formats.items():
            if date_col in country_df.columns:
                country_df = country_df.withColumn(
                    date_col,
                    parse_date_column(date_col, fmt)
                )

        # Parse any remaining date columns that aren't country-specific
        for date_col in ["enrollment_date", "last_flight_date", "date_of_birth"]:
            if date_col not in formats and date_col in country_df.columns:
                # Try ISO format as fallback
                country_df = country_df.withColumn(
                    date_col,
                    F.to_date(F.col(date_col), "yyyy-MM-dd")
                )

        result_dfs.append(country_df)

    silver_df = result_dfs[0]
    for rdf in result_dfs[1:]:
        silver_df = silver_df.unionByName(rdf)

    # --- Step 3: Compute derived columns ---
    silver_df = silver_df.withColumn("age", compute_age("date_of_birth"))
    silver_df = silver_df.withColumn("stale_member", compute_stale_member("last_flight_date"))

    # --- Step 4: Add processing timestamp ---
    silver_df = silver_df.withColumn("processed_timestamp", F.current_timestamp())

    return silver_df


# =============================================================================
# GOLD — Country Split with MERGE
# =============================================================================

def get_country_members(df: DataFrame, country_code: str) -> DataFrame:
    """Filter silver DataFrame for a specific country."""
    return df.filter(F.col("country") == country_code)


def merge_to_country_table(
    spark: SparkSession,
    source_df: DataFrame,
    target_table: str,
    country_code: str,
):
    """
    Merge (upsert) members into a country-specific gold table.

    Uses "latest record wins" — if a member already exists, we update
    only if the incoming record is newer (by load_timestamp).

    For members who moved countries: the new country's table gets the
    record via INSERT, and a separate cleanup removes them from the
    old country table.

    Args:
        spark: Active SparkSession
        source_df: Silver DataFrame filtered for this country
        target_table: Fully qualified Delta table name
        country_code: Country code for filtering
    """
    country_df = get_country_members(source_df, country_code)

    if country_df.count() == 0:
        return

    country_df = country_df.withColumn("updated_timestamp", F.current_timestamp())

    # Drop silver-specific columns not in gold schema
    gold_columns = [
        "member_id", "member_name", "date_of_birth", "tier_code",
        "enrollment_date", "last_flight_date", "agent_name", "state",
        "country", "post_code", "is_active", "individual_or_corporate",
        "age", "stale_member", "source_file", "load_timestamp",
        "updated_timestamp",
    ]
    country_df = country_df.select(
        [c for c in gold_columns if c in country_df.columns]
    )

    country_df.createOrReplaceTempView("source_members")

    merge_sql = f"""
        MERGE INTO {target_table} AS target
        USING source_members AS source
        ON target.member_id = source.member_id
        WHEN MATCHED AND source.load_timestamp > target.load_timestamp THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
    """
    spark.sql(merge_sql)


def remove_moved_members(
    spark: SparkSession,
    silver_df: DataFrame,
    country_code: str,
    target_table: str,
):
    """
    Remove members who have moved to a different country.

    If a member exists in this country's gold table but their latest
    record in silver shows a different country, delete them.

    Args:
        spark: Active SparkSession
        silver_df: Full silver DataFrame (all countries)
        country_code: This table's country
        target_table: Fully qualified Delta table name
    """
    # Find members whose latest country != this table's country
    latest_country = (
        silver_df
        .orderBy(F.col("load_timestamp").desc())
        .dropDuplicates(["member_id"])
        .select("member_id", "country")
    )

    latest_country.createOrReplaceTempView("latest_country")

    delete_sql = f"""
        DELETE FROM {target_table}
        WHERE member_id IN (
            SELECT member_id
            FROM latest_country
            WHERE country != '{country_code}'
        )
    """
    spark.sql(delete_sql)


# =============================================================================
# JSON Redemption Feed — Parse & Flatten
# =============================================================================

def parse_redemption_json(
    spark: SparkSession,
    json_path: str,
) -> DataFrame:
    """
    Parse the semi-structured JSON redemption feed into a flat table.

    Input JSON structure (one JSON object per line):
    {
        "member_id": "223457",
        "feed_date": "20240115",
        "redemptions": [
            {"txn_id": "RX10091", "txn_date": "20240110", ...},
            ...
        ]
    }

    Output: One row per transaction with member_id, feed_date, and
    all transaction fields flattened out.

    Args:
        spark: Active SparkSession
        json_path: Path to the JSON file (newline-delimited JSON)

    Returns:
        Flattened DataFrame with one row per redemption transaction
    """
    # Define the schema explicitly for reliability
    redemption_schema = StructType([
        StructField("txn_id", StringType(), True),
        StructField("txn_date", StringType(), True),
        StructField("partner", StringType(), True),
        StructField("miles_redeemed", LongType(), True),
        StructField("status", StringType(), True),
    ])

    json_schema = StructType([
        StructField("member_id", StringType(), True),
        StructField("feed_date", StringType(), True),
        StructField("redemptions", ArrayType(redemption_schema), True),
    ])

    # Read newline-delimited JSON
    raw_df = spark.read.schema(json_schema).json(json_path)

    # Explode the redemptions array — one row per transaction
    flat_df = (
        raw_df
        .withColumn("redemption", F.explode("redemptions"))
        .select(
            F.col("member_id"),
            F.to_date(F.col("feed_date"), "yyyyMMdd").alias("feed_date"),
            F.col("redemption.txn_id").alias("txn_id"),
            F.to_date(F.col("redemption.txn_date"), "yyyyMMdd").alias("txn_date"),
            F.col("redemption.partner").alias("partner"),
            F.col("redemption.miles_redeemed").alias("miles_redeemed"),
            F.col("redemption.status").alias("status"),
        )
        .withColumn("source_file", F.lit(json_path.split("/")[-1]))
        .withColumn("load_timestamp", F.current_timestamp())
    )

    return flat_df
