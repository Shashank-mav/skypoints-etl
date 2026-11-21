"""
Transformation functions for SkyPoints ETL.
Bronze ingestion, silver staging, gold country split, JSON parsing.
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


def ingest_csv_to_bronze(spark, file_path, country_code):
    """read a country csv and map it to the unified bronze schema"""

    if country_code not in COLUMN_MAPPINGS:
        raise ValueError(f"Unknown country code '{country_code}'")

    mapping = COLUMN_MAPPINGS[country_code]

    df = spark.read.option("header", "true").csv(file_path)

    for src_col, tgt_col in mapping.items():
        if src_col in df.columns:
            df = df.withColumnRenamed(src_col, tgt_col)

    if "country" not in df.columns:
        df = df.withColumn("country", F.lit(country_code))

    # fill missing columns with null
    for col_name in BRONZE_COLUMNS:
        if col_name in ("source_file", "source_country", "load_timestamp"):
            continue
        if col_name not in df.columns:
            df = df.withColumn(col_name, F.lit(None).cast(StringType()))

    file_name = file_path.split("/")[-1]
    df = add_metadata_columns(df, file_name, country_code)

    return df.select(BRONZE_COLUMNS)


def ingest_all_countries(spark, file_paths):
    """ingest multiple country files and union into one bronze df"""

    dfs = []
    for country_code, path in file_paths.items():
        df = ingest_csv_to_bronze(spark, path, country_code)
        dfs.append(df)

    result = dfs[0]
    for df in dfs[1:]:
        result = result.unionByName(df)

    return result


def transform_bronze_to_silver(df):
    """bronze -> silver: parse dates, clean nulls, compute age + stale_member"""

    # clean up string 'NULL' values (some files use literal NULL)
    for col_name in df.columns:
        df = df.withColumn(
            col_name,
            F.when(
                F.upper(F.trim(F.col(col_name))) == "NULL", None
            ).otherwise(F.col(col_name))
        )

    # parse dates per country since formats are different
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

        # fallback to ISO for any date cols not in the country config
        for date_col in ["enrollment_date", "last_flight_date", "date_of_birth"]:
            if date_col not in formats and date_col in country_df.columns:
                country_df = country_df.withColumn(
                    date_col,
                    F.to_date(F.col(date_col), "yyyy-MM-dd")
                )

        result_dfs.append(country_df)

    silver_df = result_dfs[0]
    for rdf in result_dfs[1:]:
        silver_df = silver_df.unionByName(rdf)

    # derived columns
    silver_df = silver_df.withColumn("age", compute_age("date_of_birth"))
    silver_df = silver_df.withColumn("stale_member", compute_stale_member("last_flight_date"))
    silver_df = silver_df.withColumn("processed_timestamp", F.current_timestamp())

    return silver_df


def get_country_members(df, country_code):
    return df.filter(F.col("country") == country_code)


def merge_to_country_table(spark, source_df, target_table, country_code):
    """merge members into a country gold table, latest record wins"""

    country_df = get_country_members(source_df, country_code)

    if country_df.count() == 0:
        return

    country_df = country_df.withColumn("updated_timestamp", F.current_timestamp())

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

    spark.sql(f"""
        MERGE INTO {target_table} AS target
        USING source_members AS source
        ON target.member_id = source.member_id
        WHEN MATCHED AND source.load_timestamp > target.load_timestamp THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
    """)


def remove_moved_members(spark, silver_df, country_code, target_table):
    """delete members from this country table if they moved to another country"""

    latest_country = (
        silver_df
        .orderBy(F.col("load_timestamp").desc())
        .dropDuplicates(["member_id"])
        .select("member_id", "country")
    )

    latest_country.createOrReplaceTempView("latest_country")

    spark.sql(f"""
        DELETE FROM {target_table}
        WHERE member_id IN (
            SELECT member_id
            FROM latest_country
            WHERE country != '{country_code}'
        )
    """)


def parse_redemption_json(spark, json_path):
    """parse the nested json redemption feed into a flat table"""

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

    raw_df = spark.read.schema(json_schema).json(json_path)

    # explode so each transaction gets its own row
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