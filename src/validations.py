"""
Data validation functions for SkyPoints ETL.
Checks data quality before silver -> gold promotion.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Tuple


def check_mandatory_fields(df: DataFrame) -> DataFrame:
    """Flag records missing mandatory fields"""
    checks = []

    mandatory_fields = {
        "member_id": "Missing member_id",
        "member_name": "Missing member_name",
        "enrollment_date": "Missing enrollment_date",
        "country": "Missing country",
    }

    for field, msg in mandatory_fields.items():
        checks.append(
            F.when(
                F.col(field).isNull() | (F.trim(F.col(field).cast("string")) == ""),
                F.lit(msg)
            )
        )

    error_col = F.concat_ws(
        "; ",
        *[F.when(c.isNotNull(), c) for c in checks]
    )

    return df.withColumn(
        "mandatory_check",
        F.when(error_col == "", None).otherwise(error_col)
    )


def check_key_uniqueness(df: DataFrame, key_col: str = "member_id") -> DataFrame:
    """Flag duplicate member_ids within same country"""
    dup_counts = (
        df.groupBy(key_col, "source_country")
        .count()
        .filter(F.col("count") > 1)
    )

    return df.join(
        dup_counts.select(key_col, "source_country")
        .withColumn("duplicate_check", F.lit("Duplicate member_id")),
        on=[key_col, "source_country"],
        how="left",
    )


def check_date_validity(df: DataFrame) -> DataFrame:
    """Check for future dates, flight before enrollment, unreasonable DOB etc"""
    checks = []

    checks.append(
        F.when(
            F.col("enrollment_date") > F.current_date(),
            F.lit("Enrollment date is in the future")
        )
    )

    checks.append(
        F.when(
            F.col("last_flight_date").isNotNull()
            & F.col("enrollment_date").isNotNull()
            & (F.col("last_flight_date") < F.col("enrollment_date")),
            F.lit("Flight date is before enrollment date")
        )
    )

    checks.append(
        F.when(
            F.col("date_of_birth").isNotNull()
            & (F.col("date_of_birth") > F.current_date()),
            F.lit("DOB is in the future")
        )
    )

    checks.append(
        F.when(
            F.col("age").isNotNull() & (F.col("age") > 120),
            F.lit("DOB indicates age > 120 years")
        )
    )

    error_col = F.concat_ws(
        "; ",
        *[F.when(c.isNotNull(), c) for c in checks]
    )

    return df.withColumn(
        "date_check",
        F.when(error_col == "", None).otherwise(error_col)
    )


def check_tier_code(df: DataFrame) -> DataFrame:
    """only GLD, SLV, PLT are valid"""
    valid_tiers = ["GLD", "SLV", "PLT"]

    return df.withColumn(
        "tier_check",
        F.when(
            F.col("tier_code").isNotNull()
            & ~F.upper(F.col("tier_code")).isin(valid_tiers),
            F.concat(F.lit("Invalid tier code: "), F.col("tier_code"))
        )
    )


def check_date_parse_failures(bronze_df: DataFrame, silver_df: DataFrame) -> DataFrame:
    """find dates that existed in bronze but became NULL after parsing
    eg: '2021-13-13' (month 13) in AUS data"""

    date_cols = ["enrollment_date", "last_flight_date", "date_of_birth"]

    bronze_dates = bronze_df.select(
        "member_id", "source_country",
        *[F.col(c).alias(f"raw_{c}") for c in date_cols]
    )

    silver_dates = silver_df.select(
        "member_id", "source_country",
        *[F.col(c).alias(f"parsed_{c}") for c in date_cols]
    )

    joined = bronze_dates.join(
        silver_dates,
        on=["member_id", "source_country"],
        how="inner",
    )

    checks = []
    for c in date_cols:
        checks.append(
            F.when(
                F.col(f"raw_{c}").isNotNull()
                & (F.trim(F.col(f"raw_{c}")) != "")
                & (F.upper(F.trim(F.col(f"raw_{c}"))) != "NULL")
                & F.col(f"parsed_{c}").isNull(),
                F.lit(f"Failed to parse {c}")
            )
        )

    error_col = F.concat_ws("; ", *[F.when(c.isNotNull(), c) for c in checks])

    return joined.withColumn(
        "parse_failure",
        F.when(error_col == "", None).otherwise(error_col)
    ).filter(F.col("parse_failure").isNotNull())


def run_all_validations(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """runs all checks, splits into valid records and quarantined ones"""

    validated = df
    validated = check_mandatory_fields(validated)
    validated = check_key_uniqueness(validated)
    validated = check_date_validity(validated)
    validated = check_tier_code(validated)

    error_columns = ["mandatory_check", "duplicate_check", "date_check", "tier_check"]

    validated = validated.withColumn(
        "validation_errors",
        F.concat_ws(
            "; ",
            *[F.when(F.col(c).isNotNull(), F.col(c)) for c in error_columns]
        )
    )

    validated = validated.withColumn(
        "validation_errors",
        F.when(F.col("validation_errors") == "", None).otherwise(F.col("validation_errors"))
    )

    valid_df = (
        validated
        .filter(F.col("validation_errors").isNull())
        .drop("validation_errors", *error_columns)
    )

    quarantine_df = (
        validated
        .filter(F.col("validation_errors").isNotNull())
        .select(
            "member_id",
            "member_name",
            "validation_errors",
            "source_file",
            "source_country",
        )
        .withColumn("quarantined_at", F.current_timestamp())
    )

    return valid_df, quarantine_df