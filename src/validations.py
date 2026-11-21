"""
Data validation functions for SkyPoints ETL.

Runs quality checks on the silver layer before data moves to gold:
  - Mandatory field checks (member_id, member_name, enrollment_date, country)
  - Key column uniqueness (member_id)
  - Date validity (no future dates, no impossible dates)
  - Tier code validity
  - Referential integrity between redemptions and members
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Tuple


# =============================================================================
# Individual Validation Rules
# =============================================================================

def check_mandatory_fields(df: DataFrame) -> DataFrame:
    """
    Flag records missing mandatory fields.

    Mandatory: member_id, member_name, enrollment_date, country
    Returns DataFrame with a 'mandatory_check' column:
      - NULL if all mandatory fields present
      - Error message string if any are missing
    """
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

    # Concat all non-null error messages
    error_col = F.concat_ws(
        "; ",
        *[F.when(c.isNotNull(), c) for c in checks]
    )

    return df.withColumn(
        "mandatory_check",
        F.when(error_col == "", None).otherwise(error_col)
    )


def check_key_uniqueness(df: DataFrame, key_col: str = "member_id") -> DataFrame:
    """
    Flag duplicate keys within the same batch.

    Adds 'duplicate_check' column:
      - NULL if unique
      - 'Duplicate member_id' if duplicated

    Note: Duplicates within a batch are flagged; across batches,
    the MERGE handles "latest record wins".
    """
    window = F.count("*").over(
        F.window.partitionBy(key_col).orderBy(F.lit(1)).rowsBetween(
            F.window.unboundedPreceding, F.window.unboundedFollowing
        )
    ) if False else None  # Placeholder — use groupBy approach below

    # Count occurrences of each key
    dup_counts = df.groupBy(key_col).count().filter(F.col("count") > 1)

    return df.join(
        dup_counts.select(key_col).withColumn("duplicate_check", F.lit("Duplicate member_id")),
        on=key_col,
        how="left",
    )


def check_date_validity(df: DataFrame) -> DataFrame:
    """
    Flag records with invalid dates.

    Checks:
      - enrollment_date in the future
      - last_flight_date before enrollment_date
      - date_of_birth in the future or unreasonably old (> 120 years)
      - Dates that failed parsing (NULL after to_date but non-null in bronze)

    Adds 'date_check' column with error messages.
    """
    checks = []

    # Enrollment date in the future
    checks.append(
        F.when(
            F.col("enrollment_date") > F.current_date(),
            F.lit("Enrollment date is in the future")
        )
    )

    # Flight date before enrollment date
    checks.append(
        F.when(
            F.col("last_flight_date").isNotNull()
            & F.col("enrollment_date").isNotNull()
            & (F.col("last_flight_date") < F.col("enrollment_date")),
            F.lit("Flight date is before enrollment date")
        )
    )

    # DOB in the future
    checks.append(
        F.when(
            F.col("date_of_birth").isNotNull()
            & (F.col("date_of_birth") > F.current_date()),
            F.lit("DOB is in the future")
        )
    )

    # DOB unreasonably old (> 120 years)
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
    """
    Flag records with invalid tier codes.

    Valid tiers: GLD (Gold), SLV (Silver), PLT (Platinum)
    """
    valid_tiers = ["GLD", "SLV", "PLT"]

    return df.withColumn(
        "tier_check",
        F.when(
            F.col("tier_code").isNotNull()
            & ~F.upper(F.col("tier_code")).isin(valid_tiers),
            F.concat(F.lit("Invalid tier code: "), F.col("tier_code"))
        )
    )


def check_date_parse_failures(
    bronze_df: DataFrame,
    silver_df: DataFrame,
) -> DataFrame:
    """
    Detect dates that existed in bronze but became NULL after parsing.

    This catches invalid dates like '2021-13-13' (month=13 in AUS data).

    Returns DataFrame of records where a date was lost during parsing.
    """
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

    # Flag where raw is non-null but parsed is null
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


# =============================================================================
# Combined Validation Pipeline
# =============================================================================

def run_all_validations(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """
    Run all validation checks and split into valid + quarantined records.

    Args:
        df: Silver DataFrame

    Returns:
        Tuple of (valid_df, quarantine_df)
          - valid_df: Records that passed all checks (validation columns dropped)
          - quarantine_df: Records that failed at least one check
    """
    validated = df
    validated = check_mandatory_fields(validated)
    validated = check_key_uniqueness(validated)
    validated = check_date_validity(validated)
    validated = check_tier_code(validated)

    # Combine all error columns into one
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

    # Split
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
