"""
Utility functions for SkyPoints ETL.

Handles the tricky date parsing across different country formats
and common helper operations.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType
from datetime import datetime


def parse_usa_date(col_name: str) -> F.Column:
    """
    Parse USA date format: MMDDYYYY with no separators and no zero-padding.

    Examples: '6152022' → 2022-06-15, '12282021' → 2021-12-28, '1052022' → 2022-01-05

    Strategy: Right-pad to ensure year is last 4 chars, then parse remaining
    as month+day. Since month and day aren't zero-padded, we work from the right.
    """
    col = F.col(col_name)

    # Year is always the last 4 characters
    year = F.substring(col, -4, 4)

    # Everything before the last 4 chars is MMDD (variable length)
    mmdd = F.expr(f"substring({col_name}, 1, length({col_name}) - 4)")

    # Day is the last 2 chars of mmdd (or 1 if single digit — but we pad)
    # Actually, day could be 1 or 2 digits, month could be 1 or 2 digits
    # Total mmdd length: 2 (1+1) to 4 (2+2)
    # Strategy: left-pad mmdd to 4 chars, then split
    mmdd_padded = F.lpad(mmdd, 4, "0")
    month = F.substring(mmdd_padded, 1, 2)
    day = F.substring(mmdd_padded, 3, 2)

    date_str = F.concat_ws("-", year, month, day)
    return F.to_date(date_str, "yyyy-MM-dd")


def parse_date_column(col_name: str, date_format: str) -> F.Column:
    """
    Parse a date column using the given Spark date format string.

    For USA's special format, delegates to parse_usa_date.
    For standard formats, uses Spark's to_date.
    """
    if date_format == "Mddyyyy":
        return parse_usa_date(col_name)
    else:
        return F.to_date(F.col(col_name), date_format)


def compute_age(dob_col: str = "date_of_birth") -> F.Column:
    """
    Compute age in years from a DATE column.

    Uses floor of (days between DOB and today) / 365.25 for accuracy.
    Returns NULL if DOB is null.
    """
    return F.floor(
        F.datediff(F.current_date(), F.col(dob_col)) / 365.25
    ).cast("int")


def compute_stale_member(
    flight_date_col: str = "last_flight_date",
    threshold_days: int = 90,
) -> F.Column:
    """
    Flag members as stale if their last flight was more than `threshold_days` ago.

    Returns TRUE if stale, FALSE if active, NULL if flight_date is null.
    """
    days_since = F.datediff(F.current_date(), F.col(flight_date_col))
    return F.when(
        F.col(flight_date_col).isNull(), None
    ).otherwise(
        days_since > F.lit(threshold_days)
    )


def add_metadata_columns(
    df: DataFrame,
    source_file: str,
    source_country: str,
) -> DataFrame:
    """Add source lineage metadata columns to a DataFrame."""
    return (
        df.withColumn("source_file", F.lit(source_file))
          .withColumn("source_country", F.lit(source_country))
          .withColumn("load_timestamp", F.current_timestamp())
    )
