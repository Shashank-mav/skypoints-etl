"""
Utility functions for SkyPoints ETL.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def parse_usa_date(col_name):
    """parse USA dates like 6152022 or 12282021 (MMDDYYYY, no separators, no zero padding)"""

    col = F.col(col_name)

    # year is always last 4 chars
    year = F.substring(col, -4, 4)

    # everything before that is month+day (variable length)
    mmdd = F.expr(f"substring({col_name}, 1, length({col_name}) - 4)")

    # pad to 4 chars then split
    mmdd_padded = F.lpad(mmdd, 4, "0")
    month = F.substring(mmdd_padded, 1, 2)
    day = F.substring(mmdd_padded, 3, 2)

    date_str = F.concat_ws("-", year, month, day)
    return F.try_to_date(date_str, "yyyy-MM-dd")


def parse_date_column(col_name, date_format):
    """parse a date column, delegates to parse_usa_date for the weird USA format"""

    if date_format == "Mddyyyy":
        return parse_usa_date(col_name)
    else:
        return F.try_to_date(F.col(col_name), date_format)


def compute_age(dob_col="date_of_birth"):
    """age in years from DOB, null if DOB is null"""

    return F.floor(
        F.datediff(F.current_date(), F.col(dob_col)) / 365.25
    ).cast("int")


def compute_stale_member(flight_date_col="last_flight_date", threshold_days=90):
    """true if last flight was more than threshold_days ago"""

    days_since = F.datediff(F.current_date(), F.col(flight_date_col))
    return F.when(
        F.col(flight_date_col).isNull(), None
    ).otherwise(
        days_since > F.lit(threshold_days)
    )


def add_metadata_columns(df, source_file, source_country):
    return (
        df.withColumn("source_file", F.lit(source_file))
          .withColumn("source_country", F.lit(source_country))
          .withColumn("load_timestamp", F.current_timestamp())
    )