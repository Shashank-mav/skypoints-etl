"""
Schema definitions and column mappings for SkyPoints ETL.

Each country file has a different schema. This module defines:
  - The unified bronze (raw) schema all files get mapped into
  - Per-country column name mappings
  - Per-country date format strings
"""

from pyspark.sql.types import StructType, StructField, StringType, TimestampType


# =============================================================================
# Bronze schema — all STRING except metadata columns
# =============================================================================
BRONZE_SCHEMA = StructType([
    StructField("member_id", StringType(), True),
    StructField("member_name", StringType(), True),
    StructField("date_of_birth", StringType(), True),
    StructField("tier_code", StringType(), True),
    StructField("enrollment_date", StringType(), True),
    StructField("last_flight_date", StringType(), True),
    StructField("agent_name", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("post_code", StringType(), True),
    StructField("is_active", StringType(), True),
    StructField("individual_or_corporate", StringType(), True),
    StructField("source_file", StringType(), False),
    StructField("source_country", StringType(), False),
    StructField("load_timestamp", TimestampType(), False),
])


# =============================================================================
# Column mappings: source column name → unified bronze column name
# =============================================================================
# Keys = exact column names in the CSV header
# Values = target column name in the bronze table

COLUMN_MAPPINGS = {
    "USA": {
        "ID": "member_id",
        "Name": "member_name",
        "TierCode": "tier_code",
        "EnrollmentDate": "enrollment_date",
        "FlightDate": "last_flight_date",
    },
    "IND": {
        "ID": "member_id",
        "Name": "member_name",
        "DOB": "date_of_birth",
        "TierCode": "tier_code",
        "EnrollmentDate": "enrollment_date",
        "Individual or Corporate": "individual_or_corporate",
        "Flight Date": "last_flight_date",
    },
    "AUS": {
        "Unique ID": "member_id",
        "Member Name": "member_name",
        "Tier Type": "tier_code",
        "Date of Birth": "date_of_birth",
        "Date of Enrollment": "enrollment_date",
        "Date of Flight": "last_flight_date",
    },
}


# =============================================================================
# Date formats per country (used during Silver transformation)
# =============================================================================
# These are Python strftime / Spark to_date format strings.

DATE_FORMATS = {
    "USA": {
        "enrollment_date": "Mddyyyy",   # e.g. 6152022 → ambiguous, handled in utils
        "last_flight_date": "Mddyyyy",
    },
    "IND": {
        "enrollment_date": "M/d/yyyy",  # e.g. 1/1/2022
        "last_flight_date": "M/d/yyyy",
        "date_of_birth": "M/d/yyyy",
    },
    "AUS": {
        "enrollment_date": "yyyy-MM-dd",  # e.g. 2022-05-11
        "last_flight_date": "yyyy-MM-dd",
        "date_of_birth": "yyyy-MM-dd",
    },
}

# Country code to full name mapping (for display / table naming)
COUNTRY_NAMES = {
    "USA": "usa",
    "IND": "ind",
    "AUS": "aus",
    "CAN": "can",
    "PHIL": "phil",
}

# All bronze columns (for ensuring consistent column order)
BRONZE_COLUMNS = [
    "member_id", "member_name", "date_of_birth", "tier_code",
    "enrollment_date", "last_flight_date", "agent_name", "state",
    "country", "post_code", "is_active", "individual_or_corporate",
    "source_file", "source_country", "load_timestamp",
]
