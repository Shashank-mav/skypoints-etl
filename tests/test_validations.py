"""
Unit tests for SkyPoints data validations.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType

from src.validations import (
    check_mandatory_fields,
    check_key_uniqueness,
    check_date_validity,
    check_tier_code,
    run_all_validations,
)


@pytest.fixture(scope="session")
def spark():
    """Create a local SparkSession for testing."""
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("SkyPoints-Validation-Tests")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


def make_silver_df(spark, rows):
    """Helper to create a silver-like DataFrame for validation testing."""
    schema = StructType([
        StructField("member_id", StringType(), True),
        StructField("member_name", StringType(), True),
        StructField("enrollment_date", DateType(), True),
        StructField("last_flight_date", DateType(), True),
        StructField("date_of_birth", DateType(), True),
        StructField("tier_code", StringType(), True),
        StructField("country", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("source_file", StringType(), True),
        StructField("source_country", StringType(), True),
    ])
    return spark.createDataFrame(rows, schema)


# =============================================================================
# Mandatory Field Checks
# =============================================================================

class TestMandatoryFields:

    def test_all_present_passes(self, spark):
        from datetime import date
        df = make_silver_df(spark, [
            ("1", "Sam", date(2022, 1, 1), date(2022, 6, 1), None, "PLT", "USA", None, "USA.csv", "USA")
        ])
        result = check_mandatory_fields(df)
        row = result.first()
        assert row.mandatory_check is None

    def test_missing_member_id(self, spark):
        from datetime import date
        df = make_silver_df(spark, [
            (None, "Sam", date(2022, 1, 1), None, None, "PLT", "USA", None, "USA.csv", "USA")
        ])
        result = check_mandatory_fields(df)
        row = result.first()
        assert "Missing member_id" in row.mandatory_check

    def test_missing_multiple_fields(self, spark):
        df = make_silver_df(spark, [
            (None, None, None, None, None, "PLT", "USA", None, "USA.csv", "USA")
        ])
        result = check_mandatory_fields(df)
        row = result.first()
        assert "Missing member_id" in row.mandatory_check
        assert "Missing member_name" in row.mandatory_check
        assert "Missing enrollment_date" in row.mandatory_check


# =============================================================================
# Key Uniqueness
# =============================================================================

class TestKeyUniqueness:

    def test_unique_keys_pass(self, spark):
        from datetime import date
        df = make_silver_df(spark, [
            ("1", "Sam", date(2022, 1, 1), None, None, "PLT", "USA", None, "USA.csv", "USA"),
            ("2", "John", date(2022, 1, 1), None, None, "SLV", "USA", None, "USA.csv", "USA"),
        ])
        result = check_key_uniqueness(df)
        assert result.filter(result.duplicate_check.isNotNull()).count() == 0

    def test_duplicate_keys_flagged(self, spark):
        from datetime import date
        df = make_silver_df(spark, [
            ("1", "Sam", date(2022, 1, 1), None, None, "PLT", "USA", None, "USA.csv", "USA"),
            ("1", "Samuel", date(2022, 6, 1), None, None, "PLT", "USA", None, "USA.csv", "USA"),
        ])
        result = check_key_uniqueness(df)
        dups = result.filter(result.duplicate_check.isNotNull())
        assert dups.count() == 2  # both records flagged


# =============================================================================
# Date Validity
# =============================================================================

class TestDateValidity:

    def test_valid_dates_pass(self, spark):
        from datetime import date
        df = make_silver_df(spark, [
            ("1", "Sam", date(2022, 1, 1), date(2022, 6, 1), date(1990, 5, 3), "PLT", "USA", 32, "USA.csv", "USA")
        ])
        result = check_date_validity(df)
        row = result.first()
        assert row.date_check is None

    def test_flight_before_enrollment(self, spark):
        from datetime import date
        df = make_silver_df(spark, [
            ("1", "Sam", date(2022, 6, 1), date(2022, 1, 1), None, "PLT", "USA", None, "USA.csv", "USA")
        ])
        result = check_date_validity(df)
        row = result.first()
        assert "Flight date is before enrollment date" in row.date_check

    def test_unreasonable_age(self, spark):
        from datetime import date
        df = make_silver_df(spark, [
            ("1", "Sam", date(2022, 1, 1), None, date(1890, 1, 1), "PLT", "USA", 136, "USA.csv", "USA")
        ])
        result = check_date_validity(df)
        row = result.first()
        assert "age > 120" in row.date_check


# =============================================================================
# Tier Code Validation
# =============================================================================

class TestTierCode:

    def test_valid_tiers(self, spark):
        from datetime import date
        for tier in ["GLD", "SLV", "PLT"]:
            df = make_silver_df(spark, [
                ("1", "Sam", date(2022, 1, 1), None, None, tier, "USA", None, "USA.csv", "USA")
            ])
            result = check_tier_code(df)
            assert result.first().tier_check is None

    def test_invalid_tier(self, spark):
        from datetime import date
        df = make_silver_df(spark, [
            ("1", "Sam", date(2022, 1, 1), None, None, "DIAMOND", "USA", None, "USA.csv", "USA")
        ])
        result = check_tier_code(df)
        row = result.first()
        assert "Invalid tier code" in row.tier_check


# =============================================================================
# Combined Validation Pipeline
# =============================================================================

class TestCombinedValidation:

    def test_valid_records_pass_through(self, spark):
        from datetime import date
        df = make_silver_df(spark, [
            ("1", "Sam", date(2022, 1, 1), date(2022, 6, 1), None, "PLT", "USA", None, "USA.csv", "USA"),
            ("2", "John", date(2022, 1, 1), date(2022, 3, 1), None, "SLV", "USA", None, "USA.csv", "USA"),
        ])
        valid, quarantine = run_all_validations(df)
        assert valid.count() == 2
        assert quarantine.count() == 0

    def test_invalid_records_quarantined(self, spark):
        from datetime import date
        df = make_silver_df(spark, [
            ("1", "Sam", date(2022, 1, 1), None, None, "PLT", "USA", None, "USA.csv", "USA"),   # valid
            (None, "Bad", date(2022, 1, 1), None, None, "PLT", "USA", None, "USA.csv", "USA"),   # missing ID
        ])
        valid, quarantine = run_all_validations(df)
        assert valid.count() == 1
        assert quarantine.count() == 1
