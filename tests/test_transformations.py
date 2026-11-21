"""
Unit tests for SkyPoints ETL transformations.

These tests use PySpark's local mode — they can run in a Databricks notebook
or locally with `pytest` and a local Spark installation.
"""

import pytest
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.utils import parse_usa_date, compute_age, compute_stale_member
from src.transformations import ingest_csv_to_bronze, transform_bronze_to_silver
from src.schemas import BRONZE_COLUMNS


@pytest.fixture(scope="session")
def spark():
    """Create a local SparkSession for testing."""
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("SkyPoints-Tests")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


# =============================================================================
# Utils Tests
# =============================================================================

class TestUSADateParsing:
    """Test the tricky USA date format: MMDDYYYY without separators."""

    def test_standard_date(self, spark):
        """12282021 → 2021-12-28"""
        df = spark.createDataFrame([("12282021",)], ["d"])
        result = df.select(parse_usa_date("d").alias("parsed")).first()
        assert result.parsed == date(2021, 12, 28)

    def test_single_digit_month(self, spark):
        """6152022 → 2022-06-15"""
        df = spark.createDataFrame([("6152022",)], ["d"])
        result = df.select(parse_usa_date("d").alias("parsed")).first()
        assert result.parsed == date(2022, 6, 15)

    def test_single_digit_month_and_day(self, spark):
        """1052022 → 2022-01-05"""
        df = spark.createDataFrame([("1052022",)], ["d"])
        result = df.select(parse_usa_date("d").alias("parsed")).first()
        assert result.parsed == date(2022, 1, 5)


class TestComputeAge:
    """Test age computation from DOB."""

    def test_known_age(self, spark):
        """A DOB 30 years ago should return ~30."""
        df = spark.createDataFrame([("1994-01-01",)], ["date_of_birth"])
        df = df.withColumn("date_of_birth", F.to_date("date_of_birth"))
        result = df.select(compute_age().alias("age")).first()
        assert result.age >= 30  # will be 32-33 depending on when test runs

    def test_null_dob(self, spark):
        """NULL DOB should return NULL age."""
        df = spark.createDataFrame([(None,)], ["date_of_birth"])
        df = df.withColumn("date_of_birth", F.to_date("date_of_birth"))
        result = df.select(compute_age().alias("age")).first()
        assert result.age is None


class TestStaleMember:
    """Test stale member flag computation."""

    def test_stale_member(self, spark):
        """A flight date > 90 days ago should be stale."""
        df = spark.createDataFrame([("2020-01-01",)], ["last_flight_date"])
        df = df.withColumn("last_flight_date", F.to_date("last_flight_date"))
        result = df.select(compute_stale_member().alias("stale")).first()
        assert result.stale is True

    def test_active_member(self, spark):
        """A recent flight date should not be stale."""
        from datetime import datetime, timedelta
        recent = (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d")
        df = spark.createDataFrame([(recent,)], ["last_flight_date"])
        df = df.withColumn("last_flight_date", F.to_date("last_flight_date"))
        result = df.select(compute_stale_member().alias("stale")).first()
        assert result.stale is False

    def test_null_flight_date(self, spark):
        """NULL flight date should return NULL stale flag."""
        df = spark.createDataFrame([(None,)], ["last_flight_date"])
        df = df.withColumn("last_flight_date", F.to_date("last_flight_date"))
        result = df.select(compute_stale_member().alias("stale")).first()
        assert result.stale is None


# =============================================================================
# Bronze Ingestion Tests
# =============================================================================

class TestBronzeIngestion:
    """Test CSV to bronze mapping for each country."""

    def test_usa_column_mapping(self, spark, tmp_path):
        """USA CSV columns should map to bronze schema."""
        csv_content = "ID,Name,TierCode,EnrollmentDate,FlightDate\n1,Sam,PLT,6152022,8202022"
        csv_path = str(tmp_path / "USA.csv")
        with open(csv_path, "w") as f:
            f.write(csv_content)

        df = ingest_csv_to_bronze(spark, csv_path, "USA")

        assert set(BRONZE_COLUMNS).issubset(set(df.columns))
        row = df.first()
        assert row.member_id == "1"
        assert row.member_name == "Sam"
        assert row.tier_code == "PLT"
        assert row.country == "USA"
        assert row.source_country == "USA"

    def test_ind_column_mapping(self, spark, tmp_path):
        """IND CSV should map columns including DOB and Individual/Corporate."""
        csv_content = "ID,Name,DOB,TierCode,EnrollmentDate,Individual or Corporate,Flight Date\n1,Vikas,12/1/1998,SLV,1/1/2022,I,6/15/2022"
        csv_path = str(tmp_path / "IND.csv")
        with open(csv_path, "w") as f:
            f.write(csv_content)

        df = ingest_csv_to_bronze(spark, csv_path, "IND")

        row = df.first()
        assert row.member_name == "Vikas"
        assert row.date_of_birth == "12/1/1998"
        assert row.individual_or_corporate == "I"
        assert row.country == "IND"

    def test_aus_column_mapping(self, spark, tmp_path):
        """AUS CSV should map its unique column names."""
        csv_content = "Unique ID,Member Name,Tier Type,Date of Birth,Date of Enrollment,Date of Flight\n1,Mike,PLT,NULL,2022-05-11,2022-08-01"
        csv_path = str(tmp_path / "AUS.csv")
        with open(csv_path, "w") as f:
            f.write(csv_content)

        df = ingest_csv_to_bronze(spark, csv_path, "AUS")

        row = df.first()
        assert row.member_name == "Mike"
        assert row.date_of_birth == "NULL"
        assert row.country == "AUS"

    def test_unknown_country_raises(self, spark, tmp_path):
        """Unknown country code should raise ValueError."""
        csv_path = str(tmp_path / "XYZ.csv")
        with open(csv_path, "w") as f:
            f.write("ID,Name\n1,Test")

        with pytest.raises(ValueError, match="Unknown country code"):
            ingest_csv_to_bronze(spark, csv_path, "XYZ")

    def test_missing_columns_filled_null(self, spark, tmp_path):
        """Columns not in the source file should be NULL in bronze."""
        csv_content = "ID,Name,TierCode,EnrollmentDate,FlightDate\n1,Sam,PLT,6152022,8202022"
        csv_path = str(tmp_path / "USA.csv")
        with open(csv_path, "w") as f:
            f.write(csv_content)

        df = ingest_csv_to_bronze(spark, csv_path, "USA")

        row = df.first()
        assert row.date_of_birth is None
        assert row.agent_name is None
        assert row.post_code is None
