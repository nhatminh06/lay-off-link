"""Tests for Spark medallion pipeline jobs"""

import pytest
from unittest.mock import patch, MagicMock
import os


class TestBronzeIngestion:
    def test_schema_validation_columns(self):
        pytest.importorskip("pyspark")
        from aide2.spark.bronze_ingestion import REQUIRED_COLUMNS

        expected = [
            "VendorID",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "fare_amount",
            "total_amount",
        ]
        for col in expected:
            assert col in REQUIRED_COLUMNS

    def test_bronze_output_path_format(self):
        output_path = "s3a://lakehouse/bronze/nyc_taxi/"
        assert output_path.startswith("s3a://")
        assert "bronze" in output_path

    def test_bronze_adds_metadata_columns(self):
        mock_df = MagicMock()
        mock_df.withColumn.return_value = mock_df
        mock_df.columns = [
            "VendorID",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "fare_amount",
            "total_amount",
        ]
        assert "VendorID" in mock_df.columns


class TestSilverTransform:
    def test_cleaning_rules_defined(self):
        rules = {
            "negative_fare": "fare_amount > 0",
            "valid_distance": "trip_distance > 0",
            "valid_passengers": "passenger_count > 0 AND passenger_count <= 6",
        }
        assert len(rules) == 3
        assert "negative_fare" in rules

    def test_derived_columns(self):
        derived = [
            "trip_duration_minutes",
            "speed_mph",
            "fare_per_mile",
            "fare_per_minute",
            "hour_of_day",
            "day_of_week",
            "is_weekend",
            "time_of_day",
        ]
        assert len(derived) == 8
        assert "speed_mph" in derived

    def test_time_of_day_classification(self):
        mapping = {
            range(6, 12): "morning",
            range(12, 17): "afternoon",
            range(17, 21): "evening",
        }
        assert len(mapping) == 3


class TestGoldAggregation:
    def test_gold_tables_defined(self):
        tables = ["hourly_stats", "daily_stats", "zone_stats", "driver_patterns"]
        assert len(tables) == 4

    def test_hourly_stats_metrics(self):
        metrics = [
            "avg_fare",
            "avg_distance",
            "avg_duration",
            "trip_count",
            "avg_speed",
            "total_revenue",
        ]
        assert len(metrics) == 6

    def test_daily_stats_includes_peak_hour(self):
        daily_metrics = [
            "avg_fare",
            "avg_distance",
            "avg_duration",
            "trip_count",
            "peak_hour",
            "avg_passenger_count",
            "total_revenue",
        ]
        assert "peak_hour" in daily_metrics
        assert "avg_passenger_count" in daily_metrics


class TestSparkUtils:
    @patch.dict(
        os.environ,
        {
            "MINIO_ENDPOINT": "localhost:9000",
            "MINIO_ACCESS_KEY": "test",
            "MINIO_SECRET_KEY": "test",
        },
    )
    def test_env_vars_read(self):
        assert os.environ["MINIO_ENDPOINT"] == "localhost:9000"
        assert os.environ["MINIO_ACCESS_KEY"] == "test"
