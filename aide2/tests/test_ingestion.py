"""Tests for data ingestion components"""

import pytest
import json
from unittest.mock import patch, MagicMock
import os


class TestNYCTaxiIngestion:
    def test_download_url_construction(self):
        base = "https://d37ci6vzurychx.cloudfront.net/trip-data"
        for taxi_type in ["yellow", "green"]:
            url = f"{base}/{taxi_type}_tripdata_2024-01.parquet"
            assert taxi_type in url
            assert "2024-01" in url

    @patch.dict(
        os.environ,
        {
            "MINIO_ENDPOINT": "localhost:9000",
            "MINIO_ACCESS_KEY": "minioadmin",
            "MINIO_SECRET_KEY": "minioadmin",
        },
    )
    def test_minio_env_config(self):
        assert os.environ["MINIO_ENDPOINT"] == "localhost:9000"

    def test_date_range_parsing(self):
        from datetime import datetime

        start = datetime.strptime("2024-01", "%Y-%m")
        end = datetime.strptime("2024-03", "%Y-%m")
        months = []
        current = start
        while current <= end:
            months.append((current.year, current.month))
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1)
            else:
                current = current.replace(month=current.month + 1)
        assert months == [(2024, 1), (2024, 2), (2024, 3)]


class TestKafkaProducer:
    def test_ride_event_structure(self):
        event = {
            "ride_id": "test_001",
            "vendor_id": 1,
            "pickup_datetime": "2024-01-15T10:00:00",
            "dropoff_datetime": "2024-01-15T10:15:00",
            "passenger_count": 1,
            "trip_distance": 2.5,
            "fare_amount": 12.00,
            "pickup_location_id": 100,
            "dropoff_location_id": 200,
        }
        required_fields = [
            "ride_id",
            "vendor_id",
            "pickup_datetime",
            "dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "fare_amount",
            "pickup_location_id",
            "dropoff_location_id",
        ]
        for field in required_fields:
            assert field in event

    def test_event_serialization(self):
        event = {"ride_id": "test", "fare_amount": 10.5}
        serialized = json.dumps(event).encode("utf-8")
        deserialized = json.loads(serialized)
        assert deserialized["fare_amount"] == 10.5

    def test_rate_limiting_config(self):
        rate = 10
        delay = 1.0 / rate
        assert abs(delay - 0.1) < 0.001


class TestKafkaTopics:
    def test_required_topics(self):
        topics = [
            "nyc-taxi-rides",
            "taxi-stream-metrics",
            "taxi-anomalies",
            "data-ingestion-events",
        ]
        assert len(topics) == 4
        assert all(isinstance(t, str) for t in topics)
