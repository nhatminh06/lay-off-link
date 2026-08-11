"""Tests for data ingestion components"""

import pytest
import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch, MagicMock
import os

from aide2.ingestion.kafka_producer import (
    _first_present_column,
    _to_iso,
    row_from_parquet_iter,
    run,
    synthetic_row_iter,
)


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


class TestSyntheticRowIter:
    def test_yields_well_formed_ride_dicts(self):
        row = next(synthetic_row_iter(seed=42))
        required = {
            "ride_id",
            "vendor_id",
            "pickup_datetime",
            "dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "fare_amount",
            "pickup_location_id",
            "dropoff_location_id",
        }
        assert required <= set(row.keys())
        assert row["vendor_id"] in (1, 2)
        assert 1 <= row["passenger_count"] <= 6
        assert 0.3 <= row["trip_distance"] <= 25.0

    def test_seed_is_reproducible(self):
        it1 = synthetic_row_iter(seed=7)
        it2 = synthetic_row_iter(seed=7)
        rows1 = [next(it1) for _ in range(3)]
        rows2 = [next(it2) for _ in range(3)]
        # ride_id is random uuid4 (not seeded), so compare everything else
        for r1, r2 in zip(rows1, rows2):
            r1 = {k: v for k, v in r1.items() if k != "ride_id"}
            r2 = {k: v for k, v in r2.items() if k != "ride_id"}
            assert r1 == r2

    def test_different_seeds_diverge(self):
        row_a = next(synthetic_row_iter(seed=1))
        row_b = next(synthetic_row_iter(seed=2))
        assert (row_a["fare_amount"], row_a["trip_distance"]) != (
            row_b["fare_amount"],
            row_b["trip_distance"],
        )

    def test_dropoff_after_pickup(self):
        row = next(synthetic_row_iter(seed=5))
        pickup = datetime.fromisoformat(row["pickup_datetime"])
        dropoff = datetime.fromisoformat(row["dropoff_datetime"])
        assert dropoff > pickup


class TestToIso:
    def test_naive_datetime_gets_utc_and_isoformat(self):
        out = _to_iso(datetime(2024, 1, 15, 8, 30, 0))
        assert out == "2024-01-15T08:30:00+00:00"

    def test_aware_datetime_preserves_tzinfo(self):
        out = _to_iso(datetime(2024, 1, 15, 8, 30, 0, tzinfo=timezone.utc))
        assert out == "2024-01-15T08:30:00+00:00"

    def test_pandas_timestamp_converted(self):
        pd = pytest.importorskip("pandas")
        out = _to_iso(pd.Timestamp("2024-01-15 08:30:00"))
        assert out == "2024-01-15T08:30:00+00:00"

    def test_non_datetime_falls_back_to_str(self):
        assert _to_iso("already-a-string") == "already-a-string"
        assert _to_iso(42) == "42"


class TestFirstPresentColumn:
    def test_returns_first_matching_candidate(self):
        pd = pytest.importorskip("pandas")
        df = pd.DataFrame({"lpep_pickup_datetime": [1], "other": [2]})
        assert (
            _first_present_column(
                df, ["tpep_pickup_datetime", "lpep_pickup_datetime", "pickup_datetime"]
            )
            == "lpep_pickup_datetime"
        )

    def test_raises_keyerror_when_no_candidate_matches(self):
        pd = pytest.importorskip("pandas")
        df = pd.DataFrame({"other": [2]})
        with pytest.raises(KeyError):
            _first_present_column(df, ["a", "b"])


class TestRowFromParquetIter:
    def test_normalizes_yellow_schema_columns(self, tmp_path):
        pd = pytest.importorskip("pandas")
        pytest.importorskip("pyarrow")
        df = pd.DataFrame(
            {
                "VendorID": [1],
                "tpep_pickup_datetime": [datetime(2024, 1, 15, 8, 0, 0)],
                "tpep_dropoff_datetime": [datetime(2024, 1, 15, 8, 15, 0)],
                "passenger_count": [2],
                "trip_distance": [3.1],
                "fare_amount": [14.5],
                "PULocationID": [100],
                "DOLocationID": [200],
            }
        )
        path = tmp_path / "yellow.parquet"
        df.to_parquet(path)

        rows = list(row_from_parquet_iter(path))
        assert len(rows) == 1
        row = rows[0]
        assert row["vendor_id"] == 1
        assert row["passenger_count"] == 2
        assert row["trip_distance"] == pytest.approx(3.1)
        assert row["fare_amount"] == pytest.approx(14.5)
        assert row["pickup_location_id"] == 100
        assert row["dropoff_location_id"] == 200
        assert row["pickup_datetime"] == "2024-01-15T08:00:00+00:00"

    def test_normalizes_green_schema_columns(self, tmp_path):
        pd = pytest.importorskip("pandas")
        pytest.importorskip("pyarrow")
        df = pd.DataFrame(
            {
                "vendor_id": [2],
                "lpep_pickup_datetime": [datetime(2024, 1, 15, 9, 0, 0)],
                "lpep_dropoff_datetime": [datetime(2024, 1, 15, 9, 20, 0)],
                "passenger_count": [1],
                "trip_distance": [1.2],
                "fare_amount": [7.0],
                "pickup_location_id": [50],
                "dropoff_location_id": [60],
            }
        )
        path = tmp_path / "green.parquet"
        df.to_parquet(path)

        rows = list(row_from_parquet_iter(path))
        assert rows[0]["vendor_id"] == 2
        assert rows[0]["pickup_location_id"] == 50


class TestRun:
    def test_run_sends_synthetic_rows_through_producer(self, monkeypatch):
        sent = []

        def fake_build_producer(bootstrap):
            def send(topic, payload):
                sent.append((topic, json.loads(payload)))
                if len(sent) >= 3:
                    import aide2.ingestion.kafka_producer as kp

                    kp._shutdown = True

            def flush():
                pass

            return send, flush

        import aide2.ingestion.kafka_producer as kp

        monkeypatch.setattr(kp, "build_producer", fake_build_producer)
        monkeypatch.setattr(kp, "_shutdown", False)

        run(
            bootstrap="fake:9092",
            topic="nyc-taxi-rides",
            rate=1000.0,
            parquet_path=None,
            jitter_ms=0.0,
            seed=1,
        )
        monkeypatch.setattr(kp, "_shutdown", False)

        assert len(sent) == 3
        assert all(topic == "nyc-taxi-rides" for topic, _ in sent)
        assert all("ride_id" in payload for _, payload in sent)

    def test_run_raises_for_missing_parquet_path(self, tmp_path):
        def fake_build_producer(bootstrap):
            return (lambda topic, payload: None), (lambda: None)

        with patch(
            "aide2.ingestion.kafka_producer.build_producer", side_effect=fake_build_producer
        ):
            missing = tmp_path / "does-not-exist.parquet"
            with pytest.raises(FileNotFoundError):
                run(
                    bootstrap="fake:9092",
                    topic="nyc-taxi-rides",
                    rate=10.0,
                    parquet_path=missing,
                    jitter_ms=0.0,
                    seed=None,
                )
