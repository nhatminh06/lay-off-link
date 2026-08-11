"""
Real, local-Spark unit tests for the medallion pipeline transform functions.

These exercise the actual bronze/silver/gold DataFrame-in/DataFrame-out logic
against a local (in-process) SparkSession fixture (see conftest.py). The
I/O-bound orchestrators (run_bronze/run_silver/run_gold) read/write Delta
tables on S3A (MinIO) and are out of scope here — they are statically reviewed
only, not executed by these tests.
"""

from datetime import datetime

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from aide2.spark.bronze_ingestion import (
    REQUIRED_COLUMNS,
    _add_metadata,
    _add_partition_cols,
    _validate_schema,
)
from aide2.spark.gold_aggregation import (
    daily_stats,
    driver_patterns,
    hourly_stats,
    zone_stats,
)
from aide2.spark.silver_transform import _build_merge_key, _clean, _transform
from aide2.spark.utils import merge_extra_configs

RAW_SCHEMA = StructType(
    [
        StructField("VendorID", IntegerType()),
        StructField("tpep_pickup_datetime", TimestampType()),
        StructField("tpep_dropoff_datetime", TimestampType()),
        StructField("passenger_count", IntegerType()),
        StructField("trip_distance", DoubleType()),
        StructField("fare_amount", DoubleType()),
        StructField("total_amount", DoubleType()),
        StructField("PULocationID", IntegerType()),
        StructField("DOLocationID", IntegerType()),
    ]
)


def _raw_row(
    vendor=1,
    pickup=datetime(2024, 1, 15, 8, 30, 0),
    dropoff=datetime(2024, 1, 15, 8, 45, 0),
    passengers=1,
    distance=2.5,
    fare=12.0,
    total=15.0,
    pu_loc=100,
    do_loc=200,
):
    return (vendor, pickup, dropoff, passengers, distance, fare, total, pu_loc, do_loc)


class TestBronzeIngestion:
    def test_required_columns_declared(self):
        expected = [
            "VendorID",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "fare_amount",
            "total_amount",
        ]
        assert REQUIRED_COLUMNS == expected

    def test_validate_schema_passes_with_all_required_columns(self, spark):
        df = spark.createDataFrame([_raw_row()], RAW_SCHEMA)
        _validate_schema(df, REQUIRED_COLUMNS)  # must not raise

    def test_validate_schema_raises_on_missing_column(self, spark):
        df = spark.createDataFrame([_raw_row()], RAW_SCHEMA).drop("fare_amount")
        with pytest.raises(ValueError, match="fare_amount"):
            _validate_schema(df, REQUIRED_COLUMNS)

    def test_add_metadata_adds_expected_columns(self, spark):
        df = spark.createDataFrame([_raw_row()], RAW_SCHEMA)
        enriched = _add_metadata(df, F.to_date(F.lit("2024-01-15")))
        assert set(["ingestion_timestamp", "source_file", "processing_date"]) <= set(
            enriched.columns
        )
        row = enriched.collect()[0]
        assert row["processing_date"].isoformat() == "2024-01-15"
        assert row["ingestion_timestamp"] is not None

    def test_add_partition_cols_derives_year_month(self, spark):
        df = spark.createDataFrame([_raw_row()], RAW_SCHEMA)
        out = _add_partition_cols(df)
        row = out.collect()[0]
        assert row["year"] == 2024
        assert row["month"] == 1
        assert "pickup_ts" not in out.columns


class TestSilverBuildMergeKey:
    def test_merge_key_deterministic_with_location_ids(self, spark):
        df = spark.createDataFrame([_raw_row(), _raw_row()], RAW_SCHEMA)
        out = _build_merge_key(df)
        keys = [r["merge_key"] for r in out.collect()]
        assert keys[0] == keys[1]  # identical input rows -> identical key
        assert all(isinstance(k, str) and len(k) == 64 for k in keys)  # sha2-256 hex

    def test_merge_key_differs_for_different_rows(self, spark):
        df = spark.createDataFrame([_raw_row(vendor=1), _raw_row(vendor=2)], RAW_SCHEMA)
        out = _build_merge_key(df)
        keys = [r["merge_key"] for r in out.collect()]
        assert keys[0] != keys[1]

    def test_merge_key_falls_back_without_location_columns(self, spark):
        df = spark.createDataFrame([_raw_row()], RAW_SCHEMA).drop("PULocationID", "DOLocationID")
        out = _build_merge_key(df)
        assert out.collect()[0]["merge_key"] is not None


SILVER_INPUT_SCHEMA = StructType(
    RAW_SCHEMA.fields + [StructField("ingestion_timestamp", TimestampType())]
)


class TestSilverClean:
    def _with_key(self, spark, rows):
        df = spark.createDataFrame(rows, SILVER_INPUT_SCHEMA)
        return _build_merge_key(df)

    def test_dedupes_by_merge_key_keeping_latest_ingestion(self, spark):
        older = _raw_row() + (datetime(2024, 1, 15, 9, 0, 0),)
        newer = _raw_row() + (datetime(2024, 1, 15, 10, 0, 0),)
        df = self._with_key(spark, [older, newer])
        cleaned = _clean(df)
        rows = cleaned.collect()
        assert len(rows) == 1
        assert rows[0]["ingestion_timestamp"] == datetime(2024, 1, 15, 10, 0, 0)

    def test_filters_negative_fare(self, spark):
        bad = _raw_row(fare=-5.0) + (datetime(2024, 1, 15, 9, 0, 0),)
        df = self._with_key(spark, [bad])
        assert _clean(df).count() == 0

    def test_filters_out_of_range_passenger_count(self, spark):
        bad = _raw_row(passengers=0) + (datetime(2024, 1, 15, 9, 0, 0),)
        too_many = _raw_row(passengers=9) + (datetime(2024, 1, 15, 9, 0, 0),)
        df = self._with_key(spark, [bad, too_many])
        assert _clean(df).count() == 0

    def test_filters_zero_distance_high_fare_outlier(self, spark):
        outlier = _raw_row(distance=0.0, fare=99.0) + (datetime(2024, 1, 15, 9, 0, 0),)
        df = self._with_key(spark, [outlier])
        assert _clean(df).count() == 0

    def test_keeps_valid_row(self, spark):
        good = _raw_row() + (datetime(2024, 1, 15, 9, 0, 0),)
        df = self._with_key(spark, [good])
        assert _clean(df).count() == 1


class TestSilverTransform:
    def test_derived_columns_present(self, spark):
        df = spark.createDataFrame([_raw_row()], RAW_SCHEMA)
        out = _transform(df)
        expected = {
            "trip_duration_minutes",
            "speed_mph",
            "fare_per_mile",
            "fare_per_minute",
            "hour_of_day",
            "day_of_week",
            "is_weekend",
            "time_of_day",
        }
        assert expected <= set(out.columns)

    def test_derived_values_correct_for_known_row(self, spark):
        # 15 minutes, 2.5 miles -> duration=15.0, speed=10.0 mph
        df = spark.createDataFrame([_raw_row()], RAW_SCHEMA)
        row = _transform(df).collect()[0]
        assert row["trip_duration_minutes"] == pytest.approx(15.0)
        assert row["speed_mph"] == pytest.approx(10.0)
        assert row["hour_of_day"] == 8
        assert row["time_of_day"] == "morning"
        assert row["is_weekend"] is False  # 2024-01-15 is a Monday

    def test_time_of_day_buckets(self, spark):
        cases = [(2, "night"), (7, "morning"), (14, "afternoon"), (19, "evening")]
        rows = [
            _raw_row(pickup=datetime(2024, 1, 15, h, 0, 0), dropoff=datetime(2024, 1, 15, h, 10, 0))
            for h, _ in cases
        ]
        df = spark.createDataFrame(rows, RAW_SCHEMA)
        out = _transform(df).orderBy("hour_of_day").collect()
        got = {r["hour_of_day"]: r["time_of_day"] for r in out}
        for hour, label in cases:
            assert got[hour] == label

    def test_weekend_flagged_for_sunday(self, spark):
        sunday = _raw_row(
            pickup=datetime(2024, 1, 14, 10, 0, 0), dropoff=datetime(2024, 1, 14, 10, 20, 0)
        )
        df = spark.createDataFrame([sunday], RAW_SCHEMA)
        assert _transform(df).collect()[0]["is_weekend"] is True

    def test_geo_bucket_null_without_lat_lon(self, spark):
        df = spark.createDataFrame([_raw_row()], RAW_SCHEMA)
        row = _transform(df).collect()[0]
        assert row["geo_bucket_id"] is None

    def test_geo_bucket_populated_with_lat_lon(self, spark):
        schema = StructType(
            RAW_SCHEMA.fields
            + [
                StructField("pickup_latitude", DoubleType()),
                StructField("pickup_longitude", DoubleType()),
            ]
        )
        df = spark.createDataFrame([_raw_row() + (40.7128, -74.006)], schema)
        row = _transform(df).collect()[0]
        assert row["geo_bucket_id"] == "40.71_-74.01"


GOLD_SCHEMA = StructType(
    [
        StructField("VendorID", IntegerType()),
        StructField("tpep_pickup_datetime", TimestampType()),
        StructField("passenger_count", IntegerType()),
        StructField("trip_distance", DoubleType()),
        StructField("fare_amount", DoubleType()),
        StructField("total_amount", DoubleType()),
        StructField("trip_duration_minutes", DoubleType()),
        StructField("speed_mph", DoubleType()),
        StructField("hour_of_day", IntegerType()),
        StructField("day_of_week", IntegerType()),
        StructField("is_weekend", BooleanType()),
        StructField("PULocationID", IntegerType()),
        StructField("DOLocationID", IntegerType()),
        StructField("geo_bucket_id", StringType()),
    ]
)


def _gold_row(
    vendor=1,
    pickup=datetime(2024, 1, 15, 8, 30, 0),
    passengers=1,
    distance=2.5,
    fare=12.0,
    total=15.0,
    duration=15.0,
    speed=10.0,
    hour=8,
    dow=2,
    weekend=False,
    pu=100,
    do=200,
    geo=None,
):
    return (
        vendor,
        pickup,
        passengers,
        distance,
        fare,
        total,
        duration,
        speed,
        hour,
        dow,
        weekend,
        pu,
        do,
        geo,
    )


class TestGoldAggregation:
    def test_hourly_stats_aggregates_by_hour(self, spark):
        df = spark.createDataFrame(
            [
                _gold_row(hour=8, fare=10.0),
                _gold_row(hour=8, fare=20.0),
                _gold_row(hour=9, fare=5.0),
            ],
            GOLD_SCHEMA,
        )
        out = {r["hour_of_day"]: r for r in hourly_stats(df).collect()}
        assert out[8]["trip_count"] == 2
        assert out[8]["avg_fare"] == pytest.approx(15.0)
        assert out[9]["trip_count"] == 1

    def test_daily_stats_includes_peak_hour(self, spark):
        rows = [
            _gold_row(pickup=datetime(2024, 1, 15, 8, 0, 0), hour=8),
            _gold_row(pickup=datetime(2024, 1, 15, 8, 30, 0), hour=8),
            _gold_row(pickup=datetime(2024, 1, 15, 20, 0, 0), hour=20),
        ]
        df = spark.createDataFrame(rows, GOLD_SCHEMA)
        row = daily_stats(df).collect()[0]
        assert row["trip_count"] == 3
        assert row["peak_hour"] == 8  # 2 trips at hour 8 beats 1 trip at hour 20

    def test_zone_stats_uses_location_ids_when_present(self, spark):
        df = spark.createDataFrame(
            [_gold_row(pu=100, do=200), _gold_row(pu=100, do=200), _gold_row(pu=101, do=201)],
            GOLD_SCHEMA,
        )
        out = zone_stats(df)
        assert out is not None
        rows = {(r["PULocationID"], r["DOLocationID"]): r["trip_count"] for r in out.collect()}
        assert rows[(100, 200)] == 2
        assert rows[(101, 201)] == 1

    def test_zone_stats_falls_back_to_geo_bucket(self, spark):
        df = spark.createDataFrame(
            [_gold_row(pu=None, do=None, geo="40.71_-74.01")], GOLD_SCHEMA
        ).drop("PULocationID", "DOLocationID")
        out = zone_stats(df)
        assert out is not None
        assert out.collect()[0]["trip_count"] == 1

    def test_zone_stats_returns_none_without_location_data(self, spark):
        df = spark.createDataFrame([_gold_row()], GOLD_SCHEMA).drop(
            "PULocationID", "DOLocationID", "geo_bucket_id"
        )
        assert zone_stats(df) is None

    def test_driver_patterns_groups_by_vendor_and_day(self, spark):
        df = spark.createDataFrame(
            [_gold_row(vendor=1, dow=2), _gold_row(vendor=1, dow=2), _gold_row(vendor=2, dow=3)],
            GOLD_SCHEMA,
        )
        rows = {
            (r["VendorID"], r["day_of_week"]): r["trip_count"]
            for r in driver_patterns(df).collect()
        }
        assert rows[(1, 2)] == 2
        assert rows[(2, 3)] == 1


class TestSparkUtils:
    def test_merge_extra_configs_stringifies_and_drops_none(self):
        out = merge_extra_configs(a=1, b=None, c="x", d=True)
        assert out == {"a": "1", "c": "x", "d": "True"}

    def test_merge_extra_configs_empty(self):
        assert merge_extra_configs() == {}
