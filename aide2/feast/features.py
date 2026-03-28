"""
Feast feature definitions for the NYC Taxi lakehouse gold layer.

Defines entities, batch sources (Parquet on S3), feature views, and an on-demand
view for fare ratio vs. zone averages.
"""

from datetime import timedelta

from feast import (
    Entity,
    FeatureView,
    Field,
    FileSource,
    RequestSource,
)
from feast.data_format import ParquetFormat
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Float64, Int64
from feast.value_type import ValueType

# --- Entities -----------------------------------------------------------------

taxi_zone = Entity(
    name="taxi_zone",
    join_keys=["location_id"],
    value_type=ValueType.INT64,
    description="Taxi zone / location identifier (TLC location_id).",
)

vendor = Entity(
    name="vendor",
    join_keys=["vendor_id"],
    value_type=ValueType.INT64,
    description="Vendor identifier.",
)

time_bucket = Entity(
    name="time_bucket",
    join_keys=["hour_of_day"],
    value_type=ValueType.INT64,
    description="Hour of day (0-23) for aggregated trip statistics.",
)

# --- Batch sources (Gold Delta/Parquet on object storage) ---------------------

hourly_stats_source = FileSource(
    name="hourly_stats_source",
    path="s3://lakehouse/gold/hourly_stats/",
    timestamp_field="event_timestamp",
    file_format=ParquetFormat(),
    description="Hourly aggregates from the gold zone.",
)

zone_stats_source = FileSource(
    name="zone_stats_source",
    path="s3://lakehouse/gold/zone_stats/",
    # Align gold table schema: use event_timestamp or as-of column for point-in-time joins.
    timestamp_field="event_timestamp",
    file_format=ParquetFormat(),
    description="Per-zone statistics from the gold zone.",
)

daily_stats_source = FileSource(
    name="daily_stats_source",
    path="s3://lakehouse/gold/daily_stats/",
    timestamp_field="event_timestamp",
    file_format=ParquetFormat(),
    description="Daily rollups from the gold zone.",
)

# --- Feature views ------------------------------------------------------------

hourly_trip_features = FeatureView(
    name="hourly_trip_features",
    entities=[time_bucket],
    ttl=timedelta(days=1),
    schema=[
        Field(name="avg_fare", dtype=Float64),
        Field(name="avg_distance", dtype=Float64),
        Field(name="avg_duration", dtype=Float64),
        Field(name="trip_count", dtype=Int64),
        Field(name="avg_speed", dtype=Float64),
        Field(name="total_revenue", dtype=Float64),
    ],
    source=hourly_stats_source,
    tags={"layer": "gold", "granularity": "hourly"},
)

zone_features = FeatureView(
    name="zone_features",
    entities=[taxi_zone],
    ttl=timedelta(days=7),
    schema=[
        Field(name="zone_avg_fare", dtype=Float64),
        Field(name="zone_trip_count", dtype=Int64),
        Field(name="zone_avg_distance", dtype=Float64),
    ],
    source=zone_stats_source,
    tags={"layer": "gold", "granularity": "zone"},
)

daily_trip_features = FeatureView(
    name="daily_trip_features",
    entities=[time_bucket],
    ttl=timedelta(days=30),
    schema=[
        Field(name="daily_avg_fare", dtype=Float64),
        Field(name="daily_trip_count", dtype=Int64),
        Field(name="peak_hour", dtype=Int64),
        Field(name="daily_total_revenue", dtype=Float64),
    ],
    source=daily_stats_source,
    tags={"layer": "gold", "granularity": "daily"},
)

# Request-time fields for on-demand transforms (real-time fare vs. zone average).
fare_request_source = RequestSource(
    name="fare_request",
    schema=[
        Field(name="fare_amount", dtype=Float64),
        Field(name="location_id", dtype=Int64),
    ],
    description="Live fare and pickup location for ratio features.",
)


@on_demand_feature_view(
    name="fare_prediction_features",
    entities=[taxi_zone],
    sources=[zone_features, fare_request_source],
    schema=[
        Field(name="location_id", dtype=Int64),
        Field(name="expected_fare_ratio", dtype=Float64),
    ],
    description="expected_fare_ratio = fare_amount / zone_avg_fare for the pickup zone.",
    mode="pandas",
)
def fare_prediction_features(inputs: dict):
    """
    Compute the ratio of observed fare to the zone's average fare.

    Args:
        inputs: Feast-injected dict keyed by source name (`zone_features`, `fare_request`).
    """
    import pandas as pd

    zone_df = inputs["zone_features"]
    req_df = inputs["fare_request"]

    zone_avg = zone_df["zone_avg_fare"].astype("float64")
    fare = req_df["fare_amount"].astype("float64")
    safe = zone_avg.replace(0, pd.NA)

    ratio = (fare / safe).astype("float64")

    return pd.DataFrame(
        {
            "location_id": req_df["location_id"],
            "expected_fare_ratio": ratio,
        }
    )
