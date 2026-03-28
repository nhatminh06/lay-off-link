"""
PyFlink streaming pipeline for NYC taxi ride metrics.

Reads JSON events from Kafka, applies tumbling, sliding, and session windows
using the Table API, and writes results to Kafka and MinIO-backed storage.

Requires Flink Kafka and (for Delta) Delta Lake connector JARs on the classpath
when running in a cluster; local execution uses the same SQL contracts.
"""

from __future__ import annotations

import logging
import os
import sys
from typing import Optional

from pyflink.common import Configuration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

logger = logging.getLogger(__name__)


def _bootstrap_servers() -> str:
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")


def _minio_env() -> tuple[str, str, str]:
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    access = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    secret = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
    return endpoint, access, secret


def _apply_s3a_config(conf: Configuration) -> None:
    """Configure Hadoop s3a filesystem for MinIO (path-style, static creds)."""
    endpoint, access, secret = _minio_env()
    host = endpoint.replace("https://", "").replace("http://", "").rstrip("/")
    conf.set_string("fs.s3a.endpoint", host)
    conf.set_string("fs.s3a.access.key", access)
    conf.set_string("fs.s3a.secret.key", secret)
    conf.set_string("fs.s3a.path.style.access", "true")
    conf.set_string(
        "fs.s3a.connection.ssl.enabled", "true" if endpoint.startswith("https") else "false"
    )


def _apply_table_runtime_settings(t_env: StreamTableEnvironment) -> None:
    """Watermark max out-of-orderness 10s; align window late handling where supported."""
    cfg = t_env.get_config().get_configuration()
    # 10s allowed lateness for window results (Flink 1.16+ table option name may vary by version)
    for key, val in (
        ("table.exec.window.allow-lateness", "10 s"),
        ("table.exec.window.allowed-lateness", "10000 ms"),
    ):
        try:
            cfg.set_string(key, val)
        except Exception:  # noqa: BLE001 — optional across Flink minor versions
            logger.debug("Skipping unsupported table config: %s", key)


def build_table_env(
    parallelism: Optional[int] = None,
    checkpoint_interval_ms: int = 60_000,
) -> StreamTableEnvironment:
    """Create streaming TableEnvironment with checkpointing and MinIO/s3a defaults."""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(checkpoint_interval_ms)
    if parallelism is not None:
        env.set_parallelism(parallelism)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, settings)

    _apply_s3a_config(t_env.get_config().get_configuration())
    _apply_table_runtime_settings(t_env)

    return t_env


def register_kafka_source(t_env: StreamTableEnvironment, bootstrap: str) -> None:
    """Register Kafka source `nyc_taxi_rides` with JSON and event-time watermarks."""
    ddl = f"""
    CREATE TABLE nyc_taxi_rides (
        ride_id STRING,
        vendor_id STRING,
        pickup_datetime STRING,
        dropoff_datetime STRING,
        passenger_count INT,
        trip_distance DOUBLE,
        fare_amount DOUBLE,
        pickup_location_id STRING,
        dropoff_location_id STRING,
        pickup_ts AS CAST(TO_TIMESTAMP(CAST(pickup_datetime AS STRING)) AS TIMESTAMP(3)),
        WATERMARK FOR pickup_ts AS pickup_ts - INTERVAL '10' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'nyc-taxi-rides',
        'properties.bootstrap.servers' = '{bootstrap}',
        'properties.group.id' = 'flink-taxi-metrics',
        'scan.startup.mode' = 'latest-offset',
        'format' = 'json',
        'json.fail-on-missing-field' = 'false',
        'json.ignore-parse-errors' = 'true'
    )
    """
    t_env.execute_sql(ddl)


def register_kafka_metrics_sink(t_env: StreamTableEnvironment, bootstrap: str) -> None:
    """Append-only JSON sink for unified window metrics on `taxi-stream-metrics`."""
    ddl = f"""
    CREATE TABLE taxi_stream_metrics (
        metric_type STRING,
        window_start TIMESTAMP(3),
        window_end TIMESTAMP(3),
        ride_count BIGINT,
        avg_fare DOUBLE,
        total_revenue DOUBLE,
        avg_distance DOUBLE,
        pickup_location_id STRING
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'taxi-stream-metrics',
        'properties.bootstrap.servers' = '{bootstrap}',
        'format' = 'json',
        'sink.delivery-guarantee' = 'at-least-once'
    )
    """
    t_env.execute_sql(ddl)


def register_delta_like_sink(t_env: StreamTableEnvironment, subpath: str) -> str:
    """
    Register a filesystem sink suitable for MinIO; use Delta catalog in production.

    With `flink-sql-connector-delta-lake` + `connector` = 'delta', replace this DDL.
    """
    table_name = f"delta_metrics_{subpath.replace('/', '_').replace('-', '_')}"
    path = f"s3a://lakehouse/taxi_stream_metrics/{subpath}"
    ddl = f"""
    CREATE TABLE {table_name} (
        metric_type STRING,
        window_start TIMESTAMP(3),
        window_end TIMESTAMP(3),
        ride_count BIGINT,
        avg_fare DOUBLE,
        total_revenue DOUBLE,
        avg_distance DOUBLE,
        pickup_location_id STRING
    ) WITH (
        'connector' = 'filesystem',
        'path' = '{path}',
        'format' = 'parquet',
        'sink.rolling-policy.file-size' = '128MB',
        'sink.rolling-policy.rollover-interval' = '5 m',
        'sink.partition-commit.policy.kind' = 'success-file'
    )
    """
    t_env.execute_sql(ddl)
    return table_name


def add_window_inserts(t_env: StreamTableEnvironment, delta_table: str) -> None:
    """Register tumbling, sliding (hop), and session window inserts via StatementSet."""
    # Tumbling 5-minute: count, avg fare, total revenue
    tumbling_kafka = """
    INSERT INTO taxi_stream_metrics
    SELECT
        CAST('tumbling_5m' AS STRING) AS metric_type,
        window_start,
        window_end,
        COUNT(*) AS ride_count,
        AVG(fare_amount) AS avg_fare,
        SUM(fare_amount) AS total_revenue,
        CAST(NULL AS DOUBLE) AS avg_distance,
        CAST(NULL AS STRING) AS pickup_location_id
    FROM TABLE(
        TUMBLE(TABLE nyc_taxi_rides, DESCRIPTOR(pickup_ts), INTERVAL '5' MINUTE)
    )
    GROUP BY window_start, window_end
    """

    tumbling_delta = f"""
    INSERT INTO {delta_table}
    SELECT
        CAST('tumbling_5m' AS STRING),
        window_start,
        window_end,
        COUNT(*),
        AVG(fare_amount),
        SUM(fare_amount),
        CAST(NULL AS DOUBLE),
        CAST(NULL AS STRING)
    FROM TABLE(
        TUMBLE(TABLE nyc_taxi_rides, DESCRIPTOR(pickup_ts), INTERVAL '5' MINUTE)
    )
    GROUP BY window_start, window_end
    """

    # Sliding / hop: 15-minute window, 5-minute slide — moving averages of fare and distance
    sliding_kafka = """
    INSERT INTO taxi_stream_metrics
    SELECT
        CAST('sliding_15m_5m' AS STRING),
        window_start,
        window_end,
        COUNT(*) AS ride_count,
        AVG(fare_amount) AS avg_fare,
        SUM(fare_amount) AS total_revenue,
        AVG(trip_distance) AS avg_distance,
        CAST(NULL AS STRING) AS pickup_location_id
    FROM TABLE(
        HOP(TABLE nyc_taxi_rides, DESCRIPTOR(pickup_ts), INTERVAL '5' MINUTE, INTERVAL '15' MINUTE)
    )
    GROUP BY window_start, window_end
    """

    sliding_delta = f"""
    INSERT INTO {delta_table}
    SELECT
        CAST('sliding_15m_5m' AS STRING),
        window_start,
        window_end,
        COUNT(*),
        AVG(fare_amount),
        SUM(fare_amount),
        AVG(trip_distance),
        CAST(NULL AS STRING)
    FROM TABLE(
        HOP(TABLE nyc_taxi_rides, DESCRIPTOR(pickup_ts), INTERVAL '5' MINUTE, INTERVAL '15' MINUTE)
    )
    GROUP BY window_start, window_end
    """

    # Session: 30-minute gap, grouped by pickup location
    session_kafka = """
    INSERT INTO taxi_stream_metrics
    SELECT
        CAST('session_30m_gap' AS STRING),
        window_start,
        window_end,
        COUNT(*) AS ride_count,
        AVG(fare_amount) AS avg_fare,
        SUM(fare_amount) AS total_revenue,
        AVG(trip_distance) AS avg_distance,
        CAST(pickup_location_id AS STRING) AS pickup_location_id
    FROM TABLE(
        SESSION(TABLE nyc_taxi_rides, DESCRIPTOR(pickup_ts), INTERVAL '30' MINUTE)
    )
    GROUP BY window_start, window_end, pickup_location_id
    """

    session_delta = f"""
    INSERT INTO {delta_table}
    SELECT
        CAST('session_30m_gap' AS STRING),
        window_start,
        window_end,
        COUNT(*),
        AVG(fare_amount),
        SUM(fare_amount),
        AVG(trip_distance),
        CAST(pickup_location_id AS STRING)
    FROM TABLE(
        SESSION(TABLE nyc_taxi_rides, DESCRIPTOR(pickup_ts), INTERVAL '30' MINUTE)
    )
    GROUP BY window_start, window_end, pickup_location_id
    """

    stmt_set = t_env.create_statement_set()
    for sql in (
        tumbling_kafka,
        tumbling_delta,
        sliding_kafka,
        sliding_delta,
        session_kafka,
        session_delta,
    ):
        stmt_set.add_insert_sql(sql)

    logger.info("Submitting Flink job with tumbling, sliding, and session window sinks.")
    stmt_set.execute()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
    )
    bootstrap = _bootstrap_servers()
    logger.info("Kafka bootstrap servers: %s", bootstrap)

    t_env = build_table_env()
    register_kafka_source(t_env, bootstrap)
    register_kafka_metrics_sink(t_env, bootstrap)
    delta_table = register_delta_like_sink(t_env, "stream_metrics")
    add_window_inserts(t_env, delta_table)


if __name__ == "__main__":
    main()
