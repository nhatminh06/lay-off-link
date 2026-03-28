"""
Real-time anomaly detection for NYC taxi rides from Kafka.

Stateful per-vendor rolling statistics (KeyedProcessFunction), rule-based checks,
and a simple Z-score on fare. Anomalies go to Kafka and to object storage
under ``s3a://lakehouse/anomalies/`` (Parquet filesystem sink; swap to the
Flink Delta connector for native Delta Lake tables in production).
"""

from __future__ import annotations

import json
import logging
import math
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional

from pyflink.common import Configuration, Row, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.table import EnvironmentSettings, Schema, StreamTableEnvironment
from pyflink.table.types import DataTypes

logger = logging.getLogger(__name__)

KAFKA_TOPIC_IN = "nyc-taxi-rides"
KAFKA_TOPIC_OUT = "taxi-anomalies"
DELTA_TABLE_PATH = "s3a://lakehouse/anomalies/"


def _bootstrap_servers() -> str:
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")


def _apply_s3a_config(conf: Configuration) -> None:
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    access = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    secret = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
    host = endpoint.replace("https://", "").replace("http://", "").rstrip("/")
    conf.set_string("fs.s3a.endpoint", host)
    conf.set_string("fs.s3a.access.key", access)
    conf.set_string("fs.s3a.secret.key", secret)
    conf.set_string("fs.s3a.path.style.access", "true")
    conf.set_string(
        "fs.s3a.connection.ssl.enabled", "true" if endpoint.startswith("https") else "false"
    )


def _parse_ride(raw: str) -> Optional[Row]:
    """Parse JSON line into a Row; return None if invalid."""
    try:
        obj: Dict[str, Any] = json.loads(raw)
    except json.JSONDecodeError:
        return None
    try:
        return Row(
            str(obj.get("ride_id", "")),
            str(obj.get("vendor_id", "")),
            str(obj.get("pickup_datetime", "")),
            str(obj.get("dropoff_datetime", "")),
            int(obj.get("passenger_count", 0)),
            float(obj.get("trip_distance", 0.0)),
            float(obj.get("fare_amount", 0.0)),
            str(obj.get("pickup_location_id", "")),
            str(obj.get("dropoff_location_id", "")),
        )
    except (TypeError, ValueError):
        return None


def _parse_ts_sec(s: str) -> Optional[float]:
    """Best-effort parse of datetime strings to epoch seconds."""
    if not s:
        return None
    s = s.strip()
    try:
        if s.isdigit():
            n = int(s)
            return n / 1000.0 if n > 1_000_000_000_000 else float(n)
        normalized = s.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized).timestamp()
    except (ValueError, OSError, TypeError):
        return None


@dataclass
class VendorAgg:
    """Online sufficient statistics for fare Z-score and rolling mean."""

    count: int = 0
    sum_fare: float = 0.0
    sumsq_fare: float = 0.0

    def mean_fare(self) -> float:
        return self.sum_fare / self.count if self.count else 0.0

    def std_fare(self) -> float:
        if self.count < 2:
            return 0.0
        m = self.mean_fare()
        var = max(0.0, (self.sumsq_fare / self.count) - m * m)
        return math.sqrt(var)


class VendorAnomalyDetector(KeyedProcessFunction):
    """
    Stateful detector: rolling mean per vendor, Z-score on fare, and rule checks.
    Emits JSON payload rows for anomalous rides only.
    """

    def __init__(self) -> None:
        super().__init__()
        self._state: Optional[Any] = None

    def open(self, ctx: RuntimeContext) -> None:
        desc = ValueStateDescriptor(
            "vendor_agg",
            Types.PICKLED_BYTE_ARRAY(),
        )
        self._state = ctx.get_state(desc)

    def process_element(self, value: Row, ctx: "KeyedProcessFunction.Context"):
        ride_id = value[0]
        vendor_id = value[1]
        pickup = value[2]
        dropoff = value[3]
        passenger_count = value[4]
        trip_distance = value[5]
        fare_amount = value[6]
        pickup_loc = value[7]

        raw = self._state.value()
        agg = VendorAgg()
        if raw is not None:
            try:
                prev = json.loads(raw.decode("utf-8"))
                agg = VendorAgg(
                    count=int(prev["c"]),
                    sum_fare=float(prev["sf"]),
                    sumsq_fare=float(prev["sff"]),
                )
            except (json.JSONDecodeError, KeyError, ValueError):
                pass

        reasons: list[str] = []

        if fare_amount < 0 or passenger_count <= 0:
            reasons.append("invalid_fare_or_passengers")

        if trip_distance > 100.0:
            reasons.append("distance_over_100_miles")

        mean_before = agg.mean_fare()
        if agg.count > 0 and fare_amount > 3.0 * mean_before:
            reasons.append("fare_gt_3x_rolling_mean")

        std = agg.std_fare()
        if agg.count >= 2 and std > 1e-9 and fare_amount > mean_before + 3.0 * std:
            reasons.append("fare_gt_mean_plus_3std")

        t0 = _parse_ts_sec(pickup)
        t1 = _parse_ts_sec(dropoff)
        if t0 is not None and t1 is not None and t1 > t0:
            duration_h = (t1 - t0) / 3600.0
            if duration_h > 0:
                mph = trip_distance / duration_h
                if mph > 100.0:
                    reasons.append("speed_over_100_mph")

        agg.count += 1
        agg.sum_fare += fare_amount
        agg.sumsq_fare += fare_amount * fare_amount
        self._state.update(
            json.dumps({"c": agg.count, "sf": agg.sum_fare, "sff": agg.sumsq_fare}).encode("utf-8")
        )

        if not reasons:
            return

        out = {
            "ride_id": ride_id,
            "vendor_id": vendor_id,
            "pickup_location_id": pickup_loc,
            "fare_amount": fare_amount,
            "trip_distance": trip_distance,
            "passenger_count": passenger_count,
            "reasons": reasons,
            "pickup_datetime": pickup,
            "dropoff_datetime": dropoff,
        }
        yield Row(json.dumps(out))


def _register_sinks(t_env: StreamTableEnvironment, bootstrap: str) -> None:
    """Kafka and MinIO Parquet sinks for anomaly JSON payloads."""
    t_env.execute_sql(
        f"""
        CREATE TABLE taxi_anomalies_kafka (
            payload STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{KAFKA_TOPIC_OUT}',
            'properties.bootstrap.servers' = '{bootstrap}',
            'format' = 'raw',
            'raw.charset' = 'UTF-8',
            'sink.delivery-guarantee' = 'at-least-once'
        )
        """
    )
    path = DELTA_TABLE_PATH.rstrip("/")
    t_env.execute_sql(
        f"""
        CREATE TABLE taxi_anomalies_delta (
            payload STRING
        ) WITH (
            'connector' = 'filesystem',
            'path' = '{path}',
            'format' = 'parquet',
            'sink.rolling-policy.file-size' = '64MB',
            'sink.rolling-policy.rollover-interval' = '2 m',
            'sink.partition-commit.policy.kind' = 'success-file'
        )
        """
    )


def build_and_execute() -> None:
    """Wire Kafka source, KeyedProcessFunction, and dual Table API sinks."""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(60_000)

    props = {
        "bootstrap.servers": _bootstrap_servers(),
        "group.id": "flink-taxi-anomalies",
    }
    consumer = FlinkKafkaConsumer(
        KAFKA_TOPIC_IN,
        SimpleStringSchema(),
        props,
    )
    consumer.set_start_from_latest()

    ds = env.add_source(consumer).map(_parse_ride).filter(lambda x: x is not None)
    out_type = Types.ROW_NAMED(["payload"], [Types.STRING()])
    anomalies = ds.key_by(lambda r: r[1] or "unknown").process(
        VendorAnomalyDetector(),
        output_type=out_type,
    )

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, settings)
    _apply_s3a_config(t_env.get_config().get_configuration())

    bootstrap = _bootstrap_servers()
    _register_sinks(t_env, bootstrap)

    table = t_env.from_data_stream(
        anomalies,
        Schema.new_builder().column("payload", DataTypes.STRING()).build(),
    )
    t_env.create_temporary_view("anomalies_payloads", table)

    stmt = t_env.create_statement_set()
    stmt.add_insert_sql("INSERT INTO taxi_anomalies_kafka SELECT payload FROM anomalies_payloads")
    stmt.add_insert_sql("INSERT INTO taxi_anomalies_delta SELECT payload FROM anomalies_payloads")
    logger.info("Submitting anomaly detection job (Kafka + filesystem sink).")
    stmt.execute()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
    )
    logger.info("Starting anomaly detection job; Kafka: %s", _bootstrap_servers())
    build_and_execute()


if __name__ == "__main__":
    main()
