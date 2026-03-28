#!/usr/bin/env python3
"""
Simulate a live stream of NYC taxi ride events into Kafka for Flink or other consumers.

Reads rows from a Parquet file (or generates synthetic rides) and publishes JSON messages
to a Kafka topic at a configurable rate. Adds small random jitter between sends to mimic
real-time arrival. Handles SIGINT for graceful shutdown.

Dependencies: confluent-kafka (preferred) or kafka-python; pandas; pyarrow for Parquet.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional

logger = logging.getLogger(__name__)

DEFAULT_TOPIC = "nyc-taxi-rides"
_shutdown = False


def _set_shutdown_handler() -> None:
    def _handler(signum: int, _frame: Any) -> None:
        global _shutdown
        logger.info("Received signal %s, shutting down after current send...", signum)
        _shutdown = True

    signal.signal(signal.SIGINT, _handler)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _handler)


def build_producer(
    bootstrap: str,
) -> tuple[Callable[[str, bytes], None], Callable[[], None]]:
    """
    Returns (send_fn, flush_fn). Prefers confluent-kafka; falls back to kafka-python.
    """
    try:
        from confluent_kafka import Producer

        producer = Producer({"bootstrap.servers": bootstrap})

        def send(topic: str, payload: bytes) -> None:
            producer.produce(topic, payload)
            producer.poll(0)

        def flush() -> None:
            producer.flush(10)

        return send, flush
    except ImportError:
        logger.info("confluent-kafka not available; falling back to kafka-python")
        from kafka import KafkaProducer

        producer = KafkaProducer(
            bootstrap_servers=bootstrap.split(","),
            retries=3,
        )

        def send(topic: str, payload: bytes) -> None:
            producer.send(topic, value=payload)

        def flush() -> None:
            producer.flush(timeout=10)

        return send, flush


def _first_present_column(df: Any, candidates: List[str]) -> str:
    import pandas as pd

    for name in candidates:
        if name in df.columns:
            return name
    raise KeyError(f"None of {candidates} found in Parquet columns: {list(df.columns)}")


def row_from_parquet_iter(path: Path) -> Iterator[Dict[str, Any]]:
    """
    Yield ride dicts from TLC-style Parquet (yellow/green column naming variants).
    """
    import pandas as pd

    df = pd.read_parquet(path)

    col_vendor = _first_present_column(df, ["vendor_id", "VendorID"])
    col_pickup = _first_present_column(
        df,
        ["tpep_pickup_datetime", "lpep_pickup_datetime", "pickup_datetime"],
    )
    col_dropoff = _first_present_column(
        df,
        ["tpep_dropoff_datetime", "lpep_dropoff_datetime", "dropoff_datetime"],
    )
    col_pu = _first_present_column(df, ["PULocationID", "pickup_location_id"])
    col_do = _first_present_column(df, ["DOLocationID", "dropoff_location_id"])

    for _, row in df.iterrows():
        ride_id = str(uuid.uuid4())
        yield {
            "ride_id": ride_id,
            "vendor_id": int(row[col_vendor]),
            "pickup_datetime": _to_iso(row[col_pickup]),
            "dropoff_datetime": _to_iso(row[col_dropoff]),
            "passenger_count": int(row["passenger_count"]),
            "trip_distance": float(row["trip_distance"]),
            "fare_amount": float(row["fare_amount"]),
            "pickup_location_id": int(row[col_pu]),
            "dropoff_location_id": int(row[col_do]),
        }


def _to_iso(val: Any) -> str:
    ts = val
    if hasattr(ts, "to_pydatetime"):
        ts = ts.to_pydatetime()
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts.isoformat()
    return str(val)


def synthetic_row_iter(seed: Optional[int] = None) -> Iterator[Dict[str, Any]]:
    rng = random.Random(seed)
    base = datetime.now(tz=timezone.utc).replace(microsecond=0)
    while True:
        pickup = base + timedelta(minutes=rng.randint(0, 60))
        trip_min = rng.uniform(2, 45)
        dropoff = pickup + timedelta(minutes=trip_min)
        yield {
            "ride_id": str(uuid.uuid4()),
            "vendor_id": rng.choice([1, 2]),
            "pickup_datetime": pickup.isoformat(),
            "dropoff_datetime": dropoff.isoformat(),
            "passenger_count": rng.randint(1, 6),
            "trip_distance": round(rng.uniform(0.3, 25.0), 2),
            "fare_amount": round(rng.uniform(3.0, 80.0), 2),
            "pickup_location_id": rng.randint(1, 263),
            "dropoff_location_id": rng.randint(1, 263),
        }


def run(
    bootstrap: str,
    topic: str,
    rate: float,
    parquet_path: Optional[Path],
    jitter_ms: float,
    seed: Optional[int],
) -> None:
    send, flush_producer = build_producer(bootstrap)

    if parquet_path is not None:
        if not parquet_path.is_file():
            raise FileNotFoundError(parquet_path)
        row_iter = row_from_parquet_iter(parquet_path)
        logger.info("Streaming from Parquet %s", parquet_path)
    else:
        row_iter = synthetic_row_iter(seed=seed)
        logger.info("Streaming synthetic rides (seed=%s)", seed)

    interval = 1.0 / rate if rate > 0 else 0.0
    count = 0
    try:
        for row in row_iter:
            if _shutdown:
                break
            payload = json.dumps(row, separators=(",", ":")).encode("utf-8")
            send(topic, payload)
            count += 1
            if count % max(1, int(rate)) == 0:
                logger.info("Sent %s messages", count)
            delay = interval + random.uniform(0, jitter_ms / 1000.0)
            if delay > 0:
                time.sleep(delay)
    finally:
        try:
            flush_producer()
        except Exception:
            logger.exception("Producer flush failed")


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bootstrap-servers",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Kafka bootstrap servers (or env KAFKA_BOOTSTRAP_SERVERS).",
    )
    parser.add_argument(
        "--topic",
        default=DEFAULT_TOPIC,
        help=f"Target topic (default: {DEFAULT_TOPIC}).",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=10.0,
        help="Target records per second (approximate).",
    )
    parser.add_argument(
        "--parquet",
        type=Path,
        default=None,
        help="Path to a Parquet file; if omitted, synthetic data is generated.",
    )
    parser.add_argument(
        "--jitter-ms",
        type=float,
        default=50.0,
        help="Extra uniform delay [0, jitter] in milliseconds per message.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed for synthetic data only.",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    _set_shutdown_handler()

    try:
        run(
            bootstrap=args.bootstrap_servers,
            topic=args.topic,
            rate=args.rate,
            parquet_path=args.parquet,
            jitter_ms=args.jitter_ms,
            seed=args.seed,
        )
    except KeyboardInterrupt:
        logger.info("Interrupted")
        return 130
    except Exception:
        logger.exception("Producer failed")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
