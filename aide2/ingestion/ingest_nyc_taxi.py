#!/usr/bin/env python3
"""
Download NYC TLC trip Parquet files and upload them to MinIO (S3-compatible storage).

Optionally publishes a notification to Kafka when a file is successfully ingested.

Environment (MinIO):
    MINIO_ENDPOINT: Base URL for the S3 API (e.g. http://localhost:9000)
    MINIO_ACCESS_KEY, MINIO_SECRET_KEY: credentials

Optional:
    MINIO_BUCKET: Target bucket (default: raw-data)
    KAFKA_BOOTSTRAP_SERVERS: If set, enables Kafka notifications
    KAFKA_TOPIC: Override topic (default: data-ingestion-events)

Dependencies: requests, boto3; optional: kafka-python for Kafka.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Generator, Iterable, List, Optional

import boto3
import requests
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
DEFAULT_BUCKET = "raw-data"
KEY_PREFIX = "nyc-taxi"
DEFAULT_KAFKA_TOPIC = "data-ingestion-events"


@dataclass(frozen=True)
class Month:
    """Year/month pair for TLC file naming."""

    year: int
    month: int

    def label(self) -> str:
        return f"{self.year:04d}-{self.month:02d}"


def iter_months(start: Month, end: Month) -> Generator[Month, None, None]:
    """Yield every calendar month from start through end (inclusive)."""
    y, m = start.year, start.month
    ey, em = end.year, end.month
    while (y, m) <= (ey, em):
        yield Month(y, m)
        m += 1
        if m > 12:
            m = 1
            y += 1


def parse_month(s: str) -> Month:
    parts = s.strip().split("-")
    if len(parts) != 2:
        raise ValueError(f"Expected YYYY-MM, got {s!r}")
    y, m = int(parts[0]), int(parts[1])
    if not (1 <= m <= 12):
        raise ValueError(f"Invalid month in {s!r}")
    return Month(y, m)


def build_object_key(trip_type: str, ym: Month) -> str:
    fname = f"{trip_type}_tripdata_{ym.label()}.parquet"
    return f"{KEY_PREFIX}/{fname}"


def build_public_url(trip_type: str, ym: Month) -> str:
    fname = f"{trip_type}_tripdata_{ym.label()}.parquet"
    return f"{BASE_URL}/{fname}"


def download_with_retries(
    url: str,
    dest: Path,
    max_retries: int = 5,
    backoff_sec: float = 2.0,
    chunk_size: int = 1024 * 1024,
) -> None:
    """
    Stream-download a file with retries, logging progress against Content-Length when available.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    last_error: Optional[Exception] = None
    for attempt in range(1, max_retries + 1):
        try:
            with requests.get(url, stream=True, timeout=120) as resp:
                resp.raise_for_status()
                total = int(resp.headers.get("Content-Length") or 0)
                downloaded = 0
                with open(dest, "wb") as fh:
                    for chunk in resp.iter_content(chunk_size=chunk_size):
                        if not chunk:
                            continue
                        fh.write(chunk)
                        downloaded += len(chunk)
                        if total:
                            pct = 100.0 * downloaded / total
                            if downloaded % (10 * chunk_size) < len(chunk):
                                logger.info(
                                    "Download progress %s: %.1f%% (%s / %s bytes)",
                                    url,
                                    pct,
                                    downloaded,
                                    total,
                                )
                        else:
                            if downloaded % (50 * chunk_size) < len(chunk):
                                logger.info(
                                    "Downloaded %s bytes from %s (unknown total)", downloaded, url
                                )
            logger.info("Finished download: %s -> %s", url, dest)
            return
        except (requests.RequestException, OSError) as exc:
            last_error = exc
            logger.warning(
                "Download attempt %s/%s failed for %s: %s",
                attempt,
                max_retries,
                url,
                exc,
            )
            if dest.exists():
                try:
                    dest.unlink()
                except OSError:
                    pass
            if attempt < max_retries:
                sleep_for = backoff_sec * (2 ** (attempt - 1))
                logger.info("Retrying in %.1fs", sleep_for)
                time.sleep(sleep_for)
    assert last_error is not None
    raise last_error


def make_s3_client():
    endpoint = os.environ.get("MINIO_ENDPOINT")
    key = os.environ.get("MINIO_ACCESS_KEY")
    secret = os.environ.get("MINIO_SECRET_KEY")
    if not endpoint or not key or not secret:
        raise EnvironmentError(
            "MINIO_ENDPOINT, MINIO_ACCESS_KEY, and MINIO_SECRET_KEY must be set for MinIO uploads."
        )
    return boto3.client(
        "s3",
        endpoint_url=endpoint.rstrip("/"),
        aws_access_key_id=key,
        aws_secret_access_key=secret,
    )


def ensure_bucket(client, bucket: str) -> None:
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchBucket"):
            logger.info("Creating bucket %s", bucket)
            try:
                client.create_bucket(Bucket=bucket)
            except ClientError:
                # Some S3-compatible APIs need a LocationConstraint — try without first.
                logger.exception("Could not create bucket %s", bucket)
                raise
        else:
            raise


def upload_file(client, bucket: str, key: str, local_path: Path) -> None:
    logger.info("Uploading %s to s3://%s/%s", local_path, bucket, key)
    client.upload_file(str(local_path), bucket, key)


def verify_upload_size(client, bucket: str, key: str, expected: int) -> None:
    head = client.head_object(Bucket=bucket, Key=key)
    remote = int(head["ContentLength"])
    if remote != expected:
        raise ValueError(
            f"Size mismatch after upload: local={expected} remote={remote} for s3://{bucket}/{key}"
        )
    logger.info("Verified object size %s bytes for s3://%s/%s", remote, bucket, key)


def maybe_publish_kafka(
    bootstrap: str,
    topic: str,
    payload: dict,
) -> None:
    try:
        from kafka import KafkaProducer
    except ImportError as exc:
        logger.warning("kafka-python not installed; skipping Kafka publish: %s", exc)
        return
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            retries=3,
        )
        producer.send(topic, value=payload)
        producer.flush(timeout=10)
        logger.info("Published Kafka message to %s topic=%s", bootstrap, topic)
    except Exception:
        logger.exception("Kafka publish failed")


def run_ingestion(
    trip_types: List[str],
    months: Iterable[Month],
    workdir: Path,
    bucket: str,
    publish_kafka: bool,
    kafka_bootstrap: Optional[str],
    kafka_topic: str,
) -> None:
    client = make_s3_client()
    ensure_bucket(client, bucket)

    for ym in months:
        for trip_type in trip_types:
            url = build_public_url(trip_type, ym)
            key = build_object_key(trip_type, ym)
            local = workdir / f"{trip_type}_tripdata_{ym.label()}.parquet"
            logger.info("Processing %s", url)
            download_with_retries(url, local)
            local_size = local.stat().st_size
            upload_file(client, bucket, key, local)
            verify_upload_size(client, bucket, key, local_size)

            if publish_kafka and kafka_bootstrap:
                payload = {
                    "source": "nyc_tlc",
                    "trip_type": trip_type,
                    "year": ym.year,
                    "month": ym.month,
                    "s3_uri": f"s3://{bucket}/{key}",
                    "size_bytes": local_size,
                }
                maybe_publish_kafka(kafka_bootstrap, kafka_topic, payload)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--from",
        dest="from_month",
        required=True,
        help="Start month inclusive (YYYY-MM).",
    )
    parser.add_argument(
        "--to",
        dest="to_month",
        required=True,
        help="End month inclusive (YYYY-MM).",
    )
    parser.add_argument(
        "--trip-type",
        choices=("yellow", "green", "both"),
        default="both",
        help="Which TLC dataset to ingest (default: both).",
    )
    parser.add_argument(
        "--workdir",
        type=Path,
        default=Path("./.ingest_work"),
        help="Directory for temporary downloaded files.",
    )
    parser.add_argument(
        "--bucket",
        default=os.environ.get("MINIO_BUCKET", DEFAULT_BUCKET),
        help=f"S3/MinIO bucket (default env MINIO_BUCKET or {DEFAULT_BUCKET}).",
    )
    parser.add_argument(
        "--kafka",
        action="store_true",
        help="Publish to Kafka after each successful upload (needs KAFKA_BOOTSTRAP_SERVERS).",
    )
    parser.add_argument(
        "--kafka-topic",
        default=os.environ.get("KAFKA_TOPIC", DEFAULT_KAFKA_TOPIC),
        help=f"Kafka topic (default: {DEFAULT_KAFKA_TOPIC}).",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="DEBUG logging.",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    start_m = parse_month(args.from_month)
    end_m = parse_month(args.to_month)
    if (start_m.year, start_m.month) > (end_m.year, end_m.month):
        logger.error("--from must be <= --to")
        return 2

    if args.trip_type == "both":
        trip_types = ["yellow_tripdata", "green_tripdata"]
    else:
        trip_types = [f"{args.trip_type}_tripdata"]

    months = list(iter_months(start_m, end_m))
    kafka_bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")

    if args.kafka and not kafka_bootstrap:
        logger.error("--kafka requires KAFKA_BOOTSTRAP_SERVERS")
        return 2

    try:
        run_ingestion(
            trip_types=trip_types,
            months=months,
            workdir=args.workdir,
            bucket=args.bucket,
            publish_kafka=args.kafka,
            kafka_bootstrap=kafka_bootstrap,
            kafka_topic=args.kafka_topic,
        )
    except Exception:
        logger.exception("Ingestion failed")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
