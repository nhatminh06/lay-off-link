"""
Bronze layer: ingest raw NYC Taxi Parquet from MinIO into Delta Lake with metadata and validation.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StructType

# Allow running as script (spark-submit) or module
_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from aide2.spark.utils import get_spark_session, merge_extra_configs

logger = logging.getLogger(__name__)

DEFAULT_RAW_PATH = "s3a://raw-data/nyc-taxi/"
DEFAULT_BRONZE_PATH = "s3a://lakehouse/bronze/nyc_taxi/"

# NYC TLC yellow/green trip records — adjust if your raw files use different names
REQUIRED_COLUMNS = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "total_amount",
]

OPTIONAL_LOCATION_COLUMNS = ("PULocationID", "DOLocationID")
OPTIONAL_GEO_COLUMNS = (
    "pickup_longitude",
    "pickup_latitude",
    "dropoff_longitude",
    "dropoff_latitude",
)


def _validate_schema(df: DataFrame, required: list[str]) -> None:
    cols = set(df.columns)
    missing = [c for c in required if c not in cols]
    if missing:
        raise ValueError(
            f"Schema validation failed: missing required columns: {missing}. "
            f"Present columns: {sorted(cols)}"
        )


def _add_metadata(df: DataFrame, processing_date_expr) -> DataFrame:
    return (
        df.withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_file", F.input_file_name())
        .withColumn("processing_date", processing_date_expr)
    )


def _add_partition_cols(df: DataFrame) -> DataFrame:
    pickup = F.to_timestamp(F.col("tpep_pickup_datetime"))
    return (
        df.withColumn("pickup_ts", pickup)
        .withColumn("year", F.year("pickup_ts"))
        .withColumn("month", F.month("pickup_ts"))
        .drop("pickup_ts")
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Bronze: NYC Taxi Parquet -> Delta (MinIO)")
    p.add_argument(
        "--input-path",
        default=DEFAULT_RAW_PATH,
        help="Raw Parquet path (recursive read)",
    )
    p.add_argument(
        "--output-path",
        default=DEFAULT_BRONZE_PATH,
        help="Bronze Delta table path",
    )
    p.add_argument(
        "--start-date",
        default=None,
        help="Optional inclusive lower bound on tpep_pickup_datetime (YYYY-MM-DD)",
    )
    p.add_argument(
        "--end-date",
        default=None,
        help="Optional exclusive upper bound on tpep_pickup_datetime (YYYY-MM-DD)",
    )
    p.add_argument(
        "--processing-date",
        default=None,
        help="Literal processing_date column (YYYY-MM-DD). Default: current UTC date.",
    )
    p.add_argument(
        "--write-mode",
        default="append",
        choices=("append", "overwrite"),
        help="Delta write mode",
    )
    return p.parse_args()


def run_bronze(args: argparse.Namespace) -> None:
    extra = merge_extra_configs()
    spark = get_spark_session("bronze_nyc_taxi_ingestion", extra_configs=extra)

    try:
        logger.info("Reading raw Parquet from %s", args.input_path)
        df = spark.read.parquet(args.input_path)

        if isinstance(df.schema, StructType) and len(df.schema.fields) == 0:
            raise RuntimeError("No data read: empty schema or path not found.")

        _validate_schema(df, REQUIRED_COLUMNS)
        optional_present = [
            c for c in OPTIONAL_LOCATION_COLUMNS + OPTIONAL_GEO_COLUMNS if c in df.columns
        ]
        if optional_present:
            logger.info("Optional columns present: %s", optional_present)
        logger.info("Schema validation passed (%d columns).", len(df.columns))

        if args.processing_date:
            proc = F.to_date(F.lit(args.processing_date))
        else:
            proc = F.current_date()

        enriched = _add_metadata(df, proc)
        partitioned = _add_partition_cols(enriched)

        if args.start_date:
            partitioned = partitioned.filter(
                F.col("tpep_pickup_datetime") >= F.to_timestamp(F.lit(args.start_date))
            )
        if args.end_date:
            partitioned = partitioned.filter(
                F.col("tpep_pickup_datetime") < F.to_timestamp(F.lit(args.end_date))
            )

        count = partitioned.count()
        if count == 0:
            logger.warning("No rows after date filters; nothing written.")
            return

        logger.info(
            "Writing %d rows to Delta at %s (mode=%s)", count, args.output_path, args.write_mode
        )
        (
            partitioned.write.format("delta")
            .mode(args.write_mode)
            .partitionBy("year", "month")
            .save(args.output_path)
        )
        logger.info("Bronze ingestion completed successfully.")
    except Exception:
        logger.exception("Bronze ingestion failed.")
        raise
    finally:
        spark.stop()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    args = parse_args()
    logger.info(
        "Job start | processing_date=%s | input=%s | output=%s",
        args.processing_date or datetime.now(timezone.utc).date().isoformat(),
        args.input_path,
        args.output_path,
    )
    run_bronze(args)


if __name__ == "__main__":
    main()
