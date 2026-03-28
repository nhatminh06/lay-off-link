"""
Gold layer: analytics aggregates from Silver for downstream (e.g. Feast feature store).
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.window import Window

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from aide2.spark.utils import get_spark_session, merge_extra_configs

logger = logging.getLogger(__name__)

DEFAULT_SILVER_PATH = "s3a://lakehouse/silver/nyc_taxi/"
DEFAULT_GOLD_BASE = "s3a://lakehouse/gold/"

HOURLY_PATH = "nyc_taxi/hourly_stats"
DAILY_PATH = "nyc_taxi/daily_stats"
ZONE_PATH = "nyc_taxi/zone_stats"
DRIVER_PATH = "nyc_taxi/driver_patterns"


def _base_metrics():
    return [
        F.avg("fare_amount").alias("avg_fare"),
        F.avg("trip_distance").alias("avg_distance"),
        F.avg("trip_duration_minutes").alias("avg_duration"),
        F.count(F.lit(1)).alias("trip_count"),
        F.avg("speed_mph").alias("avg_speed"),
        F.sum("total_amount").alias("total_revenue"),
    ]


def hourly_stats(df):
    g = df.groupBy("hour_of_day").agg(*_base_metrics())
    return g.withColumn("gold_updated_at", F.current_timestamp())


def daily_stats(df):
    by_date = df.withColumn("trip_date", F.to_date(F.col("tpep_pickup_datetime")))
    peak = (
        by_date.groupBy("trip_date", "hour_of_day")
        .agg(F.count(F.lit(1)).alias("hc"))
        .withColumn(
            "rk",
            F.row_number().over(
                Window.partitionBy("trip_date").orderBy(F.desc("hc"), F.asc("hour_of_day"))
            ),
        )
        .filter(F.col("rk") == 1)
        .select("trip_date", F.col("hour_of_day").alias("peak_hour"))
    )
    daily = by_date.groupBy("trip_date").agg(
        *_base_metrics(),
        F.avg("passenger_count").alias("avg_passenger_count"),
    )
    return daily.join(peak, "trip_date", "left").withColumn(
        "gold_updated_at", F.current_timestamp()
    )


def zone_stats(df):
    cols = df.columns
    if "PULocationID" in cols and "DOLocationID" in cols:
        z = df.groupBy("PULocationID", "DOLocationID").agg(
            F.avg("fare_amount").alias("avg_fare"),
            F.count(F.lit(1)).alias("trip_count"),
            F.avg("trip_distance").alias("avg_distance"),
        )
        return z.withColumn("gold_updated_at", F.current_timestamp())
    if "geo_bucket_id" in cols:
        z = (
            df.filter(F.col("geo_bucket_id").isNotNull())
            .groupBy("geo_bucket_id")
            .agg(
                F.avg("fare_amount").alias("avg_fare"),
                F.count(F.lit(1)).alias("trip_count"),
                F.avg("trip_distance").alias("avg_distance"),
            )
        )
        return z.withColumn("gold_updated_at", F.current_timestamp())
    logger.warning("No PULocationID/DOLocationID or geo_bucket_id; zone_stats not produced.")
    return None


def driver_patterns(df):
    return (
        df.groupBy("VendorID", "day_of_week")
        .agg(
            F.count(F.lit(1)).alias("trip_count"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("trip_distance").alias("avg_distance"),
            F.avg("fare_amount").alias("avg_fare"),
        )
        .withColumn("gold_updated_at", F.current_timestamp())
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Gold: Silver Delta -> aggregate Gold tables")
    p.add_argument("--silver-path", default=DEFAULT_SILVER_PATH)
    p.add_argument("--gold-base", default=DEFAULT_GOLD_BASE, help="Prefix s3a://.../gold/")
    p.add_argument("--start-date", default=None)
    p.add_argument("--end-date", default=None)
    p.add_argument(
        "--write-mode",
        default="overwrite",
        choices=("overwrite", "append"),
        help="Gold table write mode (default overwrite for idempotent daily runs)",
    )
    return p.parse_args()


def run_gold(args: argparse.Namespace) -> None:
    extra = merge_extra_configs()
    spark = get_spark_session("gold_nyc_taxi_aggregation", extra_configs=extra)

    try:
        logger.info("Reading Silver from %s", args.silver_path)
        df = spark.read.format("delta").load(args.silver_path)
        n0 = df.count()
        logger.info("Silver row count: %d", n0)

        if args.start_date:
            df = df.filter(F.col("tpep_pickup_datetime") >= F.to_timestamp(F.lit(args.start_date)))
        if args.end_date:
            df = df.filter(F.col("tpep_pickup_datetime") < F.to_timestamp(F.lit(args.end_date)))

        n1 = df.count()
        logger.info("After date filter: %d rows", n1)

        if n1 == 0:
            logger.warning("No rows for Gold; exiting.")
            return

        base = args.gold_base.rstrip("/") + "/"

        h = hourly_stats(df)
        logger.info("hourly_stats rows: %d", h.count())
        (h.write.format("delta").mode(args.write_mode).save(base + HOURLY_PATH))

        d = daily_stats(df)
        logger.info("daily_stats rows: %d", d.count())
        (d.write.format("delta").mode(args.write_mode).save(base + DAILY_PATH))

        z = zone_stats(df)
        if z is not None:
            zc = z.count()
            logger.info("zone_stats rows: %d", zc)
            if zc > 0:
                (z.write.format("delta").mode(args.write_mode).save(base + ZONE_PATH))
            else:
                logger.warning("Skipping zone_stats write (no rows after aggregation).")
        else:
            logger.warning("Skipping zone_stats write (no location columns).")

        dp = driver_patterns(df)
        logger.info("driver_patterns rows: %d", dp.count())
        (dp.write.format("delta").mode(args.write_mode).save(base + DRIVER_PATH))

        logger.info("Gold aggregation completed. Outputs under %s", base)
    except Exception:
        logger.exception("Gold aggregation failed.")
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
    run_gold(args)


if __name__ == "__main__":
    main()
