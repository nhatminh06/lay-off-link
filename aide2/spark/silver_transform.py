"""
Silver layer: clean and enrich Bronze NYC Taxi Delta; merge (UPSERT) into Silver.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from aide2.spark.utils import get_spark_session, merge_extra_configs

logger = logging.getLogger(__name__)

DEFAULT_BRONZE_PATH = "s3a://lakehouse/bronze/nyc_taxi/"
DEFAULT_SILVER_PATH = "s3a://lakehouse/silver/nyc_taxi/"

# Columns used to build a stable merge key when location IDs are missing
MERGE_KEY_HASH_COLS = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "total_amount",
]


def _log_stage(name: str, df: DataFrame) -> DataFrame:
    c = df.count()
    logger.info("Stage [%s]: row_count=%d", name, c)
    return df


def _build_merge_key(df: DataFrame) -> DataFrame:
    cols = df.columns
    has_pu = "PULocationID" in cols and "DOLocationID" in cols
    if has_pu:
        key = F.concat_ws(
            "|",
            F.col("VendorID").cast("string"),
            F.col("tpep_pickup_datetime").cast("string"),
            F.col("tpep_dropoff_datetime").cast("string"),
            F.col("PULocationID").cast("string"),
            F.col("DOLocationID").cast("string"),
        )
    else:
        parts = [
            F.coalesce(F.col(c).cast("string"), F.lit("")) for c in MERGE_KEY_HASH_COLS if c in cols
        ]
        key = F.concat_ws("|", *parts) if parts else F.lit("")

    return df.withColumn("merge_key", F.sha2(key, 256))


def _clean(df: DataFrame) -> DataFrame:
    # Dedupe by merge_key (keep one row per key — arbitrary but deterministic)
    w = Window.partitionBy("merge_key").orderBy(F.col("ingestion_timestamp").desc_nulls_last())
    deduped = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

    cleaned = deduped.filter(
        (F.col("fare_amount") >= 0)
        & (F.col("total_amount") >= 0)
        & (F.col("trip_distance") >= 0)
        & (F.col("passenger_count") > 0)
        & (F.col("passenger_count") <= 6)
    ).filter(~((F.col("trip_distance") == 0) & (F.col("fare_amount") > 50)))
    return cleaned


def _transform(df: DataFrame) -> DataFrame:
    pickup = F.to_timestamp("tpep_pickup_datetime")
    dropoff = F.to_timestamp("tpep_dropoff_datetime")
    duration_sec = F.unix_timestamp(dropoff) - F.unix_timestamp(pickup)
    duration_min = F.when(duration_sec > 0, duration_sec / 60.0).otherwise(None)

    trip_dist = F.when(F.col("trip_distance") > 0, F.col("trip_distance")).otherwise(None)
    speed = F.when(
        (duration_min.isNotNull()) & (duration_min > 0) & trip_dist.isNotNull(),
        (trip_dist * 60.0) / duration_min,
    ).otherwise(None)

    fare_pm = F.when(trip_dist.isNotNull() & (trip_dist > 0), F.col("fare_amount") / trip_dist)
    fare_pmin = F.when(
        duration_min.isNotNull() & (duration_min > 0),
        F.col("fare_amount") / duration_min,
    )

    hod = F.hour(pickup)
    dow = F.dayofweek(pickup)
    is_weekend = F.when((dow == 1) | (dow == 7), F.lit(True)).otherwise(F.lit(False))

    tod = (
        F.when((hod >= 5) & (hod < 12), F.lit("morning"))
        .when((hod >= 12) & (hod < 17), F.lit("afternoon"))
        .when((hod >= 17) & (hod < 21), F.lit("evening"))
        .otherwise(F.lit("night"))
    )

    out = (
        df.withColumn("pickup_ts", pickup)
        .withColumn("dropoff_ts", dropoff)
        .withColumn("trip_duration_minutes", duration_min)
        .withColumn(
            "speed_mph",
            F.when(speed.isNotNull(), F.least(speed, F.lit(120.0))).otherwise(speed),
        )
        .withColumn("fare_per_mile", fare_pm)
        .withColumn("fare_per_minute", fare_pmin)
        .withColumn("hour_of_day", hod)
        .withColumn("day_of_week", dow)
        .withColumn("is_weekend", is_weekend)
        .withColumn("time_of_day", tod)
    )

    # Geospatial bucketing (1-degree grid) when lat/lon exist
    has_pickup_ll = "pickup_latitude" in df.columns and "pickup_longitude" in df.columns
    if has_pickup_ll:
        out = (
            out.withColumn(
                "pickup_lat_bucket",
                F.round(F.col("pickup_latitude"), 2),
            )
            .withColumn(
                "pickup_lon_bucket",
                F.round(F.col("pickup_longitude"), 2),
            )
            .withColumn(
                "geo_bucket_id",
                F.concat_ws(
                    "_",
                    F.round(F.col("pickup_latitude"), 2).cast("string"),
                    F.round(F.col("pickup_longitude"), 2).cast("string"),
                ),
            )
        )
    else:
        out = (
            out.withColumn("pickup_lat_bucket", F.lit(None).cast("double"))
            .withColumn("pickup_lon_bucket", F.lit(None).cast("double"))
            .withColumn("geo_bucket_id", F.lit(None).cast("string"))
        )

    return out.drop("pickup_ts", "dropoff_ts")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Silver: Bronze Delta -> cleaned Silver Delta (merge)")
    p.add_argument("--bronze-path", default=DEFAULT_BRONZE_PATH)
    p.add_argument("--silver-path", default=DEFAULT_SILVER_PATH)
    p.add_argument("--start-date", default=None, help="Filter bronze pickup >= YYYY-MM-DD")
    p.add_argument("--end-date", default=None, help="Filter bronze pickup < YYYY-MM-DD")
    return p.parse_args()


def run_silver(args: argparse.Namespace) -> None:
    extra = merge_extra_configs()
    spark = get_spark_session("silver_nyc_taxi_transform", extra_configs=extra)

    try:
        logger.info("Reading Bronze Delta from %s", args.bronze_path)
        bronze = spark.read.format("delta").load(args.bronze_path)

        bronze = _log_stage("bronze_raw", bronze)

        if args.start_date:
            bronze = bronze.filter(
                F.col("tpep_pickup_datetime") >= F.to_timestamp(F.lit(args.start_date))
            )
        if args.end_date:
            bronze = bronze.filter(
                F.col("tpep_pickup_datetime") < F.to_timestamp(F.lit(args.end_date))
            )
        bronze = _log_stage("bronze_filtered", bronze)

        with_keys = _build_merge_key(bronze)
        cleaned = _clean(with_keys)
        cleaned = _log_stage("after_dedupe_and_filters", cleaned)

        silver_df = _transform(cleaned)
        silver_df = silver_df.withColumn("silver_updated_at", F.current_timestamp())
        silver_df = _log_stage("silver_transformed", silver_df)

        if silver_df.limit(1).count() == 0:
            logger.warning("No silver rows to merge.")
            return

        # Delta merge (UPSERT) on merge_key
        if DeltaTable.isDeltaTable(spark, args.silver_path):
            target = DeltaTable.forPath(spark, args.silver_path)
            (
                target.alias("t")
                .merge(
                    silver_df.alias("s"),
                    "t.merge_key = s.merge_key",
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
            logger.info("Merge into existing Silver table completed.")
        else:
            logger.info("No existing Silver table; initial write to %s", args.silver_path)
            (
                silver_df.write.format("delta")
                .mode("overwrite")
                .partitionBy("year", "month")
                .save(args.silver_path)
            )

        final = spark.read.format("delta").load(args.silver_path)
        _log_stage("silver_final", final)
        logger.info("Silver transform completed successfully.")
    except Exception:
        logger.exception("Silver transform failed.")
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
    run_silver(args)


if __name__ == "__main__":
    main()
