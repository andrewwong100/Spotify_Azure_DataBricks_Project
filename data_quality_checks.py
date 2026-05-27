# Databricks notebook source
# data_quality_checks.py
# Post-pipeline data quality assertions for Silver and Gold layers.
# Fails the job task (raises exception) if any critical check fails.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Data Quality Checks
# MAGIC Runs after each pipeline run to validate Silver and Gold table integrity.
# MAGIC Critical failures raise exceptions → ADF marks pipeline as failed → alert fires.

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, max as _max, min as _min
from datetime import date, timedelta

spark = SparkSession.builder.getOrCreate()

failures = []

def check(name: str, passed: bool, detail: str = "") -> None:
    status = "✅ PASS" if passed else "❌ FAIL"
    msg = f"{status}  |  {name}"
    if detail:
        msg += f"  →  {detail}"
    print(msg)
    if not passed:
        failures.append(name)

# COMMAND ----------
# MAGIC %md ### Silver checks

# COMMAND ----------
dim_tracks  = spark.table("spotify_catalog.silver.dim_tracks")
dim_artists = spark.table("spotify_catalog.silver.dim_artists")

# 1. No null primary keys
null_track_keys = dim_tracks.filter(col("track_key").isNull()).count()
check("dim_tracks: no null track_key", null_track_keys == 0, f"{null_track_keys} nulls")

null_artist_keys = dim_artists.filter(col("artist_key").isNull()).count()
check("dim_artists: no null artist_key", null_artist_keys == 0, f"{null_artist_keys} nulls")

# 2. SCD Type 2 — exactly one current record per natural key
scd_dupes = (
    dim_artists
    .filter(col("is_current") == True)
    .groupBy("artist_id")
    .agg(count("*").alias("n"))
    .filter(col("n") > 1)
    .count()
)
check("dim_artists: one current SCD2 record per artist_id", scd_dupes == 0, f"{scd_dupes} duplicates")

# 3. Minimum row count (guards against accidental full truncation)
track_count = dim_tracks.count()
check("dim_tracks: at least 10,000 rows", track_count >= 10_000, f"{track_count:,} rows")

artist_count = dim_artists.count()
check("dim_artists: at least 1,000 rows", artist_count >= 1_000, f"{artist_count:,} rows")

# COMMAND ----------
# MAGIC %md ### Gold checks

# COMMAND ----------
fact_streams = spark.table("spotify_catalog.gold.fact_streams")

# 4. Yesterday's data must exist (freshness check)
yesterday = (date.today() - timedelta(days=1)).isoformat()
yesterday_count = fact_streams.filter(col("stream_date") == yesterday).count()
check(
    f"fact_streams: data exists for {yesterday}",
    yesterday_count > 0,
    f"{yesterday_count:,} rows"
)

# 5. No negative stream counts
neg_streams = fact_streams.filter(col("total_streams") < 0).count()
check("fact_streams: no negative total_streams", neg_streams == 0, f"{neg_streams} rows")

# 6. Skip rate must be between 0 and 1
invalid_skip_rate = fact_streams.filter(
    (col("skip_rate") < 0) | (col("skip_rate") > 1)
).count()
check("fact_streams: skip_rate between 0 and 1", invalid_skip_rate == 0, f"{invalid_skip_rate} rows")

# 7. No orphaned fact rows (every track_key must exist in dim_tracks)
orphans = (
    fact_streams
    .join(dim_tracks, fact_streams.track_key == dim_tracks.track_key, "left_anti")
    .count()
)
check("fact_streams: no orphaned track_key", orphans == 0, f"{orphans} orphans")

# COMMAND ----------
# MAGIC %md ### Final result

# COMMAND ----------
print(f"\n{'='*60}")
print(f"Data quality summary: {len(failures)} failure(s)")
if failures:
    print(f"Failed checks: {failures}")
    raise Exception(f"Data quality FAILED: {failures}")
else:
    print("All checks passed ✅")
