# Databricks notebook source
# gold_fact_streams.py
# Gold layer: fact_streams — aggregated streaming metrics for BI and analytics.
# Reads from Silver dimensions and builds a query-optimized star schema fact table.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold Layer — fact_streams
# MAGIC Joins Silver dimension tables to produce the canonical streaming analytics fact table.
# MAGIC Partitioned by `stream_date` for fast time-range queries.

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as _sum, avg, countDistinct,
    date_trunc, current_timestamp, dense_rank
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

spark = SparkSession.builder.getOrCreate()

STORAGE_ACCOUNT = spark.conf.get("spark.storage.account", "yourstorageacct")
GOLD_BASE = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net"

# COMMAND ----------
# MAGIC %md ### 1. Load Silver tables

# COMMAND ----------
tracks    = spark.table("spotify_catalog.silver.dim_tracks")
artists   = spark.table("spotify_catalog.silver.dim_artists")
albums    = spark.table("spotify_catalog.silver.dim_albums")
streams   = spark.table("spotify_catalog.silver.fact_stream_events")   # from silver_dimensions

# COMMAND ----------
# MAGIC %md ### 2. Build enriched stream events (join all dimensions)

# COMMAND ----------
enriched = (
    streams
    .join(tracks,  streams.track_id  == tracks.track_key,   "left")
    .join(artists, streams.artist_id == artists.artist_key, "left")
    .join(albums,  streams.album_id  == albums.album_key,   "left")
    .select(
        # Temporal
        streams.stream_id,
        streams.stream_timestamp,
        date_trunc("day", streams.stream_timestamp).alias("stream_date"),

        # Track dimensions
        tracks.track_key,
        tracks.track_name,
        tracks.duration_ms,
        tracks.explicit,
        tracks.track_popularity,

        # Artist dimensions (current record via SCD Type 2)
        artists.artist_key,
        artists.artist_name,
        artists.genres,                      # genre array — tracked historically via SCD2
        artists.artist_popularity,
        artists.followers,

        # Album dimensions
        albums.album_key,
        albums.album_name,
        albums.album_type,
        albums.release_date,

        # Stream metrics
        streams.play_duration_ms,
        streams.was_skipped,
        streams.platform,
        streams.country_code,
    )
)

# COMMAND ----------
# MAGIC %md ### 3. Aggregate to daily grain per track × artist × market

# COMMAND ----------
daily_agg = (
    enriched
    .groupBy(
        "stream_date", "track_key", "track_name",
        "artist_key", "artist_name", "genres",
        "album_key", "album_name", "country_code", "platform"
    )
    .agg(
        count("stream_id").alias("total_streams"),
        countDistinct("stream_id").alias("unique_streams"),
        _sum("play_duration_ms").alias("total_play_ms"),
        avg("play_duration_ms").alias("avg_play_ms"),
        _sum(col("was_skipped").cast("int")).alias("total_skips"),
        (
            _sum(col("was_skipped").cast("int")) / count("stream_id")
        ).alias("skip_rate"),
    )
    .withColumn("_updated_at", current_timestamp())
)

# COMMAND ----------
# MAGIC %md ### 4. MERGE into Gold Delta table (upsert — idempotent)

# COMMAND ----------
target_table = "spotify_catalog.gold.fact_streams"

if spark.catalog.tableExists(target_table):
    gold_table = DeltaTable.forName(spark, target_table)
    (
        gold_table.alias("target")
        .merge(
            daily_agg.alias("source"),
            """
            target.stream_date   = source.stream_date AND
            target.track_key     = source.track_key   AND
            target.artist_key    = source.artist_key  AND
            target.country_code  = source.country_code AND
            target.platform      = source.platform
            """
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"✅ MERGE complete → {target_table}")
else:
    (
        daily_agg.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("stream_date")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )
    print(f"✅ Initial write complete → {target_table}")

# COMMAND ----------
# MAGIC %md ### 5. Optimize + Z-Order for query performance

# COMMAND ----------
spark.sql(f"""
    OPTIMIZE {target_table}
    ZORDER BY (artist_key, stream_date, country_code)
""")
print("✅ OPTIMIZE + ZORDER complete")
