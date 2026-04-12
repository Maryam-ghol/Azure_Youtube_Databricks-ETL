from src.transform import (
    flatten_channels,
    clean_channels,
    flatten_videos,
    flatten_video_statistics,
    join_video_data
)
from src.load import write_delta


# ---------------------------
# CHANNELS
# ---------------------------

def silver_channels(spark, catalog: str):
    df_raw = spark.table(f"{catalog}.bronze.channels_raw")

    df = flatten_channels(df_raw)
    df = clean_channels(df)

    write_delta(
        df,
        f"{catalog}.silver.channels",
        mode="overwrite"
    )


# ---------------------------
# VIDEOS
# ---------------------------

def silver_videos(spark, catalog: str):
    df_raw = spark.table(f"{catalog}.bronze.videos_raw")

    df = flatten_videos(df_raw)

    write_delta(
        df,
        f"{catalog}.silver.videos",
        mode="overwrite"
    )


# ---------------------------
# VIDEO STATS
# ---------------------------

def silver_video_stats(spark, catalog: str):
    df_raw = spark.table(f"{catalog}.bronze.video_stats_raw")

    df = flatten_video_statistics(df_raw)

    write_delta(
        df,
        f"{catalog}.silver.video_stats",
        mode="overwrite"
    )


# ---------------------------
# ENRICHED VIDEOS
# ---------------------------

def silver_videos_enriched(spark, catalog: str):
    df_videos = spark.table(f"{catalog}.silver.videos")
    df_stats = spark.table(f"{catalog}.silver.video_stats")

    df = join_video_data(df_videos, df_stats)

    write_delta(
        df,
        f"{catalog}.silver.videos_enriched",
        mode="overwrite"
    )


# ---------------------------
# FULL SILVER PIPELINE
# ---------------------------

def run_silver_pipeline(spark, catalog: str):
    silver_channels(spark, catalog)
    silver_videos(spark, catalog)
    silver_video_stats(spark, catalog)
    silver_videos_enriched(spark, catalog)