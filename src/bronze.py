from pyspark.sql.functions import current_timestamp, lit
from src.extract import (
    fetch_channel_data,
    get_uploads_playlist_id,
    fetch_playlist_videos,
    fetch_video_statistics
)
from src.transform import json_to_df
from src.load import write_delta


# ---------------------------
# CHANNELS
# ---------------------------

def bronze_channels(spark, youtube, channel_id: str, catalog: str):
    data = fetch_channel_data(youtube, channel_id)

    df = json_to_df(spark, data)

    df = df.withColumn("channel_id_input", lit(channel_id))  # ✅ FIXED
    df = df.withColumn("ingestion_time", current_timestamp())

    write_delta(
        df,
        f"{catalog}.bronze.channels_raw",
        mode="append"
    )


# ---------------------------
# VIDEOS
# ---------------------------

def bronze_videos(spark, youtube, channel_id: str, catalog: str):
    playlist_id = get_uploads_playlist_id(youtube, channel_id)
    videos = fetch_playlist_videos(youtube, playlist_id)

    df = json_to_df(spark, videos)

    df = df.withColumn("channel_id_input", lit(channel_id))  # ✅ FIXED
    df = df.withColumn("playlist_id_input", lit(playlist_id))  # ✅ NEW
    df = df.withColumn("ingestion_time", current_timestamp())

    write_delta(
        df,
        f"{catalog}.bronze.videos_raw",
        mode="append"
    )

    return videos


# ---------------------------
# VIDEO STATS (WITH DURATION)
# ---------------------------

def bronze_video_stats(spark, youtube, videos: list, channel_id: str, catalog: str):
    video_ids = [v["snippet"]["resourceId"]["videoId"] for v in videos]

    # ⚠️ IMPORTANT: must include contentDetails
    stats = fetch_video_statistics(youtube, video_ids)

    df = json_to_df(spark, stats)

    df = df.withColumn("channel_id_input", lit(channel_id))  # ✅ ADD
    df = df.withColumn("ingestion_time", current_timestamp())

    write_delta(
        df,
        f"{catalog}.bronze.video_stats_raw",
        mode="append"
    )


# ---------------------------
# FULL BRONZE PIPELINE
# ---------------------------

def run_bronze_pipeline(spark, youtube, channel_id: str, catalog: str):
    bronze_channels(spark, youtube, channel_id, catalog)
    videos = bronze_videos(spark, youtube, channel_id, catalog)
    bronze_video_stats(spark, youtube, videos, channel_id, catalog)