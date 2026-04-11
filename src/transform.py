from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    explode,
    from_json,
    schema_of_json,
    lit,
    current_timestamp
)
import json


# ---------------------------
# JSON → DataFrame
# ---------------------------

def json_to_df(spark, data) -> DataFrame:
    """
    Convert Python JSON (dict/list) to Spark DataFrame with inferred schema
    """
    json_str = json.dumps(data)

    df = spark.createDataFrame([(json_str,)], ["value"])
    schema = schema_of_json(lit(json_str))

    df = df.select(from_json(col("value"), schema).alias("data")).select("data.*")

    return df


# ---------------------------
# BRONZE → SILVER (CHANNELS)
# ---------------------------

def flatten_channels(df: DataFrame) -> DataFrame:
    """
    Flatten channel JSON into structured columns
    """
    df_exploded = df.select(
        explode("items").alias("item"),
        "ingestion_time"
    )

    df_flat = df_exploded.select(
        col("item.id").alias("channel_id"),
        col("item.snippet.title").alias("title"),
        col("item.statistics.subscriberCount").alias("subscribers"),
        col("item.statistics.viewCount").alias("views"),
        col("item.statistics.videoCount").alias("videos"),
        col("ingestion_time")
    )

    return df_flat


def clean_channels(df: DataFrame) -> DataFrame:
    """
    Clean and cast channel data
    """
    df_clean = df.select(
        col("channel_id"),
        col("title"),
        col("subscribers").cast("long"),
        col("views").cast("long"),
        col("videos").cast("long"),
        col("ingestion_time")
    )

    df_clean = df_clean.fillna({
        "subscribers": 0,
        "views": 0,
        "videos": 0
    })

    return df_clean


# ---------------------------
# VIDEOS TRANSFORMATION
# ---------------------------

def flatten_videos(df: DataFrame) -> DataFrame:
    """
    Flatten playlistItems response into video-level data
    """
    df_exploded = df.select(
        explode("col").alias("item") if "col" in df.columns else explode("*").alias("item")
    )

    df_flat = df_exploded.select(
        col("item.snippet.resourceId.videoId").alias("video_id"),
        col("item.snippet.title").alias("title"),
        col("item.snippet.publishedAt").alias("published_at"),
        col("item.snippet.description").alias("description"),
        col("item.snippet.channelId").alias("channel_id"),
        col("item.snippet.channelTitle").alias("channel_title")
    )

    return df_flat


def flatten_video_statistics(df: DataFrame) -> DataFrame:
    """
    Flatten video statistics (views, likes, comments)
    """
    df_exploded = df.select(
        explode("*").alias("item")
    )

    df_stats = df_exploded.select(
        col("item.id").alias("video_id"),
        col("item.statistics.viewCount").cast("long").alias("views"),
        col("item.statistics.likeCount").cast("long").alias("likes"),
        col("item.statistics.commentCount").cast("long").alias("comments")
    )

    return df_stats


def join_video_data(df_videos: DataFrame, df_stats: DataFrame) -> DataFrame:
    """
    Join video metadata with statistics
    """
    return df_videos.join(df_stats, on="video_id", how="left")


# ---------------------------
# GOLD TRANSFORMATIONS
# ---------------------------

def create_channel_analytics(df_channels: DataFrame, df_videos: DataFrame) -> DataFrame:
    """
    Create aggregated Gold layer metrics
    """
    from pyspark.sql.functions import sum, count, max

    df_agg = df_videos.groupBy("channel_id", "channel_title").agg(
        count("video_id").alias("total_videos"),
        sum("views").alias("total_views"),
        max("published_at").alias("latest_video_date")
    )

    df_final = df_agg.join(
        df_channels.select("channel_id", "subscribers"),
        on="channel_id",
        how="left"
    )

    df_final = df_final.withColumn("ingestion_time", current_timestamp())

    return df_final


# ---------------------------
#  TRANSFORM VIDEOS
# ---------------------------

    def transform_videos(spark, videos: list):
    """
    Convert playlistItems response → clean video metadata DataFrame
    """
    df = json_to_df(spark, videos)

    from pyspark.sql.functions import explode, col

    df_exploded = df.select(explode("*").alias("item"))

    df_flat = df_exploded.select(
        col("item.snippet.resourceId.videoId").alias("video_id"),
        col("item.snippet.title").alias("title"),
        col("item.snippet.publishedAt").alias("published_at"),
        col("item.snippet.description").alias("description"),
        col("item.snippet.channelId").alias("channel_id"),
        col("item.snippet.channelTitle").alias("channel_title")
    )

    return df_flat