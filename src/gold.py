from pyspark.sql.functions import (
    col,
    count,
    sum,
    avg,
    max,
    desc,
    current_timestamp,
    try_divide
)
from src.load import write_delta


# ---------------------------
# CHANNEL SUMMARY
# ---------------------------

def gold_channel_summary(spark, catalog: str):
    df_channels = spark.table(f"{catalog}.silver.channels")
    df_videos = spark.table(f"{catalog}.silver.videos_enriched")

    df_agg = df_videos.groupBy("channel_id", "channel_title").agg(
        count("video_id").alias("total_videos"),
        sum("views").alias("total_views"),
        avg("views").alias("avg_views_per_video"),
        max("published_at").alias("latest_video_date")
    )

    df_final = df_agg.join(
        df_channels.select("channel_id", "subscribers"),
        on="channel_id",
        how="left"
    )

    df_final = df_final.withColumn("ingestion_time", current_timestamp())

    write_delta(
        df_final,
        f"{catalog}.gold.channel_summary",
        mode="overwrite"
    )


# ---------------------------
# VIDEO PERFORMANCE
# ---------------------------

def gold_video_performance(spark, catalog: str):
    df = spark.table(f"{catalog}.silver.videos_enriched")

    df = df.withColumn(
        "engagement_rate",
        try_divide(col("likes") + col("comments"), col("views"))
    )

    df = df.withColumn("ingestion_time", current_timestamp())

    write_delta(
        df,
        f"{catalog}.gold.video_performance",
        mode="overwrite"
    )

# ---------------------------
# VIDEO PERFORMANCE BY TYPE
# ---------------------------

def gold_video_performance_by_type(spark, catalog: str):
    df = spark.table(f"{catalog}.gold.video_performance")

    df_short = df.filter(col("video_type") == "short_video")
    df_long = df.filter(col("video_type") == "long_video")

    write_delta(df_short, f"{catalog}.gold.video_performance_short", mode="overwrite")
    write_delta(df_long, f"{catalog}.gold.video_performance_long", mode="overwrite")




# ---------------------------
# TOP VIDEOS
# ---------------------------

def gold_top_videos(spark, catalog: str, top_n: int = 10):
    df = spark.table(f"{catalog}.gold.video_performance")

    df_top = df.orderBy(desc("views")).limit(top_n)

    write_delta(
        df_top,
        f"{catalog}.gold.top_videos",
        mode="overwrite"
    )


# ---------------------------
# TOP VIDEOS BY TYPE
# ---------------------------

def gold_top_videos_by_type(spark, catalog: str, top_n: int = 10):
    df = spark.table(f"{catalog}.gold.video_performance")

    df_short = df.filter(col("video_type") == "short_video") \
                 .orderBy(desc("views")) \
                 .limit(top_n)

    df_long = df.filter(col("video_type") == "long_video") \
                .orderBy(desc("views")) \
                .limit(top_n)

    write_delta(df_short, f"{catalog}.gold.top_short_videos", mode="overwrite")
    write_delta(df_long, f"{catalog}.gold.top_long_videos", mode="overwrite")


# ---------------------------
#  VIDEOS  TYPE SUMMARY
# ---------------------------

def gold_video_type_summary(spark, catalog: str):
    df = spark.table(f"{catalog}.silver.videos_enriched")

    df_summary = df.groupBy("video_type").agg(
        count("video_id").alias("total_videos"),
        sum("views").alias("total_views"),
        avg("views").alias("avg_views"),
        avg(try_divide(col("likes") + col("comments"), col("views"))).alias("avg_engagement")
    )

    df_summary = df_summary.withColumn("ingestion_time", current_timestamp())

    write_delta(
        df_summary,
        f"{catalog}.gold.video_type_summary",
        mode="overwrite"
    )



# ---------------------------
# FULL GOLD PIPELINE
# ---------------------------

def run_gold_pipeline(spark, catalog: str):
    gold_channel_summary(spark, catalog)
    gold_video_performance(spark, catalog)
    gold_top_videos(spark, catalog)