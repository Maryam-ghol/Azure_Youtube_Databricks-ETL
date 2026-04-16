from src.transform import (
    flatten_channels,
    clean_channels,
    flatten_videos,
    flatten_video_statistics,
    join_video_data
)
from src.load import write_delta

from pyspark.sql.window import Window
from pyspark.sql.functions import (
    row_number,
    col,
    regexp_extract,
    when
)


# ---------------------------
# CHANNELS
# ---------------------------

def silver_channels(spark, catalog: str):
    df_raw = spark.table(f"{catalog}.bronze.channels_raw")

    df = flatten_channels(df_raw)
    df = clean_channels(df)

    window = Window.partitionBy("channel_id").orderBy(col("ingestion_time").desc())

    df = df.withColumn("rn", row_number().over(window)) \
           .filter(col("rn") == 1) \
           .drop("rn")

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
    df = deduplicate_videos(df)

    write_delta(
        df,
        f"{catalog}.silver.videos",
        mode="overwrite"
    )


# ---------------------------
# VIDEO STATS (WITH DURATION)
# ---------------------------

def silver_video_stats(spark, catalog: str):
    df_raw = spark.table(f"{catalog}.bronze.video_stats_raw")

    df = flatten_video_statistics(df_raw)
    df = deduplicate_videos(df)

    # ---------------------------
    # Extract duration (ISO 8601 → seconds)
    # ---------------------------

    df = df.withColumn(
        "minutes",
        regexp_extract("duration", "PT(\\d+)M", 1).try_cast("int")
    ).withColumn(
        "seconds",
        regexp_extract("duration", "PT\\d*M(\\d+)S", 1).try_cast("int")
    )

    df = df.fillna({"minutes": 0, "seconds": 0})

    df = df.withColumn(
        "duration_sec",
        col("minutes") * 60 + col("seconds")
    )

    # ---------------------------
    # Classification (3 min = 180 sec)
    # ---------------------------

    df = df.withColumn(
        "video_type",
       when(col("duration_sec") < 180, "short_video")
       .otherwise("long_video")
    )

    write_delta(
        df,
        f"{catalog}.silver.video_stats",
        mode="overwrite"
    )
   


# ---------------------------
#for duplicated videos
# ---------------------------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col
from pyspark.sql.functions import greatest
from pyspark.sql.functions import lower, trim

def deduplicate_videos(df):
    

    if "stats_ingestion_time" in df.columns:
        df = df.withColumn(
            "latest_time",
            greatest(col("ingestion_time"), col("stats_ingestion_time"))
        )
    else:
        df = df.withColumn("latest_time", col("ingestion_time"))

    window = Window.partitionBy("video_id").orderBy(col("latest_time").desc())

    df = df.withColumn("rn", row_number().over(window)) \
        .filter(col("rn") == 1) \
        .drop("rn", "latest_time")
    
    

    if "video_type" in df.columns:
        df = df.withColumn("video_type", lower(trim(col("video_type"))))

    return df

# ---------------------------
# ENRICHED VIDEOS
# ---------------------------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

def silver_videos_enriched(spark, catalog: str):
    df_videos = spark.table(f"{catalog}.silver.videos")

    df_stats = spark.table(f"{catalog}.silver.video_stats") \
        .withColumnRenamed("ingestion_time", "stats_ingestion_time")

    df = join_video_data(df_videos, df_stats)

    from pyspark.sql.functions import greatest

    df = df.withColumn(
        "latest_time",
        greatest(col("ingestion_time"), col("stats_ingestion_time"))
    )

    window = Window.partitionBy("video_id").orderBy(col("latest_time").desc())

    df = df.withColumn("rn", row_number().over(window)) \
           .filter(col("rn") == 1) \
           .drop("rn", "latest_time")

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