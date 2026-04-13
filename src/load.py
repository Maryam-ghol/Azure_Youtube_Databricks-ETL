from pyspark.sql import DataFrame


# ---------------------------
# GENERIC DELTA WRITER
# ---------------------------

def write_delta(
    df: DataFrame,
    table_name: str,
    mode: str = "overwrite"
):
    """
    Write DataFrame to Delta table
    """
    df.write \
        .format("delta") \
        .mode(mode) \
        .option("overwriteSchema", "true") \
        .saveAsTable(table_name)


# ---------------------------
# PARTITIONED WRITE (OPTIONAL)
# ---------------------------

def write_partitioned(
    df: DataFrame,
    table_name: str,
    partition_cols: list,
    mode: str = "overwrite"
):
    """
    Write partitioned Delta table
    """
    df.write \
        .format("delta") \
        .mode(mode) \
        .partitionBy(partition_cols) \
        .saveAsTable(table_name)


# ---------------------------
# UPSERT (MERGE) FOR INCREMENTAL LOAD
# ---------------------------

def merge_delta(
    spark,
    df: DataFrame,
    table_name: str,
    condition: str
):
    """
    Perform MERGE INTO (upsert) for incremental pipelines
    """
    df.createOrReplaceTempView("source_temp")

    merge_sql = f"""
        MERGE INTO {table_name} AS target
        USING source_temp AS source
        ON {condition}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """

    spark.sql(merge_sql)


# ---------------------------
# TABLE CREATION HELPER
# ---------------------------

def create_table_if_not_exists(
    spark,
    table_name: str,
    schema: str
):
    """
    Create table if not exists using SQL schema
    """
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        ({schema})
        USING DELTA
    """)