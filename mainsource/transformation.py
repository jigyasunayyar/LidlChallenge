from pyspark.sql import (functions as func, DataFrame, Window)


def get_time_windows(df: DataFrame):
    """
    creates custom time windows of 10 min and 1 min.
    :param df:
    :return:
    """
    min_time = df.agg(func.min("time").alias("min_time")).collect()[0]["min_time"]
    df = df.withColumn("min_time", func.lit(min_time))
    df = df.withColumn("time_diff", func.col("time").cast("long") - func.col("min_time").cast("long"))
    # df = df.withColumn("division", func.col("time_diff")/600)
    df = df.withColumn("time_per_10_min", func.floor(func.col("time_diff") / 600) + 1)
    df = df.withColumn("time_per_1_min", func.floor(func.col("time_diff") / 60))

    wind_spec = Window.partitionBy(func.col("time_per_10_min")).orderBy(func.col("time_per_1_min"))
    df = df.withColumn("time_per_1_min", func.dense_rank().over(wind_spec))

    return df


def get_per_min_agg(df: DataFrame):
    """
    aggregates action per minute within 10 min window
    :param df:
    :return:
    """
    agg_df = df.groupby("time_per_10_min", "time_per_1_min").pivot("action").agg(func.count(func.col("action")))

    return agg_df


def get_per_window_agg(df: DataFrame):
    """
    aggregates action per 10 min window
    :param df:
    :return:
    """
    agg_df = df.groupby("time_per_10_min").agg(func.round(func.avg(func.col("close")), 2).alias("avg_close")
                                               , func.round(func.avg(func.col("open")), 2).alias("avg_open"))
    return agg_df
