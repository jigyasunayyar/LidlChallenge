from pyspark import SparkConf


def get_spark_config(app_name: str = "Default Spark App") -> SparkConf:
    config = SparkConf().setAppName(app_name) \
        .set("spark.dynamicAllocation.initialExecutors", "1") \
        .set("spark.dynamicAllocation.minExecutors", "1")\
        .set("spark.dynamicAllocation.maxExecutors", "10")\
        .set("spark.dynamicAllocation.enabled", "true")\
        .set("spark.sql.shuffle.partitions", "6")\
        .set("spark.driver.memory", "512m")\
        .set("spark.driver.cores", "1")\
        .set("spark.executor.memory", "512m")\
        .set("spark.executor.cores", "2")\
        .set("spark.sql.files.maxPartitionBytes", "200000") \
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    return config
