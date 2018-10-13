from functools import lru_cache

from pyspark.sql import SparkSession


@lru_cache(maxsize=None)
def get_or_create_spark_session():
    return SparkSession.builder.master("local[*]").appName("Spark Demo").getOrCreate()
