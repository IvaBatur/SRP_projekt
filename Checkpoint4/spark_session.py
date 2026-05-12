from pyspark.sql import SparkSession

def get_spark_session(app_name="Hotel_ETL_Sustav"):
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.29") \
        .getOrCreate()