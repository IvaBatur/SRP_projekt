from pyspark.sql.functions import col, trim, initcap, row_number
from pyspark.sql.window import Window

def transform_location_dim(csv_booking_df, spark):

    location_df = csv_booking_df.select(
        initcap(trim(col("country"))).alias("country"),
        col("region"),
        col("continent")
    ).distinct()


    window = Window.orderBy("country")
    return location_df.withColumn("location_tk", row_number().over(window)) \
        .select("location_tk", "country", "region", "continent")