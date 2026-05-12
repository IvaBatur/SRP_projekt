from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

def transform_hotel_dim(db_hotel_df):
    df = db_hotel_df.select(col("hotel")).distinct()
    w = Window.orderBy("hotel")
    df = df.withColumn("hotel_tk", row_number().over(w))

    df = df.withColumn("hotel_type", col("hotel"))

    return df.select("hotel_tk", "hotel", "hotel_type")