from pyspark.sql.functions import row_number, col, when, lit
from pyspark.sql.window import Window

def transform_status_dim(status_df):
    if "status_description" not in status_df.columns:
        temp_df = status_df.select(col("is_canceled").alias("is_canceled_status")).distinct()
        temp_df = temp_df.withColumn("reservation_status",
            when(col("is_canceled_status") == 1, "Canceled").otherwise("Check-out"))
        temp_df = temp_df.withColumn("status_description", col("reservation_status"))
    else:
        temp_df = status_df.withColumnRenamed("is_canceled", "is_canceled_status")

    w = Window.orderBy("reservation_status")
    final_df = temp_df.withColumn("status_tk", row_number().over(w))

    return final_df.select(
        "status_tk",
        "reservation_status",
        "status_description",
        "is_canceled_status"
    )