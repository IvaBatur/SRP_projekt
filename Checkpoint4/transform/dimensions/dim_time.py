from pyspark.sql.functions import col, row_number, to_date, concat, lit, when, quarter, weekofyear, date_format
from pyspark.sql.window import Window

def transform_time_dim(csv_booking_df):
    df = csv_booking_df.select(
        "arrival_date_year",
        "arrival_date_month",
        "arrival_date_day_of_month"
    ).distinct()

    df = df.withColumn("month_num",
        when(col("arrival_date_month") == "January", "01")
        .when(col("arrival_date_month") == "February", "02")
        .when(col("arrival_date_month") == "March", "03")
        .when(col("arrival_date_month") == "April", "04")
        .when(col("arrival_date_month") == "May", "05")
        .when(col("arrival_date_month") == "June", "06")
        .when(col("arrival_date_month") == "July", "07")
        .when(col("arrival_date_month") == "August", "08")
        .when(col("arrival_date_month") == "September", "09")
        .when(col("arrival_date_month") == "October", "10")
        .when(col("arrival_date_month") == "November", "11")
        .when(col("arrival_date_month") == "December", "12")
    ).withColumn("arrival_date",
        to_date(concat(col("arrival_date_year"), lit("-"), col("month_num"), lit("-"), col("arrival_date_day_of_month")))
    )

    dim_time = df.select(
        col("arrival_date"),
        col("arrival_date_year").alias("year"),
        col("arrival_date_month").alias("month_name"),
        col("month_num").alias("month_number"),
        col("arrival_date_day_of_month").alias("day"),
        quarter("arrival_date").alias("quarter"),
        weekofyear("arrival_date").alias("week_number"),
        when(date_format(col("arrival_date"), "E").isin("Sat", "Sun"), 1).otherwise(0).alias("is_weekend")
    )

    w = Window.orderBy("arrival_date")
    return dim_time.withColumn("time_tk", row_number().over(w))