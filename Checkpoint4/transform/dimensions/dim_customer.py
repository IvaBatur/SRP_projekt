from pyspark.sql.functions import col, row_number, lit
from pyspark.sql.window import Window

def transform_customer_dim(db_customer_df):
    df = db_customer_df.select(
        col("customer_type"),
        col("is_repeated_guest")
    ).distinct()


    w = Window.orderBy("customer_type", "is_repeated_guest")
    
    df = df.withColumn("customer_tk", row_number().over(w)) \
        .withColumn("version", lit(1)) \
        .withColumn("date_from", lit("2026-01-01").cast("date")) \
        .withColumn("date_to", lit("9999-12-31").cast("date"))


    return df.select(
        "customer_tk",
        "customer_type",
        "is_repeated_guest",
        "version",
        "date_from",
        "date_to"
    )