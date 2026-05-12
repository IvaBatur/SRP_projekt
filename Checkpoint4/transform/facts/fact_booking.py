from pyspark.sql.functions import col, row_number, concat, lit, when, to_date
from pyspark.sql.window import Window

def transform_booking_fact(db_booking_df, dim_customer, dim_hotel, dim_location, dim_meal, dim_status, dim_vrijeme):

    b = db_booking_df.withColumn("month_num",
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
    ).withColumn("temp_date",
        to_date(concat(col("arrival_date_year"), lit("-"), col("month_num"), lit("-"), col("arrival_date_day_of_month"))).cast("date")
    )

    res = b.alias("b") \
        .join(dim_customer.alias("c"),
    (col("b.customer_type") == col("c.customer_type")) &
    (col("b.is_repeated_guest") == col("c.is_repeated_guest")), "left") \
        .join(dim_hotel.alias("h"), col("b.hotel") == col("h.hotel_type"), "left") \
        .join(dim_location.alias("l"), "country", "left") \
        .join(dim_meal.alias("m"), "meal", "left") \
        .join(dim_status.alias("s"), "reservation_status", "left") \
        .join(dim_vrijeme.alias("v"), col("b.temp_date") == col("v.arrival_date"), "left")

    final_fact = res.select(
        col("c.customer_tk"),
        col("h.hotel_tk"),
        col("l.location_tk"),
        col("m.meal_tk"),
        col("s.status_tk"),
        col("v.time_tk"),
        col("b.lead_time"),
        col("b.stays_in_weekend_nights"),
        col("b.stays_in_week_nights"),
        (col("b.stays_in_weekend_nights") + col("b.stays_in_week_nights")).alias("total_nights"),
        col("b.adr"),
        col("b.adults"),
        col("b.children"),
        col("b.adr").alias("average_daily_rate"),
        col("b.is_canceled"),
        col("b.total_of_special_requests"),
        
    )

    w = Window.orderBy(col("time_tk"), col("hotel_tk"))
    return final_fact.withColumn("fact_booking_tk", row_number().over(w))








