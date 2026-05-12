from pyspark.sql.functions import col, row_number, when, lit
from pyspark.sql.window import Window

def transform_meal_dim(db_meal_df):
    
    df = db_meal_df.select("meal").distinct()

    df = df.withColumn("meal_description",
        when(col("meal") == "BB", "Bed & Breakfast")
        .when(col("meal") == "HB", "Half Board")
        .when(col("meal") == "FB", "Full Board")
        .when(col("meal") == "SC", "Self Catering")
        .when(col("meal") == "Undefined", "No Meal Package")
        .otherwise("Other")
    )

    df = df.withColumn("is_package",
        when(col("meal").isin("BB", "HB", "FB"), 1).otherwise(0)
    )

    w = Window.orderBy("meal")
    df = df.withColumn("meal_tk", row_number().over(w))

    return df.select("meal_tk", "meal", "meal_description", "is_package")