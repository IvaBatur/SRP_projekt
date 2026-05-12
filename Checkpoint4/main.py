import os
import sys
sys.path.insert(0, r"C:\SRP_projekt-main\Checkpoint4")

os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17"
os.environ["SPARK_HOME"] = r"C:\spark"
os.environ["HADOOP_HOME"] = r"C:\spark"

os.environ["PATH"] = (
    os.path.join(os.environ["JAVA_HOME"], "bin") + os.pathsep +
    os.path.join(os.environ["SPARK_HOME"], "bin") + os.pathsep +
    os.environ.get("PATH", "")
)

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from spark_session import get_spark_session
from extract.extract_mysql import extract_all_tables
from extract.extract_csv import extract_from_csv
from transform.pipeline import run_transformations
from load.run_loading import write_spark_df_to_mysql

def main():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    spark.catalog.clearCache()

    print("🚀 Starting data extraction")
    mysql_df = extract_all_tables()

    path_booking = r"C:\SRP_projekt-main\checkpoint2\hotel_bookings2.csv"

    csv_df = {
        "csv_booking": extract_from_csv(path_booking),
    }

    merged_df = {**mysql_df, **csv_df}
    print("✅ Data extraction completed")

    print("🚀 Starting data transformation")
    load_ready_dict = run_transformations(merged_df, spark)
    print("✅ Data transformation completed")

    print("🚀 Starting data loading")
    for table_name, df in load_ready_dict.items():
        write_spark_df_to_mysql(df, table_name)
    print("👏 Data loading completed")

if __name__ == "__main__":
    main()