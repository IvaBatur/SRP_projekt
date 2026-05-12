from pyspark.sql import DataFrame
import pymysql

def truncate_table(table_name: str):
    conn = pymysql.connect(
        host="127.0.0.1",
        user="root",
        password="root",
        database="dw"
    )
    try:
        with conn.cursor() as cursor:
            cursor.execute("SET FOREIGN_KEY_CHECKS=0")
            cursor.execute(f"TRUNCATE TABLE `{table_name}`")
            cursor.execute("SET FOREIGN_KEY_CHECKS=1")
        conn.commit()
    finally:
        conn.close()

def write_spark_df_to_mysql(spark_df: DataFrame, table_name: str, mode: str = "append"):
    truncate_table(table_name)
    
    jdbc_url = "jdbc:mysql://127.0.0.1:3306/dw?useSSL=false&allowPublicKeyRetrieval=true"
    connection_properties = {
        "user": "root",
        "password": "root",
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    print(f"Writing to table `{table_name}` with mode `{mode}`...")
    spark_df.write.jdbc(
        url=jdbc_url,
        table=table_name,
        mode=mode,
        properties=connection_properties
    )
    print(f"Done writing to `{table_name}`.")