import os
from utils.spark_session import create_spark_session
from config.schema import schema

def ingest_data(file_path):
    spark = create_spark_session("DataIngestionApp")


    sales_data = spark.read.format("csv") \
        .option("header", "true") \
        .schema(schema) \
        .load(file_path)
    sales_data.show(10)
    
    sales_data.printSchema()
if __name__ == "__main__":
    file_path = os.path.join("data","raw","sales_data.csv")
    ingest_data(file_path)
