from utils.spark_session import create_spark_session
from scripts.ingest import ingest_data
import os 
from pyspark.sql.functions import col,round





spark = create_spark_session("CosmeticTransformation")


def tranform_sales_data(df):

    # Calculate Revenue Per Box

    revenue_per_box = df.select("Country", "Amount", "Boxes_Shipped")\
    .withColumn(
        "Revenue_Per_Box",
        round(col("Amount") / col("Boxes_Shipped"), 2)
    )


    # total_revenue per country
    total_revenue = df.groupBy("Country")\
           .sum("Amount")\
           .withColumnRenamed("sum(Amount)", "Total_Revenue")\
           .orderBy("Total_Revenue")

    return revenue_per_box, total_revenue

if __name__ == "__main__":
    file_path = os.path.join(
        "data","raw","sales_data.csv"
    )

    # Ingest the data
    df = ingest_data(file_path)
    revenue_per_box, total_revenue = tranform_sales_data(df)
    revenue_per_box.show()
    total_revenue.show()

    
