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
    total_revenue_per_country = df.groupBy("Country")\
           .sum("Amount")\
           .withColumnRenamed("sum(Amount)", "Total_Revenue")\
           .withColumn("Total_Revenue", round(col("Total_Revenue"), 2))\
           .orderBy("Total_Revenue")

    # total revenue
    total_revenue = df.agg({"Amount": "sum"})\
        .withColumnRenamed("sum(Amount)", "Total_Revenue")\
        .withColumn("Total_Revenue", round(col("Total_Revenue"), 2))
    
    # number of boxes shipped
    total_boxes_shipped = df.agg({"Boxes_Shipped": "sum"})\
        .withColumnRenamed("sum(Boxes_Shipped)", "Total_Boxes_Shipped")
    

    # number of sales person
    total_sales_person = df.agg({"Sales_Person": "count"})\
        .withColumnRenamed("count(Sales_Person)", "Total_Sales_Persons")\
        .withColumn("Total_Sales_Persons", round(col("Total_Sales_Persons"), 2))
    
    # Return all the transformed dataframes
    return revenue_per_box, total_revenue_per_country, total_revenue, total_boxes_shipped, total_sales_person

if __name__ == "__main__":
    file_path = os.path.join(
        "data","raw","sales_data.csv"
    )

    # Ingest the data
    df = ingest_data(file_path)
    revenue_per_box, total_revenue_per_country, total_revenue, total_boxes_shipped = tranform_sales_data(df)
    revenue_per_box.show()
    total_revenue_per_country.show()
    total_revenue.show()
    total_boxes_shipped.show()
