from utils.spark_session import create_spark_session
from scripts.ingest import ingest_data
import os 
from pyspark.sql.functions import col,round,countDistinct





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
    total_sales_person = df.agg(countDistinct("Sales_Person").alias("Total_Sales_Persons"))

    # Return all the transformed dataframes
    return revenue_per_box, total_revenue_per_country, total_revenue, total_boxes_shipped, total_sales_person


# save the transformed dataframes to csv files
def save_transformed_dataframes(df, filename):
    output_path = os.path.join("data", "transforms", filename)
    df.coalesce(1).write.mode("overwrite").csv(output_path, header=True)

if __name__ == "__main__":
    file_path = os.path.join(
        "data","raw","sales_data.csv"
    )

    # Ingest the data
    df = ingest_data(file_path)
    # Transform the data
    revenue_per_box, total_revenue_per_country, total_revenue, total_boxes_shipped ,total_sales_person = tranform_sales_data(df)
    

    # Save the transformed dataframes
    save_transformed_dataframes(revenue_per_box, "revenue_per_box")
    save_transformed_dataframes(total_revenue_per_country, "total_revenue_per_country")
    save_transformed_dataframes(total_revenue, "total_revenue")
    save_transformed_dataframes(total_boxes_shipped, "total_boxes_shipped")
    save_transformed_dataframes(total_sales_person, "total_sales_person")


    # tranforms view in console
    revenue_per_box.show()
    total_revenue_per_country.show()
    total_revenue.show()
    total_boxes_shipped.show()
    total_sales_person.show()
