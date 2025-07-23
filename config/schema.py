from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

schema = StructType([
    StructField("Sales_Person", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("Product", StringType(), True),
    StructField("Date", StringType(), True),
    StructField("Amount", FloatType(), True),
    StructField("Boxes_Shipped", IntegerType(), True)
])