from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import col, sum, round, count
from pyspark.sql.types import *


spark = SparkSession.builder.appName("CustomerOrders-DF").getOrCreate()

schema = StructType([
    StructField("customerID", IntegerType(), True),
    StructField("productID", StringType(), True),
    StructField("amountSpent", FloatType(), True)
])

customerOrders = spark.read.schema(schema).csv("file:///d:/ws/git/pySpark/source-data/customer-orders.csv")

groupByCustomer = customerOrders.select("customerID", "amountSpent").groupBy("customerID")

totalSpent = groupByCustomer.agg(round(sum("amountSpent"), 2).alias("total_spent")).orderBy("total_spent")
totalSpent.show(totalSpent.count())
spark.stop()