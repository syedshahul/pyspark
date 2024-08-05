from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, round, lit,format_number, concat
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

schema = StructType([ \
    StructField("stationID", StringType(), True), \
    StructField("date", IntegerType(), True), \
    StructField("measure_type", StringType(), True), \
    StructField("temperature", FloatType(), True)])

# Create a SparkSession
spark = SparkSession.builder.appName("TemperatureConverter").getOrCreate()

# Read the CSV file into a DataFrame
df = spark.read.schema(schema).csv("file:///D:/ws/git/pySpark/source-data/1800.csv", header=True, inferSchema=True)

# Define the conversion formula
def celsius_to_fahrenheit(celsius):
    return round(0.1 *(celsius * 9 / 5) + 32,2)

# Create a new column with the converted temperature
df = df.withColumn("fahrenheit_temp", when(col("temperature").isNotNull(),
                                           celsius_to_fahrenheit(col("temperature"))).otherwise(None))

# Show the DataFrame with the converted temperature
formatF = df.withColumn("format_F", concat(format_number(col("fahrenheit_temp"), 2), lit("F")))

formatF.show()
