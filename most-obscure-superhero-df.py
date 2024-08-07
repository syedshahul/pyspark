from pyspark.sql import SparkSession, functions as func, types as sparkTypes
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

spark = SparkSession.builder.appName("MostObscureSuperhero").getOrCreate()

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])

names = spark.read.schema(schema).option("sep", " ").csv("source-data/Marvel_Names.txt")
lines = spark.read.text("source-data/Marvel_Graph.txt")

graphConnections = lines.withColumn("hero1", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("hero1").agg(func.sum("connections").alias("connections"))

minConnectionCount = graphConnections.agg(func.min("connections")).first()[0]

# mostObscure = graphConnections.filter(func.col("connections") == minConnectionCount) \
#     .groupBy("hero1").count().orderBy(func.desc("count"))

nameGraph = graphConnections.join(names, graphConnections.hero1 == names.id, "inner")\
    .filter(graphConnections.connections==minConnectionCount)\
    .select(graphConnections.hero1, names.name, graphConnections.connections).orderBy(graphConnections.connections)
print("mostObscureHeros")
nameGraph.show(nameGraph.count())

spark.stop()