from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg,round, sum

spark = SparkSession.builder.appName("GroupFriends").getOrCreate()

friends = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("source-data/fakefriends-header.csv")

friends.printSchema()

ageGroup = friends.groupBy("userID")
print("Group By userID and sum friends \n")   
ageGroup.\
    agg(sum("friends").alias("sum_of_friends")).orderBy("userID").show(10)

friendsByAge = friends.select("age","friends")
print("Avg By Friends\n")   
friendsByAge.groupBy("age")\
    .agg(avg("friends").alias("avg_friends")).show()

print("Avg By Friends sory by age \n")    
friendsByAge.groupBy("age")\
    .agg(round(avg("friends"),2).alias("avg_friends_sortBy_age")).sort("age").show()

print("Aggregrate By Friends\n")
friendsByAge.groupBy("age")\
    .agg(round(avg("friends"),2).alias("agg_friends_sortBy_age")).sort("age").show()

spark.stop()