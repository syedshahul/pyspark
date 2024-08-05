from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

rdd = sc.parallelize([(33,385), (26,2), (55,221), (59,30), (33,5), (40,465), (68,21), (59,318), (37,22)])

aff = rdd.mapValues(lambda x: (x,1))
print("\n mapValues: \n", aff.collect())

rk = aff.reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))
print("\n reduceByKey: \n", rk.collect())

avga = rk.mapValues(lambda x: round(x[0]/x[1]))

print("\n Collecting average")
results = sorted(avga.collect())
for result in results:
    print(result)