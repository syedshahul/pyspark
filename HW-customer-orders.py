from pyspark import SparkConf, SparkContext
from operator import add

def getCustomerDetails(line):
    record = line.split(',')
    customerID = int(record[0])
    productID = record[1]
    amountSpent = float(record[2])
    return (customerID, amountSpent)
    
conf = SparkConf().setMaster("local").setAppName("CustomerOrders")
sc = SparkContext(conf=conf)

customerOrders = sc.textFile("file:///d:/ws/git/pySpark/customer-orders.csv")

data = customerOrders.map(getCustomerDetails)
groupData = data.mapValues(lambda x: (x,1))
groupDataSum = groupData.reduceByKey(lambda x,y: (round(x[0]+y[0],2),x[1]+y[1]))
#sortBy Value x[1]
sortedData = groupDataSum.sortBy(lambda x: x[1])
#soryBy Key
#sortedData =sorted(groupDataSum.collect())
sortedData =sortedData.collect()

for customer in sortedData:
    print(f"Customer ID: {customer[0]}, Total Spent: {customer[1][0]}")


# Another approach to soryBy Values
totalByCustomer = data.reduceByKey(lambda x,y: round(x+y,2))
flipped =totalByCustomer.map(lambda x : (x[1],x[0]))
#print(totalByCustomer.collect())
sortedByValue = flipped.sortByKey()
results = sortedByValue.collect()
for result in results:
    print(result)