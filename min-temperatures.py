from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)

lines = sc.textFile("file:///d:/ws/git/pySpark/1800.csv")
parsedLines = lines.map(parseLine)
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])

stationTemps = minTemps.map(lambda x: (x[0], x[2]))

minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y))
minresults = minTemps.collect()

maxTemps = parsedLines.filter(lambda x: "TMAX" in x[1])

stationTemps = maxTemps.map(lambda x: (x[0], x[2]))
maxFTemps = stationTemps.reduceByKey(lambda x, y: max(x,y))
maxFresults = maxFTemps.collect()

#print the results

print("\nmin")
for result in minresults:
    print(result[0] + "\t{:.2f}F".format(result[1]))

print("\nmax")
for result in maxFresults:
    print(result[0] + "\t{:.2f}F".format(result[1]))    
