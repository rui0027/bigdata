### 1)

from pyspark import SparkContext

sc = SparkContext(appName = "exercise 1")
# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

# (key, value) = (year,temperature)
year_temperature = lines.map(lambda x: (x[1][0:4],float(x[3])))

#filter
year_temperature = year_temperature.filter(lambda x: int(x[0])>=1950 and int(x[0])<=2014)

#Get max
max_temperatures = year_temperature.reduceByKey(lambda a,b: a if a>=b else b)
max_temperatures = max_temperatures.sortBy(ascending = False, keyfunc=lambda k: k[1])

#print(max_temperatures.collect())

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
max_temperatures.saveAsTextFile("BDA/output")




### 2)

from pyspark import SparkContext

sc = SparkContext(appName = "exercise 1")
# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

# (key, value) = (year-month,temperature)
year_temperature = lines.map(lambda x: (x[1][0:7],float(x[3])))

#filter
year_temperature = year_temperature.filter(lambda x: int(x[0][0:4])>=1950 and int(x[0][0:4])<=2014 and x[1]>10)

#map
year_temperature = year_temperature.map(lambda x: (x[0],1))

#count
count_temperatures=year_temperature.reduceByKey(lambda a,b: a+b)

#print(max_temperatures.collect())

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
count_temperatures.saveAsTextFile("BDA/output")




### Repeat the exercise,this time taking only distinct readings from each station.
from pyspark import SparkContext

sc = SparkContext(appName = "exercise 1")
# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

# (key, value) = ((year-month,station),temperature)
year_temperature = lines.map(lambda x: ((x[1][0:7],x[0]),float(x[3])))

#filter
year_temperature = year_temperature.filter(lambda x: int(x[0][0][0:4])>=1950 and int(x[0][0][0:4])<=2014 and x[1]>10)

#count
year_temperature = year_temperature.reduceByKey(lambda a,b: 1)
count_temperatures=year_temperature.reduceByKey(lambda a,b: a+b)

#print(max_temperatures.collect())

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
count_temperatures.saveAsTextFile("BDA/output")




### 3)

