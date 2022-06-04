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
#Get min
#min_temperatures = year_temperature.reduceByKey(lambda a,b: a if a<=b else b)
#min_temperatures = min_temperatures.sortBy(ascending = False, keyfunc=lambda k: k[1])
print(max_temperatures.collect())
# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
max_temperatures.saveAsTextFile("BDA/output")



### lowest temperature
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
min_temperatures = year_temperature.reduceByKey(lambda a,b: a if a<=b else b)
min_temperatures = min_temperatures.sortBy(ascending = False, keyfunc=lambda k: k[1])

print(min_temperatures.collect())

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
min_temperatures.saveAsTextFile("BDA/output")



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
count_temperatures=count_temperatures.sortBy(ascending = False, keyfunc=lambda k: k[1])
print(count_temperatures.collect())

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
year_temperature = year_temperature.reduceByKey(max)
year_temperature = year_temperature.map(lambda x: (x[0][0],1))
count_temperatures=year_temperature.reduceByKey(lambda a,b: a+b)
count_temperatures=count_temperatures.sortBy(ascending = False, keyfunc=lambda k: k[1])

#print(max_temperatures.collect())

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
count_temperatures.saveAsTextFile("BDA/output")




### 3)
from pyspark import SparkContext

sc = SparkContext(appName = "exercise 1")
# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

# (key, value) = ((year-month-date,station),temperature)
date_temperature = lines.map(lambda x: ((x[1][0:10],x[0]),float(x[3])))

#filter
date_temperature = date_temperature.filter(lambda x: int(x[0][0][0:4])>=1960 and int(x[0][0][0:4])<=2014)

#Get max and min 
#(key, value) = ((year-month-date,station),(max,min))
max_temperatures = date_temperature.reduceByKey(max)
min_temperatures = date_temperature.reduceByKey(min)
maxmin_temperatures = max_temperatures.join(min_temperatures)
#reduce
#(key, value) = ((year-month,station),(max,min,1))
month_temperature = maxmin_temperatures.map(lambda x:((x[0][0][0:7],x[0][1]),(x[1][0],x[1][1],int(1))))
ave_temperature = month_temperature.reduceByKey(lambda a,b: (a[0]+b[0],a[1]+b[1],a[2]+b[2]))
ave_temperature = ave_temperature.map(lambda x: (x[0],(x[1][0]+x[1][1])/(x[1][2]*2))).sortBy(ascending = False, keyfunc=lambda k: k[1])

#print(ave_temperature.collect())

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
ave_temperature.saveAsTextFile("BDA/output")




### 4)
from pyspark import SparkContext

sc = SparkContext(appName = "exercise 1")
# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
tem_lines = temperature_file.map(lambda line: line.split(";"))
precipitation_file = sc.textFile("BDA/input/precipitation-readings.csv")
pre_lines = precipitation_file.map(lambda line: line.split(";"))

# (key, value) = (station,temperature)
station_temp = tem_lines.map(lambda x: (x[0],float(x[3])))
# (key, value) = (station,precipitation)
station_pre = pre_lines.map(lambda x: (x[0],float(x[3])))

#Get max, filter
max_temp = station_temp.reduceByKey(lambda a,b: a if a>=b else b)
max_temp = max_temp.filter(lambda x: x[1]>=25 and x[1]<=30)
max_pre = station_pre.reduceByKey(lambda a,b: a if a>=b else b)
max_pre = max_pre.filter(lambda x: x[1]>=10 and x[1]<=20)

#merge
station_max = max_temp.join(max_pre)

#print(station_max.collect())

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
station_max.saveAsTextFile("BDA/output")




### 5)
from pyspark import SparkContext

sc = SparkContext(appName = "exercise 1")
# This path is to the file on hdfs
pre_file = sc.textFile("BDA/input/precipitation-readings.csv")
pre_lines = pre_file.map(lambda line: line.split(";"))
o_file = sc.textFile("BDA/input/stations-Ostergotland.csv")
o_lines = o_file.map(lambda line: line.split(";"))

station_list = o_lines.map(lambda x: x[0]).collect()
b_station_list = sc.broadcast(station_list)
# (key, value) = ((year-month,station),precipitation)
pre_all = pre_lines.map(lambda x: ((x[1][0:7],x[0]),float(x[3])))
pre_o = pre_all.filter(lambda x: int(x[0][0][0:4])>=1993 and int(x[0][0][0:4])<=2016 and x[0][1] in b_station_list.value)

#Get monthly precipitation
# (key,value)=((year-month,station),month_pre)
pre_o_month = pre_o.reduceByKey(lambda a,b: a+b)
# (key,value)=((year-month),(month_pre,1))
pre_o_monthavg = pre_o_month.map(lambda x: (x[0][0],(x[1],1)))
pre_o_monthavg = pre_o_monthavg.reduceByKey(lambda a,b: (a[0]+b[0],a[1]+b[1]))
pre_o = pre_o_monthavg.map(lambda x : (x[0],x[1][0]/x[1][1])).sortBy(ascending = False, keyfunc=lambda k: k[1])

print(pre_o.collect())

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
pre_o.saveAsTextFile("BDA/output")
