### 1)
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName = "exercise 2")
sqlContext = SQLContext(sc)
# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

tempReadingsRow = lines.map(lambda p: (p[0], p[1], int(p[1].split("-")[0]),int(p[1].split("-")[1]), p[2], float(p[3]), p[4] ))
# Specifying the schema programatically and registering the DataFrame as a table
tempReadingsString = ["station", "date", "year", "month", "time", "value","quality"]
# Apply the schema to the RDD.
tem_df = sqlContext.createDataFrame(tempReadingsRow,tempReadingsString)
# Register the DataFrame as a table.
tem_df.registerTempTable("tempReadingsTable")
# Can run queries now
#year_max =tem_df.filter((tem_df['year']>=1950) & (tem_df['year']<=2014)).groupBy('year','station').agg(F.max('value').alias('year_max_temperature')).orderBy('year_max_temperature',ascending=0)
year_min =tem_df.filter((tem_df['year']>=1950) & (tem_df['year']<=2014)).groupBy('year','station').agg(F.min('value').alias('year_min_temperature')).orderBy('year_min_temperature',ascending=0)

#year_max.select('year','station','year_max_temperature').rdd.saveAsTextFile("BDA/output")
year_min.select('year','station','year_min_temperature').rdd.saveAsTextFile("BDA/output")


### 2)
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName = "exercise 2")
sqlContext = SQLContext(sc)
# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

tempReadingsRow = lines.map(lambda p: (p[0], p[1], int(p[1].split("-")[0]),int(p[1].split("-")[1]), p[2], float(p[3]), p[4] ))
# Specifying the schema programatically and registering the DataFrame as a table
tempReadingsString = ["station", "date", "year", "month", "time", "value","quality"]
# Apply the schema to the RDD.
tem_df = sqlContext.createDataFrame(tempReadingsRow,tempReadingsString)
# Register the DataFrame as a table.
tem_df.registerTempTable("tempReadingsTable")

#count_readings =tem_df.filter((tem_df['year']>=1950) & (tem_df['year']<=2014) & (tem_df['value']>10)).groupBy('year','month').agg(F.count('value').alias('count_readings')).orderBy('count_readings',ascending=0)
distinct_count_readings = tem_df.filter((tem_df['year']>=1950) & (tem_df['year']<=2014) & (tem_df['value']>10)).groupBy('year','month').agg(F.countDistinct('station').alias('distinct_count_readings')).orderBy('distinct_count_readings',ascending=0)



#count_readings.rdd.saveAsTextFile("BDA/output")
distinct_count_readings.rdd.saveAsTextFile("BDA/output")



### 3)
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName = "exercise 2")
sqlContext = SQLContext(sc)
# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

tempReadingsRow = lines.map(lambda p: (p[0], p[1], int(p[1].split("-")[0]),int(p[1].split("-")[1]), p[2], float(p[3]), p[4] ))
# Specifying the schema programatically and registering the DataFrame as a table
tempReadingsString = ["station", "date", "year", "month", "time", "value","quality"]
# Apply the schema to the RDD.
tem_df = sqlContext.createDataFrame(tempReadingsRow,tempReadingsString)
# Register the DataFrame as a table.
tem_df.registerTempTable("tempReadingsTable")
# Can run queries now
avg_monthly = tem_df.filter((tem_df['year']>=1960) & (tem_df['year']<=2014)).groupBy('year','month','station').agg(F.avg('value').alias('monthly_avg_temp')).orderBy('monthly_avg_temp',ascending=0)

avg_monthly.rdd.saveAsTextFile("BDA/output")



### 4)
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName = "exercise 2")
sqlContext = SQLContext(sc)
# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
precipitation_file = sc.textFile("BDA/input/precipitation-readings.csv")
temp_lines = temperature_file.map(lambda line: line.split(";"))
pre_lines = precipitation_file.map(lambda line: line.split(";"))

tempReadingsRow = temp_lines.map(lambda p: (p[0], p[1], int(p[1].split("-")[0]),int(p[1].split("-")[1]), p[2], float(p[3]), p[4] ))
preReadingsRow = pre_lines.map(lambda p: (p[0], p[1], int(p[1].split("-")[0]),int(p[1].split("-")[1]), p[2], float(p[3]), p[4] ))
# Specifying the schema programatically and registering the DataFrame as a table
tempReadingsString = ["station", "date", "year", "month", "time", "temp_value","quality"]
preReadingsString = ["station", "date", "year", "month", "time", "pre_value","quality"]
# Apply the schema to the RDD.
tem_df = sqlContext.createDataFrame(tempReadingsRow,tempReadingsString)
pre_df = sqlContext.createDataFrame(preReadingsRow,preReadingsString)
# Register the DataFrame as a table.
tem_df.registerTempTable("tempReadingsTable")
pre_df.registerTempTable("preReadingsTable")
# Can run queries now
stations_temp = tem_df.groupBy('station').agg(F.max('temp_value').alias('max_temperature'))

stations_pre = pre_df.groupBy('station',"date").agg(F.sum('pre_value').alias('daily_precipitation'))
stations_pre = stations_pre.groupBy('station').agg(F.max('daily_precipitation').alias('max_precipitation'))

station_list = stations_pre.join(stations_temp,stations_pre['station']==stations_temp['station'],'inner').drop(stations_temp['station'])
station_list = station_list.filter((station_list['max_temperature']>=25) & (station_list['max_temperature']<=30)& (station_list['max_precipitation']<=200)& (station_list['max_precipitation']>=100)).orderBy('station',ascending=0)

station_list.rdd.saveAsTextFile("BDA/output")


### 5)
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName = "exercise 2")
sqlContext = SQLContext(sc)
# This path is to the file on hdfs
region_file = sc.textFile("BDA/input/stations-Ostergotland.csv")
precipitation_file = sc.textFile("BDA/input/precipitation-readings.csv")
region_lines = region_file.map(lambda line: line.split(";"))
pre_lines = precipitation_file.map(lambda line: line.split(";"))

regionReadingsRow = region_lines.map(lambda p: (p[0],))
preReadingsRow = pre_lines.map(lambda p: (p[0], p[1], int(p[1].split("-")[0]),int(p[1].split("-")[1]), p[2], float(p[3]), p[4] ))
# Specifying the schema programatically and registering the DataFrame as a table
regionReadingsString = ["station"]
preReadingsString = ["station", "date", "year", "month", "time", "pre_value","quality"]
# Apply the schema to the RDD.
region_df = sqlContext.createDataFrame(regionReadingsRow,regionReadingsString)
pre_df = sqlContext.createDataFrame(preReadingsRow,preReadingsString)
# Register the DataFrame as a table.
region_df.registerTempTable("regionstationsTable")
pre_df.registerTempTable("preReadingsTable")
# Can run queries now
pre_region_df = pre_df.join(region_df,pre_df['station']==region_df['station'],'inner').drop(region_df['station'])
avg_monthly = pre_region_df.filter((pre_region_df['year']>=1993) & (pre_region_df['year']<=2016)).groupBy('year','month','station').agg(F.sum('pre_value').alias('monthly_everystation')).groupBy('year','month').agg(F.avg('monthly_everystation').alias('avg_monthly')).orderBy(['year','month'],ascending=[0,0])

avg_monthly.rdd.saveAsTextFile("BDA/output")
