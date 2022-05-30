from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

# Load a text file and convert each line to a tuple.
rdd = sc.textFile(”FILENAME")
parts = rdd.map(lambda l: l.split(";"))
tempReadingsRow = parts.map(lambda p: (p[0], p[1], int(p[1].split("-")[0]),
int(p[1].split("-")[1]), p[2], float(p[3]), p[4] ))
# Specifying the schema programatically and registering the DataFrame as a table
tempReadingsString = ["station", "date", "year", "month", "time", "value",
"quality"]
# Apply the schema to the RDD.
schemaTempReadings = sqlContext.createDataFrame(tempReadingsRow,
tempReadingsString)
# Register the DataFrame as a table.
schemaTempReadings.registerTempTable("tempReadingsTable")
# Can run queries now
