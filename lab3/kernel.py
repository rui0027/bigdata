from __future__ import division
from math import radians, cos, sin, asin, sqrt, exp
from datetime import datetime
from pyspark import SparkContext
sc = SparkContext(appName="lab_kernel")
def haversine(lon1, lat1, lon2, lat2):
  """
  Calculate the great circle distance between two points
  on the earth (specified in decimal degrees)
  """
  # convert decimal degrees to radians
  lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
  # haversine formula
  dlon = lon2 - lon1
  dlat = lat2 - lat1
  a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
  c = 2 * asin(sqrt(a))
  km = 6367 * c
  return km

h_distance = # Up to you
h_date = # Up to you
h_time = # Up to you
a = 58.4274 # Up to you
b = 14.826 # Up to you
date = "2013-07-04" # Up to you

stations = sc.textFile("BDA/input/stations.csv")
temps = sc.textFile("BDA/input/temps.csv")

# Your code here
stations_lines = stations.map(lambda line: line.split(";"))
temp_lines = temps.map(lambda line: line.split(";"))
#(station_num,(latitude,longitude))
stations_data = stations_lines.map(lambda x: (x[0],(float(x[3]),float(x[4]))))
#(station_num,distance)
distance_stations = stations_data.map(lambda x: (x[0],haversine(a,b,x[1],x[2])))

broadcast_distance = sc.broadcast(distance_stations.collect())

#(station_num,(date-time,temperature))
temp_data = temp_lines.map(lambda x: (x[0],(datetime(int(x[1][0:4]),int(x[1][5:7]),int(x[1][8:10]),int(x[2][0:2]),int(x[2][3:5]),int(x[2][6:8])),float(x[3])))).cache()

# gaussian kermel
def gk(x,h):
  return exp(-x^2/(2*h^2))

pre_temp={}

for time in ["24:00:00", "22:00:00", "20:00:00", "18:00:00", "16:00:00", "14:00:00",
"12:00:00", "10:00:00", "08:00:00", "06:00:00", "04:00:00"]:
  # filter the data
  datatime_interest = datetime(int(date[0:4]),int(date[5:7]),int(date[8:10]),int(time[0:2]),int(time[3:5]),int(time[6:8]))
  temp_filter = temp_data.filter(lambda x: x[1][1]<datatime_interest)
  kernel_fun = temp_filter.map(lambda x: (gk(dict(broadcast_distance)[x[0]],h_distance),gk((x[1][0]-datatime_interest).days,h_date),gk((x[1][0].hour-datatime_interest.hour,h_time),h_time),x[1][1]))
  # (kernel_distance,kernel_date,kernel_hour,temperature)
  kernel_fun = kernel_fun.map(lambda x: ((x[0]+x[1]+x[2])*x[3],x[0]+x[1]+x[2]))
  # (kernel_fun*y,sum_kernel_fun)
  kernel = kernel_fun.reduce(lambda a,b: (a[0]+b[0],a[1]+b[1])).collect()
  pre_temp[time]=kernel[0][0]/kernel[0][1]

pre_temp.collect().saveAsTextFile("BDA/output")








