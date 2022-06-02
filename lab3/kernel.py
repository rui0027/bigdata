from __future__ import division
from math import radians, cos, sin, asin, sqrt, exp
from datetime import datetime, timedelta
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

h_distance = 1# Up to you
h_date = 7# Up to you
h_time = 0.5# Up to you
a = 58.4274 # Up to you
b = 14.826 # Up to you
date = "2014-07-04" # Up to you

stations = sc.textFile("BDA/input/stations.csv")
temps = sc.textFile("BDA/input/temperature-readings-small.csv")

# Your code here
stations_lines = stations.map(lambda line: line.split(";"))
temp_lines = temps.map(lambda line: line.split(";"))

#(station_num,(latitude,longitude))
stations_data = stations_lines.map(lambda x: (x[0],(float(x[3]),float(x[4]))))

#(station_num,distance)
distance_stations = stations_data.map(lambda x: (x[0],haversine(b,a,x[1][1],x[1][0])))
#broadcast distance data
broadcast_distance = sc.broadcast(distance_stations.collect())

# gaussian kernel
def gk(x,h):
  return exp(-x**2/(2*h**2))

#map the temp data
#(station_num,date,time,temperature)
temp_data = temp_lines.map(lambda x: (x[0],datetime(int(x[1][0:4]),int(x[1][5:7]),int(x[1][8:10])),x[2],float(x[3])))

#filter the dates which are no later than the date of interest(including the day)
datetime_pre = datetime(int(date[0:4]),int(date[5:7]),int(date[8:10]))
temp_data_pre = temp_data.filter(lambda x: x[1]<=datetime_pre)

#the hour doesn't influence the kernel function of distance and day, so we can preprocess these 2 kernel functions
#(date,time,distance_kernel,day_kernel,temperature)
kernel_fun_pre = temp_data_pre.map(lambda x: (x[1],x[2],gk(dict(broadcast_distance.value)[x[0]],h_distance),gk((datatime_pre-x[1]).days,h_date),x[3])).cache()

# prediction
prediction_temp={}

for time in ["24:00:00", "22:00:00", "20:00:00", "18:00:00", "16:00:00", "14:00:00",
"12:00:00", "10:00:00", "08:00:00", "06:00:00", "04:00:00"]:
  if time=="24:00:00":
  	datetime_interest = datetime(int(date[0:4]),int(date[5:7]),int(date[8:10]),0,0,0)+timedelta(days=1)
  else:
  	datetime_interest = datetime(int(date[0:4]),int(date[5:7]),int(date[8:10]),int(time[0:2]),int(time[3:5]),int(time[6:8]))
  kernel_fun = kernel_fun_pre.filter(lambda x: x[0].replace(hour=x[1][0:2],minute=x[1][3:5],second=x[1][6:8])<datetime_interest)
  #(distance_kernel+day_kernel+hour_kernel,temperature)
  kernel_all = kernel_fun.map(lambda x: (x[2]+x[3]+gk(abs((datetime(0,0,0,time[0:2],time[3:5],time[6:8])-datetime(0,0,0,x[1][0:2],x[1][3:5],x[1][6:8])).total_seconds())/3600,h_time),x[4]))
  #(sum_kernel,sum_kernel * temperature) 
  kernel_sum = kernel_all.map(lambda x:(x[0],x[0]*x[1]))
  predict_value = kernel_sum.reduce(lambda a,b: (a[0]+b[0],a[1]+b[1]))
  prediction_temp[time]=kernel[1]/kernel[0]

list(prediction_temp.items()).rdd.saveAsTextFile("BDA/output")




