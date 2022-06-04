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

h_distance = 30# Up to you
h_date = 3# Up to you
h_time = 1# Up to you
a = 58.4274 # Up to you
b = 14.826 # Up to you
date = "2013-07-04" # Up to you

stations = sc.textFile("BDA/input/stations.csv")
temps = sc.textFile("BDA/input/temperature-readings.csv")

# Your code here
stations_lines = stations.map(lambda line: line.split(";"))
temp_lines = temps.map(lambda line: line.split(";"))

#(station_num,distance)
distance_stations = stations_lines.map(lambda x: (x[0],haversine(b,a,float(x[4]),float(x[3]))))
#broadcast distance data
broadcast_distance = sc.broadcast(dict(distance_stations.collect()))

# gaussian kernel
def gk(x,h):
	return exp(-x**2/(2*h**2))
# distance kernel
def kernel_distance(station_num,h_d):
  return gk(broadcast_distance.value[station_num],h)
  
def kernel_day(day_1,day_2,h_day):
  return gk(abs((day_1-day_2).days)%365+round(abs((day_1-day_2).days/365)/4),h_day)
  
def kernel_hour(time_1,time_2,h_t):
	time_2 = time_2.replace(year=time_1.year,month=time_1.month,day=time_1.day)
	diff=abs((time_1-time_2).total_seconds())/3600
	return gk(diff,h_t)
	
#preprocess
#(station_num,datetime,temperature)
temp_data = temp_lines.map(lambda x: (x[0],datetime(int(x[1][0:4]),int(x[1][5:7]),int(x[1][8:10]),int(x[2][0:2]),int(x[2][3:5]),int(x[2][6:8])),float(x[3])))
temp_data = temp_data.filter(lambda x: x[1]<=(datetime(int(date[0:4]),int(date[5:7]),int(date[8:10]),0,0,0)+timedelta(days=1))).cache()

test_h_d=range()
test_h_day=
test_h_hour=
# prediction
prediction_temp={}

for i in range(30):
  	datetime_interest = datetime(int(date[0:4]),int(date[5:7]),int(date[8:10]),23,59,59)
  else:
  	datetime_interest = datetime(int(date[0:4]),int(date[5:7]),int(date[8:10]),int(time[0:2]),int(time[3:5]),int(time[6:8]))
  #filter the dates which are posterior	
  #(station_num,datetime,temperature)
  temp_filter = temp_data.filter(lambda x: x[1]<datetime_interest)

  #(kernel_diatance, kernel_day, kernel_date,temperature)
  kernel_all = temp_filter.map(lambda x: (kernel_distance(x[0],test_h_d[i]),kernel_day(datetime_interest,x[1],test_h_day[i]),kernel_hour(datetime_interest,x[1],test_h_hour[i]),x[2]))
  #(kernel_diatance, kernel_day, kernel_date,temperature,kdistance*temp,kday*temp, khou*temp) 
  kernel_sum = kernel_all.map(lambda x: (x[0],x[1],x[2],x[3],x[0]*x[3],x[1]*x[3],x[2]*x[3])).reduce(lambda a,b: (a[0]+b[0],a[1]+b[1],a[2]+b[2],a[4]+b[4],a[5]+b[5],a[6]+b[6]))
  kernel_predict = (kernel_sum[4]/kernel_sum[0],kernel_sum[5]/kernel_sum[1],kernel_sum[6]/kernel_sum[2]) 
  prediction_temp[time]=kernel_sum[1]/kernel_sum[0]

sc.parallelize(prediction_temp.items()).saveAsTextFile("BDA/output")

