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
temp_sample = temp_lines.map(lambda x: (x[0],datetime(int(x[1][0:4]),int(x[1][5:7]),int(x[1][8:10]),int(x[2][0:2]),int(x[2][3:5]),int(x[2][6:8])),float(x[3]))).sample(False,0.1)
sample_rdd = temp_sample.randomSplit(0.7,0.3)
temp_train = sample_rdd[0].cache()
temp_test = sample_rdd[1].cache()

test_h_d=[10,20,30,40,50,60,70,80,90,100,150,200]
test_h_day=[1,3,5,7,10,12,15,20,25,30,40,60]
test_h_hour=[0.5,1,1.5,2,2.5,3,3.5,4,5,6,7,10]

error_distance = []
error_day = []
error_hour = []
for i in range(12):
	error=(0,0,0)
	for test_date in temp_test.collect():
  	#filter the dates which are posterior	
  	#(station_num,datetime,temperature)
  	temp_train = temp_train.filter(lambda x: x[1]<test_date[1])
  	#(kernel_diatance, kernel_day, kernel_date,temperature)
  	kernel_all = temp_train.map(lambda x: (kernel_distance(x[0],test_h_d[i]),kernel_day(test_date[1],x[1],test_h_day[i]),kernel_hour(test_date[1],x[1],test_h_hour[i]),x[2]))
  	#(kernel_diatance, kernel_day, kernel_date,temperature,kdistance*temp,kday*temp, khou*temp) 
  	kernel_sum = kernel_all.map(lambda x: (x[0],x[1],x[2],x[3],x[0]*x[3],x[1]*x[3],x[2]*x[3])).reduce(lambda a,b: (a[0]+b[0],a[1]+b[1],a[2]+b[2],a[4]+b[4],a[5]+b[5],a[6]+b[6]))
  	#(kernel_distance prediction,kernel_day prediction,kernel_hour prediction)
		kernel_predict = (kernel_sum[4]/kernel_sum[0],kernel_sum[5]/kernel_sum[1],kernel_sum[6]/kernel_sum[2]) 
  	error = (error[0]+abs(test_date[2]-kernel_predict[0]),error[1]+abs(test_date[2]-kernel_predict[1]),error[2]+abs(test_date[2]-kernel_predict[2]))
	error_distance.append(error[0])
	error_day.append(error[1])
	error_hour.append(error[2])

parameters = [test_h_d[argmin(error_distance)],test_h_day[argmin(error_day)],test_h_hour[argmin(error_hour)]]

sc.parallelize(parameters).saveAsTextFile("BDA/output")

