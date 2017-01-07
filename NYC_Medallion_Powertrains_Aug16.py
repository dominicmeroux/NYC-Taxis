# Authorized NYC Medallion Vehicles
# Dataset downloaded from https://data.cityofnewyork.us/Transportation/Medallion-Vehicles-Authorized/rhe8-mgbb
# Downloaded August 10, 2016
# Analysis uses Spark Version 2.0.0

df = spark.read.csv('/Users/dmeroux/Downloads/m8DI5uks.csv', header=True)

VehTypes = df.groupBy("Vehicle Type").count()
VehTypes.show()
#+------------+-------+                                                          
#|Vehicle Type|  count|
#+------------+-------+
#|         HYB|1267081|
#|         LV1|    119|
#|         CNG|    223|
#|         DSE|    527|
#|            | 635431|
#|         WAV|  69247|
#+------------+-------+

A = VehTypes.take(VehTypes.count())

TotalCount = df.count()

for i in range(0,len(A)):
	print str(A[i][0]) + "\t" + str(round((float(A[i][1])/TotalCount)*100,2)) + "%"

#HYB	64.23%
#LV1	 0.01%
#CNG	 0.01%
#DSE	 0.03%
#   	32.21%
#WAV	 3.51%