

# Import Libraries
from pyspark import SparkConf, SparkContext

# Create spark context object
sc = SparkContext()

print("##########MAP###########")
sourceRDD = sc.parallelize([51,44,11,12,14,5,6,3,3,1,7,7,11],4) 

### Transform elements using map 
mapRdd = sourceRDD.map(lambda x:x+1) 

### Get the results out of RDD using action collect 
mapResult = mapRdd.collect() 

### Print the results 
print(mapResult) 

print("##########Filter###########")

# Filtering condition to get even elements 
filterRDD = sourceRDD.filter(lambda x:x%2==0) 

# Get the result from RDD using collect action 
filterResult = filterRDD.collect() 

# check the output 
print(filterResult) 

print("##########End###########")

# Fill your code here

sc.stop()