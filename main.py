from pyspark import SparkContext
from pyspark.sql import SparkSession

data = range(1,50)
spark = SparkSession.builder.appName("kamal").getOrCreate()

rdd = spark.sparkContext.parallelize(data,4)
rddc = rdd.count()
print(rddc)