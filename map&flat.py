from pyspark.sql import SparkSession
from pyspark import SparkContext

spark = SparkSession.builder.appName("Map").getOrCreate()
data = ("C:\\Users\\Kamal Nayan\\Documents\\Orders\\part*")


rdd = spark.sparkContext.textFile(data)

for i in rdd.take(5):
    print(i)