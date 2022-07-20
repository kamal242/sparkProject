from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import *

spark = SparkSession.builder.appName("Map").getOrCreate()
data_ord_path = ("C:\\Users\\Kamal Nayan\\Documents\\Orders\\part-00000")
data_ordItems_path = ("C:\\Users\\Kamal Nayan\\Documents\\Order_items\\part-00000")

#
# rdd = spark.sparkContext.textFile(data)
#
#
# for i in rdd.take(5):
#     print(i)
#
# julyRdd = rdd.filter(lambda x:(x.split(',')[1].split('-')[1])=='07').map(lambda x: x.split(',')[2])
# AugRdd = rdd.filter(lambda x:(x.split(',')[1].split('-')[1])=='08').map(lambda x: x.split(',')[2])
#
# print("july rdd")
# for i in julyRdd.take(5):
#     print(i)
# print("july count" +str(julyRdd.count()))
#
# print("Aug rdd")
# for i in AugRdd.take(5):
#     print(i)
# print("Aug count"+str(AugRdd.count()))
#
# unionrdd = julyRdd.union(AugRdd)
# print("union rdd count"+str(unionrdd.count()))
# for i in unionrdd.take(5):
#     print(i)
# # rdd.collect()
# #rddMap = rdd.map(lambda x : x.split(',')[0])
# #for i in rddMap.take(5):
#     #print(i)
# #for i in rdd.take(5):
#     #print(i)
#
#------------------MAp------------------
ord = spark.sparkContext.textFile(data_ord_path)
ordItems = spark.sparkContext.textFile(data_ordItems_path)
# for i in ordItems.take(5):
#     print(i)

# ordMap = ord.map(lambda x: x.split(',')[0])
# for i in ordMap.take(5):
#     print(i)

#takes orderId and status
# ordMapStatus = ord.map(lambda x: (x.split(',')[0],x.split(',')[3]))
# for i in ordMapStatus.take(5):
#     print(i)

#convert orderDate into dd/mm/yy
# ordDateMap = ord.map(lambda x: (x.split(',')[1]).replace('-','/'))
# ordDateMapRef= ordDateMap.map(lambda x:(x.split(' ')[0]))
#
# #KEY vALUE pAIR
# ordByKeyValue = ord.map(lambda x:(x.split(',')[0],x))

#userDefined Function LowerCase
# def lowerCase(str):
#     return str.lower()
#
# ordLower = ord.map(lambda x: lowerCase(x.split(',')[3]))


##------------flatMap(f,preservesPartitioning=False) and reduceByKey
# ordflatMap = ord.flatMap(lambda x: x.split(','))
# wordCountStepOne = ordflatMap.map(lambda w: (w,1))
# wordCountStepTwo = wordCountStepOne.reduceByKey(lambda x,y:x+y)
# print(wordCountStepTwo.take(10),sep='\n')




##-----filter(f)---------------
###Print all the orders which are closed or Complete and ordered in the year 2013
# filteredOrd = ord.filter(lambda x:x.split(',')[3] in ["CLOSED","COMPLETE"]  and x.split(',')[1].split('-')[0] == "2013")
# for i in filteredOrd.take(4):print(i)

##-----------mapValues(f)--------
# rdd = spark.sparkContext.parallelize((("a", (1,2,3)), ("b", (3,4,5)),("a", (1,2,3,4,5))))
# def f(x): return len(x)
# print(rdd.mapValues(f).collect())


##---------------Join-------
# ordMap = ord.map(lambda x:(x.split(',')[0],x.split(',')[2]))
# ordItemsMap = ordItems.map(lambda x:(x.split(',')[1],x.split(',')[4]))
# findSubtotalForCust = ordMap.join(ordItemsMap)
# print(findSubtotalForCust.take(10))
#
# ##--------cogroup-------******REVISE********
# x = spark.sparkContext.parallelize((("a",1),("b",4)))
# y = spark.sparkContext.parallelize(("a",2))
# xy= x.cogroup(y)
# for i,j in list(xy.take(5)):print(i+''+str(map(list(j))))
#
# #-----------cartesian------
# rdd = spark.sparkContext.parallelize((5,4,2))
# print(sorted(rdd.cartesian(rdd).collect()))

####---------------------------------------------Total Aggregations------------
###count() returns the number of elements in the RDD
# noOfOrders = ord.filter(lambda x:x.split(',')[3]).count()
# print(noOfOrders)

##Reduce():   Reduces the elements of this RDD using the specified commutative and associative binary operator. Currently
# reduces partitions locally
### Find the total quantity sold for Order ID 1-10.
# #for i in ordItems.take(5):
#     #print(i)
# orderQuantitySold = ordItems.filter(lambda x:int(x.split(',')[1])<11).map(lambda x:float(x.split(',')[4])).reduce(lambda x,y:x+y)
# print(orderQuantitySold)
#


##----------------------------------- Shuffle:--------------------------------
##
##
# • Shuffling is a process of redistributing data across partitions or even nodes.
# • Shuffle operation would create a new stage.
# • Based on data size we may reduce or increase the number of partitions using configuration spark.sql.shuffle.partitions or through codes like repartition
# and coalesce.
# • Costly operation as it involves disk I/O, Network I/O and data serialization/de-Serilization.
# • Spark shuffling triggers for transformation operations like gropByKey(), reducebyKey(), join(), union(),cogroup, groupBy() etc.
# • distinct creates a shuffle.
# • Count and countByKey does not create any shuffle.
# • Avoid shuffling at all cost. If Shuffling is absolutely necessary, use combiner.
# • Out of 3 main key aggregation APIs, the groupByKey does not use a combiner and so should be avoided. The reduceByKey and
# aggregrateByKey use the combiner and should be preferred.


###-------- Key Aggregations:-----
#groupByKey()
#aggregateByKey()
#reduceByKey()
#countByKey()



#-------------------groupByKey()--------------------------
# from functools import reduce
# ordGrp = ordItems.map(lambda x:( int(x.split(",")[2]) ,float(x.split(",")[4]) )).groupByKey()
# # for i in ordGrp.take(5):print(i,y)
# d=reduce(lambda x,y: x+y[1])
# ordGrpSum =ordGrp.map(lambda x: (x[0] ,d))
# print(ordGrpSum.take(10))

#-------------------aggregateByKey(zeroValue,seqOp,combOp,(numPartitions))----------------------------
#First aggregates elements in each partition and then sends across the network
#Shuffling is less as data elem is combined in map phase only
#zeroValaue: initial value to initialize accumulater. Use 0 for integer and NULL for collections
#seqOP:Function to accumulate the result in each partition.
#combOP: function used to combine result of all the partitions

# ordexpl = spark.sparkContext.parallelize([
# (2,"Joseph",200), (2,"Jimmy",250), (2,"Tina",130), (4,"Jimmy",50), (4,"Tina",300),
# (4,"Joseph",150), (4,"Ram",200), (7,"Tina",200), (7,"Joseph",300), (7,"Jimmy",80)],2)
#
# # for i in ordexpl.take(5):
# #     print(i)
#
# ordPAir = ordexpl.map(lambda x: (x[0],(x[1],x[2])))
# for i in ordPAir.take(5):
#     print(i)
#

# zero_val = 0
#
# #Define sequence operation
# #sequence opeation : finding max revenue from each partitions
# def seq_op(accumulator,element):
#     if(accumulator>element[1]):
#         return accumulator
#     else:
#         return element[1]
#
# #Define combiner operation
# #combiner Operation: finding Max Revenue from all partitions
# def comb_op(accumulator1,accumulator2):
#     if(accumulator1>accumulator2):
#         return accumulator1
#     else:
#         return accumulator2
# aggr_ordItems = ordPAir.aggregateByKey(zero_val,seq_op,comb_op)
# for i in aggr_ordItems.collect():
#     print(i)

#***************pg 168***************
#DataFrame APIs
ordDf = spark.read.load(data_ord_path, format ='csv',sep=',',schema =('order_id int,order_date timestamp,order_customer_id int,order_status string'))


# from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType
# # ordDF=ord.toDF().show()
# schema = StructType([\
#     StructField("orderId",IntegerType(),True),\
#     StructField("order_Date",DateType(),True),\
#     StructField("customerId",IntegerType(),True),\
#     StructField("OrderStatus", StringType(), True),\
#     ])
# ordDf.show()

# data=(('Robert',35,40,40),('Robert',35,40,40),('Ram',31,33,29),('Ram',31,33,91))
# emp = spark.createDataFrame(data=data,schema=('name','score1','score2','score3'))
# emp.show()

#******selection******
# 3 ways of selecting
# ordDf.select(ordDf.order_id,'order_id', "order_id",(ordDf.order_id+10).alias('order10')).show()

# ordDf.selectExpr("substring(order_date,1,4) As order_year").show()

# ordDf.select(ordDf.order_date).show()

# ordDf.withColumn('order_month',substring(ordDf.order_date,1,10)).show()


#********Join**************

# df1 = spark.createDataFrame(data=((1,'Robert'),(2,'Ria'),(3,'James')),schema='empid int,empname string')
# df2 = spark.createDataFrame(data=((2,'USA'),(4,'India')),schema='empid int,country string')
# df1.join(df2,df1.empid == df2.empid,'inner').select(df1.empid,df2.country).show()

#****MultiColumn Join*********
# df1 = spark.createDataFrame(data=((1,101,'Robert'),(2,102,'Ria'),(3,103,'James')),schema='empid int,deptid int,empname string')
# df2 = spark.createDataFrame(data=((2,102,'USA'),(4,104,'India')),schema='empid int,deptid int,country string')
# df1.show()
# df2.show()
#
# df1.join(df2,(df1.empid == df2.empid) & (df1.deptid == df2.deptid)).show()

####________________________________________19-07-2022____________________
#********groupBy()******
# data = (("James","Sales","NY",9000,34),
# ("Alicia","Sales","NY",8600,56),
# ("Robert","Sales","CA",8100,30),
# ("Lisa","Finance","CA",9000,24),
# ("Deja","Finance","CA",9900,40),
# ("Sugie","Finance","NY",8300,36),
# ("Ram","Finance","NY",7900,53),
# ("Kyle","Marketing","CA",8000,25),
# ("Reid","Marketing","NY",9100,50)
# )

data = (("James","Sales","NY",9000,34),
("Alicia","Sales","NY",8600,56),
("Robert","Sales","CA",8100,30),
("John","Sales","AZ",8600,31),
("Ross","Sales","AZ",8100,33),
("Kathy","Sales","AZ",1000,39),
("Lisa","Finance","CA",9000,24),
("Deja","Finance","CA",9900,40),
("Sugie","Finance","NY",8300,36),
("Ram","Finance","NY",7900,53),
("Satya","Finance","AZ",8200,53),
("Kyle","Marketing","CA",8000,25),
("Reid","Marketing","NY",9100,50)
)

schema=("empname","dept","state","salary","age")
df  = spark.createDataFrame(data,schema)
# df.show()


# df.groupBdfy(.dept,df.state).min("salary","age").show()

# df.where(df.state == 'NY').groupBy(df.dept).agg(min("salary").alias("min_salary")).show()

# df.select("dept","state","salary").show()
# df_t = df.groupBy(df.dept).pivot("state").sum("salary")
# df_t.show()

spec = Window.partitionBy("dept").orderBy("salary")

# df.select(df.dept,df.salary).withColumn("rank",rank().over(spec))\
#     .withColumn("row_number",row_number().over(spec))\
#     .withColumn("dense_rank",dense_rank().over(spec))\
#     .show()

##****Analytical Window function*********
#Return offset one less than the current
#Return offset one more than the current
#Custom Program to check if prev salary is greater than prev salary prints True or False
# df1 = df.select(df.dept,df.salary)\
#     .withColumn("lag_prev_salary",F.lag("salary",1,0).over(spec))
#
# df3= df1.withColumn("diff",F.when((df1.salary >= df1.lag_prev_salary) ,"True")\
#                                 .otherwise("False"))
# df3.show()


##*************************20-07-2022************************

###Encryption
import pyspark.sql.types
# df.select((df.empname),df.dept,df.state,md5((df.salary).cast("string")),df.age).show()

###Window Function
spec = Window.partitionBy("dept").orderBy("salary")

##Find out the first salary
df1 = df.select(df.dept,df.salary)\
    .withColumn("first_salary",first("salary").over(spec)).show()

##GroupBy API
df2 = df1.select(df1.dept,df1.salary).groupBy(df1.dept).avg().show()
