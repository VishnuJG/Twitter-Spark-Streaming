from __future__ import absolute_import
from kafka import KafkaProducer
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.streaming import StreamingContext
import pyspark.sql.types as tp
from pyspark.sql import Row
from pyspark.sql.functions import *
from datetime import datetime
import time
from json import dumps

sc = SparkContext(appName="PySparkShell")
# spark = StreamingContext(sc, 300)
spark = SparkSession(sc)

topic_name="food"
connection_port='127.0.0.1:9092'
producer = KafkaProducer(bootstrap_servers='localhost:9092', api_version=(3, 1, 0), value_serializer=lambda x: dumps(x).encode('utf-8'))

my_schema = tp.StructType([tp.StructField(name= 'id', dataType= tp.StringType(),  nullable= True),tp.StructField(name= 'tag',dataType= tp.StringType(),  nullable= True), tp.StructField(name= 'tweet',dataType= tp.StringType(),nullable= True), tp.StructField(name= 'time',dataType=tp.StringType(), nullable=True)])
my_data = spark.read.csv('D:\PES6\DBTLab\DBTProject\\result.csv',schema=my_schema,header=True)

#data = {'number' : "vishnu"}
#producer.send(topic_name, value="Hello Vishnu")
#producer.flush()
#producer.close()
print("########################done#################################")

tumblingWindows = my_data.withWatermark("time","5 minutes").groupBy("tag", window("time", "5 minutes")).count()
tumblingWindows.show(40, truncate=False)


for i in tumblingWindows.collect():
    print(i['tag'])
    print(i['window'])
    print(i['count'])

    producer.send(i['tag'], value=i['count'])
producer.flush()
producer.close()




# my_data.show(5)

# my_data.printSchema()

# kafka_producer_object=KafkaProducer(bootstrap_server=connection_port, value_serializer=lambda x:dumps(x).encode('utf-8'))

# producer.flush()