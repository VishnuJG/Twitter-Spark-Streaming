from pyspark import SparkContext
from kafka import KafkaConsumer
from pyspark.sql.session import SparkSession
from pyspark.streaming import StreamingContext
import pyspark.sql.types as tp
from json import loads
from pyspark.sql import Row
import psycopg2
import pandas as pd
from pyspark.sql import SparkSession
from sqlalchemy import create_engine


sc = SparkContext(appName="PySparkShell")
spark = StreamingContext(sc, 300)
spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.jars", "D:/PES6/DBTLab/DBTProject/postgresql-42.3.4.jar").getOrCreate()
topic_name=["food", "Crypto", "Elon", "USA", "Meta"]
connection_port='127.0.0.1:9092'
#df = spark.read.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/moviesdb").option("dbtable", "movies").option("user", "postgres").option("password", "vishnu2001").option("driver", "org.postgresql.Driver").load()

#df.printSchema()
#df.show()

conn=psycopg2.connect(database="dbttopics", user="postgres", password="vishnu2001", host="127.0.0.1", port="5432")
cur=conn.cursor()



for i in topic_name:

    consumer = KafkaConsumer(i,bootstrap_servers=['localhost:9092'], api_version=(3, 1, 0), auto_offset_reset='earliest', enable_auto_commit=True, group_id=None, request_timeout_ms=1000, consumer_timeout_ms=5000 )
    for message in consumer:
        print(message.value.decode('utf-8'))
        if message.value.decode('utf-8').isalpha():
            continue;
        cur.execute(f"update topics_table set topicount=topicount+{message.value.decode('utf-8')} where title='{i}';")
    consumer.close()

conn.commit()
cur.close()
conn.close()
    




