import requests
import os
import json
import csv
from datetime import datetime
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

spark = SparkSession(sc)
sc = SparkContext(appName="PySparkShell")

csvFile = open('result.csv', 'a')
csvWriter = csv.writer(csvFile)

# To set your enviornment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'
bearer_token = """AAAAAAAAAAAAAAAAAAAAAEkabQEAAAAAi6I2OvSdJVW6XeAyifv5K2%2BfWK0%3DoyKFIOq7EQgNYMTrG7ymr5ikm1cT8896L6wxGVu7S5CpwK84v6"""


def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2FilteredStreamPython"
    return r


def get_rules():
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream/rules", auth=bearer_oauth
    )
    if response.status_code != 200:
        raise Exception("Cannot get rules (HTTP {}): {}".format(response.status_code, response.text))
    print(json.dumps(response.json()))
    return response.json()


def delete_all_rules(rules):
    if rules is None or "data" not in rules:
        return None

    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload
    )
    if response.status_code != 200:
        raise Exception("Cannot delete rules (HTTP {}): {}".format(response.status_code, response.text))
    print(json.dumps(response.json()))


def set_rules(delete):
    # You can adjust the rules if needed
    sample_rules = [
        {"value": "Elon", "tag": "Elon"},
        {"value": "food", "tag": "food"},
        {"value": "USA", "tag": "USA"},
        {"value": "Meta", "tag": "Meta"},
        {"value": "Crypto", "tag": "Crypto"}
    ]
    payload = {"add": sample_rules}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload,
    )
    if response.status_code != 201:
        raise Exception("Cannot add rules (HTTP {}): {}".format(response.status_code, response.text))
    print(json.dumps(response.json()))


def get_stream(set):
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream", auth=bearer_oauth, stream=True,
    )
    print(response.status_code)
    if response.status_code != 200:
        raise Exception("Cannot get stream (HTTP {}): {}".format(response.status_code, response.text))
    for response_line in response.iter_lines():
        if response_line:
            json_response = json.loads(response_line)
            # print(json.dumps(json_response, indent=4, sort_keys=True))
            # print(json_response['data']['id'].encode('utf8'), json_response['matching_rules'][0]['tag'].encode('utf8'), json_response['data']['text'].encode('utf8'))
            currtime = datetime.now()
            

            topic_name="food"
            connection_port='127.0.0.1:9092'
            producer = KafkaProducer(bootstrap_servers='localhost:9092', api_version=(3, 1, 0), value_serializer=lambda x: dumps(x).encode('utf-8'))

            #my_schema = tp.StructType([tp.StructField(name= 'id', dataType= tp.StringType(),  nullable= True),tp.StructField(name= 'tag',dataType= tp.StringType(),  nullable= True), tp.StructField(name= 'tweet',dataType= tp.StringType(),nullable= True), tp.StructField(name= 'time',dataType=tp.StringType(), nullable=True)])
            #my_data = spark.read.csv('D:\PES6\DBTLab\DBTProject\\result.csv',schema=my_schema,header=True)

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

            #csvWriter.writerow([json_response['data']['id'], json_response['matching_rules'][0]['tag'], json_response['data']['text'].encode('utf8'), currtime])


def main():
    rules = get_rules()
    delete = delete_all_rules(rules)
    set = set_rules(delete)
    get_stream(set)


if __name__ == "__main__":
    main()