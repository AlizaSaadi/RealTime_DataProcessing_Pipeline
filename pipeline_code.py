from pymongo import MongoClient
from confluent_kafka import Producer
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
import json

MONGO_CONNECTION_STRING = "mongodb://localhost:27017/"
MONGO_DB_NAME = "Ball_by_Ball"
MONGO_COLLECTION_NAME = "data"

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

mongo_client = MongoClient(MONGO_CONNECTION_STRING)
mongo_db = mongo_client[MONGO_DB_NAME]
mongo_collection = mongo_db[MONGO_COLLECTION_NAME]


def publish_to_kafka(data):
    match_id = data.get("Match_Id", "default_topic")  # Using Match_Id for topic selection
    topic = f"match_{match_id}"  # Creating topic name based on Match_Id
    kafka_producer.produce(topic, json.dumps(data).encode('utf-8'), callback=delivery_report)

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

kafka_producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

conf = SparkConf().setAppName("KafkaStreamExample")
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 1)

kafka_stream = ssc.socketTextStream("localhost", 9092)
parsed_data = kafka_stream.map(lambda x: json.loads(x))

grouped_data = parsed_data.map(lambda data: (data["Match_Id"], data)).groupByKey()
grouped_data.foreachRDD(lambda rdd: send_to_kafka(rdd))

def send_to_kafka(rdd):
    for match_id, data_list in rdd.collect():
        topic = f"match_{match_id}"  # Create topic name based on Match_Id
        for data in data_list:
            kafka_producer.produce(topic, json.dumps(data).encode('utf-8'))


