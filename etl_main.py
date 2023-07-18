from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pymongo import MongoClient


client = MongoClient('localhost', 27017)

db = client['Demo']

# Creating a SparkContext
sc = SparkContext(appName="ClickBaitStreamingApp")

# Creating a StreamingContext with a batch interval of 5 seconds
ssc = StreamingContext(sc, 5)

# Set Kafka broker and topic details
kafka_params = {
    "bootstrap.servers": "localhost:9092"
}
topic = "test"
topics = [topic]

# Creating a Kafka direct stream
kafka_stream = KafkaUtils.createDirectStream(
    ssc, topics, kafkaParams=kafka_params)

print("********created stream<<<<<<<<<<<<<<<")


# Processing each RDD in the Kafka stream
def func(rdd):
    value = rdd.map(lambda x: x[1])
    if value:
        y = value.collect()
        print("value:", y)
        #Inserting the data
        db['clickbaits'].insert_one(y)
        print("inserted")
        
# Starting the Spark Streaming application
kafka_stream.foreachRDD(func)
ssc.start()
ssc.awaitTermination()


# C:\spark32\bin\spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4 etl_main.py
