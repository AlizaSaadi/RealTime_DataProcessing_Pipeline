Overview:
This project implements a real-time data processing pipeline using MongoDB, Kafka, and Apache Spark. Data is ingested from a MongoDB collection, processed in real-time with Spark Streaming, and published to dynamic Kafka topics based on Match_Id.

Features:

MongoDB Integration: Fetches and processes ball-by-ball cricket data stored in MongoDB.

Dynamic Kafka Topics: Publishes data to Kafka topics dynamically generated using Match_Id for targeted real-time streaming.

Spark Streaming: Leverages Apache Spark for real-time data processing and transformation.

Custom Callbacks: Provides delivery reports for Kafka message status.


Requirements: 
Python 3.x
MongoDB
Apache Kafka
Apache Spark

Code Highlights:

MongoDB Connection: Establishes a connection to MongoDB and accesses the Ball_by_Ball database.

Kafka Producer: Publishes JSON-encoded cricket data to dynamically generated Kafka topics.

Spark Streaming Context: Processes incoming data streams in real-time using Spark.

Custom Kafka Callbacks: Logs message delivery statuses.
