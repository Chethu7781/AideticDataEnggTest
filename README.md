# COMPLETE PROGRESS and STEPS NEEDS TO BE DONE IN PROJECT
I have made the complete setup for the project by installing following packages and modules
spark: 3.2.4
kafka-python
pymongo
requests
datetime
pandas

I have designed a database schema for inserting data in the kafka andI have written the kafka producer file and also was able to successfully send 100 records to the kafka topic, and also have written the etl script logic and tried to execute it.

 # the project still needs work 
 Resolve module not found error. " ModuleNotFoundError: No module named 'pyspark.streaming.kafka'" and indexing and sorting the data using elastic search.

 The command for execution of etl script is
 # C:\spark32\bin\spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4 etl_main.py
