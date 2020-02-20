
pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 

#Structured Streaming consumer for NLP
spark.readStream.format("kafka")\
.option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")\
.option("subscribe", "NLP")\
.option("startingOffsets", "earliest")\
.load()\
.selectExpr("CAST(value AS STRING)")\
.writeStream.format("text")\
.option("checkpointLocation", "/user/__dsets/NLP/")\
.option("path", "/user/__dsets/NLP/")\
.start()\
.awaitTermination()


#Stuctured Streamng consumer fr Crypt

spark.readStream.format("kafka")\
.option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")\
.option("subscribe", "crypt")\
.option("startingOffsets", "earliest")\
.load()\
.selectExpr("CAST(value AS STRING)")\
.writeStream.format("text")\
.option("checkpointLocation", "/user/__dsets/files/")\
.option("path", "/user/__dsets/files/")\
.start()\
.awaitTermination()


