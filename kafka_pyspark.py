# Imports
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql.functions import col, from_json

spark_version = '3.3.2'
packages = 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1'


#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0,org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 pyspark-shell'


spark = SparkSession.builder.appName("kafka-spark").config("spark.jars.packages", packages).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


df_reader = spark\
    .read\
    .format("kafka")\
	.option("kafka.bootstrap.servers","localhost:9092")\
	.option("subscribe","test")\
    .load()
 


readDF = df_reader.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)")
readDF.show()

# df1 = df.selectExpr("cast(value as string)")

# json_schema = StructType().add("emp_id", IntegerType()) \
#     .add("name", StringType()) \
#     .add("salary", IntegerType())\
#     .add(("dept_id"),StringType())\
#     .add(("manager_id"),StringType())
# df2 = df1.select(from_json(col("value"),json_schema))

# df2.writeStream\
# 	.format("console")\
# 	.outputMode("append")\
# 	.option("truncate","false")\
# 	.start()\
# 	.awaitTermination()



