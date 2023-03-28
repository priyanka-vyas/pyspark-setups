from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from kafka import KafkaProducer
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming import StreamingContext
spark = SparkSession \
        .builder \
        .appName("Kafka Integration") \
        .config("spark.sql.debug.maxToStringFields", "100") \
        .getOrCreate()
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
    .option("kafka.security.protocol", "SSL") \
    .option("failOnDataLoss", "false") \
    .option("subscribe", "topic1") \
    .option("includeHeaders", "true") \
    .option("startingOffsets", "latest") \
    .option("spark.streaming.kafka.maxRatePerPartition", "50") \
    .load()
def func_call(df, batch_id):
    df.selectExpr("CAST(value AS STRING) as json")
    requests = df.rdd.map(lambda x: x.value).collect()
    logging.info(requests)
query = kafka_df.writeStream \
    .format(HiveWarehouseSession.STREAM_TO_STREAM) \
    .foreachBatch(func_call) \
    .option("checkpointLocation","file://F:/tmp/kafka/checkpoint") \
    .trigger(processingTime="5 minutes") \
    .start().awaitTermination()                           

df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
    .option("kafka.security.protocol", "SSL") \
    .option("failOnDataLoss", "false") \
    .option("topic", "topic2") \
    .save()