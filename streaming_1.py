from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, IntegerType
import time

spark = SparkSession.builder.appName("SparkStreamingKafka").getOrCreate()

stream_schema = StructType().add('id', IntegerType()).add('action', StringType())
users_schema = StructType().add('id', IntegerType()).add('user_name', StringType()).add('user_age', IntegerType())


users_data = [
    (0, "Jim", 18),
    (1, "Roza", 30),
    (2, "Bob", 18),
    (3, "Lena", 60),
    (4, "Kazimir", 33)
]

users = spark.createDataFrame(data=users_data, schema=users_schema)

input_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "netology") \
    .option("failOnDataLoss", False) \
    .load()

json_stream = input_stream \
    .select(col("timestamp").cast("string"), from_json(col("value").cast("string"), stream_schema) \
            .alias("parsed_value"))


clean_data = json_stream.select(
    col("timestamp"),
    col("parsed_value.id").alias('id'),
    col("parsed_value.action").alias("action"))


join_stream = clean_data.join(
    users,
    clean_data.id == users.id, "left_outer").select(
    users.user_name,
    users.user_age,
    clean_data.timestamp,
    clean_data.action)

# join_stream \
#     .writeStream \
#     .format("console") \
#     .outputMode("append") \
#     .option("truncate", False) \
#     .start() \
#     .awaitTermination()

res = join_stream \
    .writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .option("checkpointLocation", "checkpoint_1") \
    .start()

time.sleep(130)
res.stop()
