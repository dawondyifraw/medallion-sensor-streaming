import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, DoubleType, StringType
from streaming.kafka_consumer import get_kafka_stream

BRONZE_PATH = "data/bronze/sensor_data_medallion"
KAFKA_BROKER = "localhost:9092"
TOPIC = "sensor-data-medallion"  # env.air.v1 env.noise.v1 env.telemetry.v1

# Schema matches the producer payload
schema = StructType() \
    .add("timestamp", StringType()) \
    .add("co2_ppm", DoubleType()) \
    .add("noise_db", DoubleType()) \
    .add("temperature_c", DoubleType()) \
    .add("latitude", DoubleType()) \
    .add("longitude", DoubleType())

# Spark session
spark = SparkSession.builder \
    .appName("KafkaToBronze") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "io.delta:delta-spark_2.12:3.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Read Kafka stream (raw JSON in `value`)
df_raw = get_kafka_stream(spark,topic=TOPIC,servers=KAFKA_BROKER)

# Parse JSON payload
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Write to Bronze Delta
query = df_parsed.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "data/bronze/_checkpoints/sensor_data_medallion") \
    .start(BRONZE_PATH)

print("Kafka consumer is writing raw data to Bronze Delta Lake...")
query.awaitTermination()
