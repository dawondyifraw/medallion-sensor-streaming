from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count

# Delta Lake paths
SILVER_PATH = "data/silver/sensor_data_medallion"
GOLD_PATH = "data/gold/sensor_data_medallion"
CHECKPOINT_PATH = "data/gold/_checkpoints/zone_hourly_summary"

# Spark session with Delta + Kafka support
spark = SparkSession.builder \
    .appName("SilverToGold") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "io.delta:delta-spark_2.12:3.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

# Stream Silver data
df_silver_stream = spark.readStream.format("delta").load(SILVER_PATH)

# Aggregate by zone and hour
df_gold = df_silver_stream.groupBy("zone", "hour").agg(avg("co2_ppm").alias("avg_co2"),avg("noise_db").alias("avg_noise"),count("*").alias("record_count"))

# Write aggregates to Gold (streaming)
query = df_gold.writeStream.format("delta").outputMode("complete").option("checkpointLocation", CHECKPOINT_PATH).start(GOLD_PATH)
# Complete mode is required for aggregations.

print("✅ Gold Layer is running: aggregating Silver data by zone and hour...")
query.awaitTermination()
