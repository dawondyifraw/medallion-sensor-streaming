from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_timestamp, hour
from pyspark.sql.types import DoubleType, StringType, StructType

# Delta Lake paths
BRONZE_PATH = "data/bronze/sensor_data_medallion"
SILVER_PATH = "data/silver/sensor_data_medallion"

# Spark session with Delta + Kafka support
spark = SparkSession.builder \
    .appName("BronzeToSilver") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "io.delta:delta-spark_2.12:3.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

# Stream Bronze data
df_bronze_stream = spark.readStream.format("delta").load(BRONZE_PATH)

# Extract hour for time-based analysis (rush hours, quiet zones, etc.)
df_with_hour = df_bronze_stream.withColumn("timestamp_parsed", to_timestamp(col("timestamp"))) \
                               .withColumn("hour", hour(col("timestamp_parsed")))

# Basic quality filters for sensor ranges
df_cleaned = df_with_hour.dropna(subset=["co2_ppm", "noise_db", "temperature_c", "latitude", "longitude"]) \
    .filter((col("co2_ppm") > 0) & (col("co2_ppm") < 10000)) \
    .filter((col("noise_db") > 10) & (col("noise_db") < 150))

# Time-of-day label for grouping
df_with_daytime = df_cleaned.withColumn(
    "time_of_day",
    when((col("hour") >= 5) & (col("hour") < 12), "morning")
    .when((col("hour") >= 12) & (col("hour") < 17), "afternoon")
    .when((col("hour") >= 17) & (col("hour") < 21), "evening")
    .otherwise("night")
)

# Human-friendly pollution categories for dashboards/alerts
df_with_pollution = df_with_daytime.withColumn(
    "pollution_category",
    when((col("co2_ppm") < 400) & (col("noise_db") < 50), "Clean & Quiet")
    .when((col("co2_ppm") < 800) & (col("noise_db") < 70), "Moderate")
    .otherwise("High Impact")
)

# Zone labels based on bounding boxes (demo geo-tagging)
df_enriched = df_with_pollution.withColumn(
    "zone",
    when((col("latitude") >= 51.728) & (col("latitude") <= 51.730) &
         (col("longitude") >= 5.360) & (col("longitude") <= 5.364), "residential")
    .when((col("latitude") >= 51.726) & (col("latitude") <= 51.728) &
         (col("longitude") >= 5.360) & (col("longitude") <= 5.366), "industrial")
    .when((col("latitude") >= 51.729) & (col("latitude") <= 51.731) &
         (col("longitude") >= 5.365) & (col("longitude") <= 5.368), "construction")
    .otherwise("unknown")
)

# Write enriched stream to Silver
# One-time bootstrap to create the Silver schema if needed.
'''df_enriched.limit(1).write \
    .format("delta") \
    .mode("overwrite") \
    .save("data/silver/sensor_data_medallion")'''

query = df_enriched.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", SILVER_PATH) \
    .start(SILVER_PATH)

print("Silver stream is running: cleaning and enriching data from Bronze layer...")

query.awaitTermination()

# Optional spark-submit:
# spark-submit --driver-memory 4G --executor-memory 4G scripts/bronze_to_silver.py
# spark-submit --packages io.delta:delta-spark_2.12:3.0.0 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog scripts/spark_reader.py
