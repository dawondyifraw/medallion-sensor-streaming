from pyspark.sql import SparkSession
import os
print("Current working directory:", os.getcwd())
# Start or attach to a SparkSession
spark = SparkSession.builder \
    .appName("InspectGold") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

import os
df_silver = spark.read.format("delta").load("data/silver/sensor_data_medallion")
df_silver.groupBy("zone", "hour").count().orderBy("zone", "hour").show()
df_silver.show()


# Bronze sanity check (raw values)
df_bronze = spark.read.format("delta").load("data/bronze/sensor_data_medallion")
df_bronze.select("latitude", "longitude", "timestamp").show()

# Silver sanity check (enrichment)
df_silver = spark.read.format("delta").load("data/silver/sensor_data_medallion")
df_silver.groupBy("zone").count().show()
df_silver.select("zone", "hour", "co2_ppm").orderBy("zone", "hour").show()

# Gold sanity check (aggregates)
df_gold = spark.read.format("delta").load("data/gold/sensor_data_medallion")
df_gold.orderBy("zone", "hour").show()
