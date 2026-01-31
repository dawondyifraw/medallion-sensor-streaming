from pyspark.sql import SparkSession
from delta.tables import DeltaTable

BRONZE_PATH = "data/bronze/sensor_data_medallion"

spark = SparkSession.builder \
    .appName("ReadDeltaBronze") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Wait for JARs to load (handy in dev)
spark.sparkContext.setLogLevel("INFO")

# Load the Delta table
delta_table = DeltaTable.forPath(spark, BRONZE_PATH)
delta_table.toDF().show()


# CLI run example
''' spark-submit \
  --packages io.delta:delta-spark_2.12:3.0.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  scripts/spark_reader.py '''


# Run with PYSPARK_SUBMIT_ARGS="--packages io.delta:delta-spark_2.12:3.0.0 pyspark-shell" python scripts/spark_reader.py
