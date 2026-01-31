from pyspark.sql import SparkSession

# Initialize a Spark session for quick tests
spark = SparkSession.builder \
    .appName("KafkaToBronze") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "io.delta:delta-spark_2.12:3.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

print("Spark is summoned and ready to obey some commands")
          
BRONZE_PATH = "data/bronze/sensor_data_medallion"

from delta.tables import DeltaTable
delta_table = DeltaTable.forPath(spark, BRONZE_PATH)
delta_table.toDF().show()
