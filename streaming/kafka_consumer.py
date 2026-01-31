from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, DoubleType, StringType


servers="localhost:9092" 
topic="sensor-data-medallion"


def get_kafka_stream(
    spark,
    topics: str=topic,
    servers: str=servers,
    starting_offsets: str = "earliest",
    consumer_group: str = "spark_env_air_bronze",
):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", servers)
        .option("subscribe", topics)

        # first run only: earliest. after that, checkpoint controls progress
        .option("startingOffsets", starting_offsets)

        #.option("startingOffsets", latest_offsets)


        # do not die if offsets aged out due to retention
        .option("failOnDataLoss", "false")

        # throttle, prevents bursty producers from killing the job
        .option("maxOffsetsPerTrigger", 200000)

        # better consumer behavior under load
        .option("kafka.group.id", consumer_group)
        .option("kafka.session.timeout.ms", "45000")
        .option("kafka.request.timeout.ms", "60000")
        .option("kafka.max.poll.interval.ms", "300000")
        .option("kafka.fetch.max.bytes", str(50 * 1024 * 1024))

        .load()
    ) 