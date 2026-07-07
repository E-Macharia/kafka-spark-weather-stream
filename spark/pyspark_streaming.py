"""
PySpark Structured Streaming Job.
Consumes weather telemetry from Kafka, performs real-time analytics,
identifies anomalies (Extreme Heat & Freezing), and stores raw data, 
aggregated average metrics, and alerts into a PostgreSQL database.
"""
import time
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from config import settings

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Define schema matching our weather telemetry payload
WEATHER_SCHEMA = StructType([
    StructField("city", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("timestamp", DoubleType(), True)
])

DB_URL = settings.DB_JDBC_URL
DB_PROPERTIES = {
    "user": settings.DB_USER,
    "password": settings.DB_PASSWORD,
    "driver": "org.postgresql.Driver"
}

# Automatically enable SSL for cloud database hosts like Neon or Supabase
if "neon.tech" in DB_URL or "supabase" in DB_URL or "aws.neon.tech" in DB_URL:
    DB_PROPERTIES["ssl"] = "true"
    DB_PROPERTIES["sslmode"] = "require"

def process_batch(batch_df, batch_id):
    """
    Callback function that processes each micro-batch.
    Writes raw records, computes metrics, and generates alerts.
    """
    logger.info(f"Processing micro-batch ID: {batch_id}")
    
    if batch_df.count() == 0:
        logger.info("Batch is empty. Skipping.")
        return

    # Cache the dataframe since we execute multiple actions on it
    batch_df.persist()

    try:
        # 1. Write raw stream data to PostgreSQL 'raw_weather' table
        logger.info(f"Writing {batch_df.count()} raw records to PostgreSQL...")
        batch_df.write.jdbc(
            url=DB_URL,
            table="raw_weather",
            mode="append",
            properties=DB_PROPERTIES
        )

        # 2. Filter anomalies: Extreme Heat (> 35.0 C) and Freeze Alerts (< 0.0 C)
        alerts_df = batch_df.filter((col("temperature") > 35.0) | (col("temperature") < 0.0)) \
            .withColumn("alert_type", expr("CASE WHEN temperature > 35.0 THEN 'Extreme Heat Alert' ELSE 'Freeze Alert' END"))
        
        alert_count = alerts_df.count()
        if alert_count > 0:
            logger.info(f"🚨 Detected {alert_count} weather anomalies! Writing to database...")
            alerts_df.select("city", "temperature", "alert_type", "timestamp") \
                .write.jdbc(
                    url=DB_URL,
                    table="weather_alerts",
                    mode="append",
                    properties=DB_PROPERTIES
                )

        # 3. Calculate running average temperatures per city and overwrite 'weather_metrics'
        # Read all historical raw weather entries to calculate a true running average across the entire dataset
        logger.info("Recalculating average temperatures per city...")
        
        # Read historical data from raw_weather
        historical_df = batch_df.sql_ctx.read.jdbc(
            url=DB_URL,
            table="raw_weather",
            properties=DB_PROPERTIES
        )
        
        # Compute aggregate average metrics
        metrics_df = historical_df.groupBy("city") \
            .agg(avg("temperature").alias("avg_temperature")) \
            .withColumn("last_updated", expr("CURRENT_TIMESTAMP"))

        # Overwrite weather_metrics table with latest aggregates
        metrics_df.write.jdbc(
            url=DB_URL,
            table="weather_metrics",
            mode="overwrite",
            properties=DB_PROPERTIES
        )
        logger.info("Metrics successfully updated.")

    except Exception as e:
        logger.error(f"Error processing micro-batch {batch_id}: {e}")
    finally:
        # Unpersist dataframe to release memory resources
        batch_df.unpersist()

def main():
    logger.info("Starting PySpark Structured Streaming Session...")
    
    # Initialize Spark Session with Kafka & PostgreSQL dependencies
    spark = SparkSession.builder \
        .appName("WeatherDataStreamingJob") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.2") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    # Read the real-time weather stream from Kafka
    logger.info(f"Subscribing to Kafka topic '{settings.WEATHER_TOPIC}'...")
    kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", settings.KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", settings.WEATHER_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # Convert binary payload key/value to readable strings and parse json schema
    parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING) as json_val") \
        .select(from_json(col("json_val"), WEATHER_SCHEMA).alias("data")) \
        .select("data.*")

    # Convert epoch double timestamp to Timestamp type
    processed_stream = parsed_stream.withColumn("timestamp", col("timestamp").cast("timestamp"))

    # Define write stream query using the foreachBatch processing logic
    query = processed_stream.writeStream \
        .foreachBatch(process_batch) \
        .trigger(processingTime="5 seconds") \
        .start()

    logger.info("Streaming query started. Processing incoming telemetry...")
    query.awaitTermination()

if __name__ == "__main__":
    main()
