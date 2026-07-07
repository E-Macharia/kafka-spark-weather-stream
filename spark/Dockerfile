FROM apache/spark:3.5.1

USER root

WORKDIR /app

# Copy requirements and project files
COPY spark/requirements.txt .
COPY . .

# Set environment paths
ENV PYTHONPATH=/app

# Submit the Spark job with necessary Kafka and PostgreSQL Maven dependencies
CMD ["/opt/spark/bin/spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.2", "spark/pyspark_streaming.py"]
