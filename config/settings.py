"""
Shared configuration parameters for the Weather Data Engineering Pipeline.
Detects if running inside Docker or locally and adapts endpoints.
"""
import os
import socket

# Helper to check if running inside Docker
def is_docker() -> bool:
    path = '/proc/self/cgroup'
    return (
        os.path.exists('/.dockerenv') or
        (os.path.exists(path) and any('docker' in line for line in open(path)))
    )

# Kafka Configuration
# If running in Docker compose network, Kafka host is 'kafka:9092'.
# If running locally on Windows, Kafka host is '127.0.0.1:9092'.
DEFAULT_KAFKA = "kafka:9092" if is_docker() else "127.0.0.1:9092"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_KAFKA)
WEATHER_TOPIC = os.getenv("WEATHER_TOPIC", "weather-topic")

# PostgreSQL Database Configuration
DB_HOST = os.getenv("DB_HOST", "postgres" if is_docker() else "127.0.0.1")
DB_PORT = os.getenv("DB_PORT", "5432" if is_docker() else "5433")
DB_NAME = os.getenv("DB_NAME", "weather_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

# JDBC Connection URL (for Spark Structured Streaming)
DB_JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

# SQLAlchemy/Psycopg2 URL (for Streamlit dashboard)
DB_SQL_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Weather Simulation Settings
SIMULATION_INTERVAL_SECONDS = float(os.getenv("SIMULATION_INTERVAL_SECONDS", "2.0"))
CITIES = [
    "Nairobi", "Mombasa", "Kisumu", "New York", 
    "London", "Tokyo", "Paris", "Sydney", "Berlin", "Mumbai"
]

# Temperature ranges to generate realistic data (in Celsius)
MIN_TEMP = -5.0
MAX_TEMP = 40.0
