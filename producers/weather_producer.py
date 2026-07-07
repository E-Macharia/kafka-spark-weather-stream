"""
Kafka Weather Producer.
Generates simulated weather statistics for cities and pushes to a Kafka topic.
Uses the shared config package for configurations.
"""
import time
import random
import json
import logging
import sys
import os
import socket
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Append project root directory to path to allow importing config package
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.append(project_root)

from config import settings

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class WeatherProducer:
    def __init__(self, bootstrap_servers: str, topic: str):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = None

    def connect(self, retries: int = 10, delay: int = 5) -> bool:
        """Attempts to connect to the Kafka broker with retries."""
        logger.info(f"Connecting to Kafka broker at {self.bootstrap_servers}...")
        for attempt in range(1, retries + 1):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    acks='all',
                    retries=3
                )
                logger.info("Producer successfully connected to Kafka broker!")
                return True
            except Exception as e:
                logger.warning(f"Connection attempt {attempt}/{retries} failed: {e}")
                if attempt < retries:
                    logger.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
        logger.error("Could not connect to Kafka broker. Exiting.")
        return False

    def generate_weather_data(self) -> dict:
        """Generates random city-specific weather metrics."""
        city = random.choice(settings.CITIES)
        temperature = round(random.uniform(settings.MIN_TEMP, settings.MAX_TEMP), 1)
        return {
            "city": city,
            "temperature": temperature,
            "timestamp": time.time()
        }

    def start_streaming(self):
        """Continuously streams weather events to the Kafka topic."""
        if not self.producer:
            if not self.connect():
                return

        logger.info(f"Starting weather data stream to topic '{self.topic}'...")
        try:
            while True:
                weather_data = self.generate_weather_data()
                
                # Send the message asynchronously
                future = self.producer.send(self.topic, value=weather_data)
                
                # Block until message is successfully acknowledged
                record_metadata = future.get(timeout=10)
                
                logger.info(
                    f"Sent: {weather_data['city']} - {weather_data['temperature']} C "
                    f"(Offset: {record_metadata.offset})"
                )
                
                time.sleep(settings.SIMULATION_INTERVAL_SECONDS)
        except KeyboardInterrupt:
            logger.info("Streaming manually terminated.")
        except Exception as e:
            logger.error(f"Unexpected streaming error: {e}")
        finally:
            self.close()

    def close(self):
        """Safely cleans up Kafka socket connection."""
        if self.producer:
            logger.info("Closing producer connection...")
            self.producer.close()
            logger.info("Producer closed.")

if __name__ == "__main__":
    # Ensure DNS resolution patches are applied for Windows loopbacks
    # (Since this script can also be run locally on the host)
    producer = WeatherProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        topic=settings.WEATHER_TOPIC
    )
    producer.start_streaming()
