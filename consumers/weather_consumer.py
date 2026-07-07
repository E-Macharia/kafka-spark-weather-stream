"""
Kafka Weather Consumer.
Listens to Kafka weather-topic and displays incoming events in real time.
"""
import json
import logging
import sys
import os
import time
from datetime import datetime
from kafka import KafkaConsumer

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

class WeatherConsumer:
    def __init__(self, bootstrap_servers: str, topic: str):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.consumer = None

    def connect(self, retries: int = 10, delay: int = 5) -> bool:
        """Attempts to connect to Kafka broker and subscribe with retries."""
        logger.info(f"Connecting to Kafka broker at {self.bootstrap_servers}...")
        for attempt in range(1, retries + 1):
            try:
                self.consumer = KafkaConsumer(
                    self.topic,
                    bootstrap_servers=self.bootstrap_servers,
                    auto_offset_reset='latest',
                    enable_auto_commit=True,
                    group_id='weather-monitoring-cli-group',
                    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
                )
                logger.info(f"Successfully subscribed to topic '{self.topic}'!")
                return True
            except Exception as e:
                logger.warning(f"Connection attempt {attempt}/{retries} failed: {e}")
                if attempt < retries:
                    logger.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
        logger.error("Could not connect to Kafka broker. Exiting.")
        return False

    def start_consuming(self):
        """Polls messages from Kafka in a loop and prints them."""
        if not self.consumer:
            if not self.connect():
                return

        logger.info("Waiting for real-time weather reports...")
        try:
            for message in self.consumer:
                weather = message.value
                timestamp_raw = weather.get("timestamp", time.time())
                time_str = datetime.fromtimestamp(timestamp_raw).strftime('%Y-%m-%d %H:%M:%S')

                print("\n" + "=" * 40)
                print("     REAL-TIME WEATHER UPDATE")
                print(f"  City:        {weather.get('city')}")
                print(f"  Temperature: {weather.get('temperature')} C")
                print(f"  Timestamp:   {time_str}")
                print("=" * 40)
        except KeyboardInterrupt:
            logger.info("Consumer terminated by user.")
        except Exception as e:
            logger.error(f"Unexpected error during consumption: {e}")
        finally:
            self.close()

    def close(self):
        """Closes Kafka connections safely."""
        if self.consumer:
            logger.info("Closing consumer...")
            self.consumer.close()
            logger.info("Consumer closed.")

if __name__ == "__main__":
    consumer = WeatherConsumer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        topic=settings.WEATHER_TOPIC
    )
    consumer.start_consuming()
