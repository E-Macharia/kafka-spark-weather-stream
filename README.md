# 🌦️ End-to-End Weather Data Engineering Pipeline
### Real-Time Event Streaming with Apache Kafka, PySpark, PostgreSQL, and Streamlit

This project is a modern, production-style real-time data engineering pipeline. It simulates real-time weather telemetry from a sensor array, streams the events through a message broker, processes and aggregates the data using a distributed stream-processing framework, persists the results in a relational database, and visualizes the streaming metrics on an interactive web dashboard.

---

## 🏗️ Architecture & Data Flow

Below is the conceptual layout of the pipeline. All components are containerized and orchestrated seamlessly using **Docker Compose**:

```
┌─────────────────┐      ┌──────────────┐      ┌──────────────────┐      ┌─────────────┐
│ Weather Sensor  │ ───> │ Apache Kafka │ ───> │ PySpark Structured│ ───> │ PostgreSQL  │
│ (Python Worker) │      │ (Broker node)│      │ Streaming Job    │      │ Data Store  │
└─────────────────┘      └──────────────┘      └──────────────────┘      └─────────────┘
                                                                                │
                                                                                ▼
                                                                         ┌─────────────┐
                                                                         │  Streamlit  │
                                                                         │  Dashboard  │
                                                                         └─────────────┘
```

1. **Simulated Weather Sensor (Producer)**: A Python service that generates random city weather updates (temperature, timestamp) and writes them to a Kafka topic.
2. **Apache Kafka Broker**: A distributed, standalone event store running in **KRaft mode** (Zookeeper-less) that manages the `"weather-topic"` buffer.
3. **Structured Stream Processor (Spark)**: A PySpark application that consumes events from Kafka, detects high-temperature anomalies (alerts), calculates rolling average temperatures per city, and writes to PostgreSQL.
4. **Relational Database (PostgreSQL)**: Stores raw telemetry logs (`raw_weather`), running statistical averages (`weather_metrics`), and high-priority anomalies (`weather_alerts`).
5. **Interactive Dashboard (Streamlit)**: Connects to the database and serves Plotly charts displaying live trends, average temperatures, and reactive alert boards, auto-refreshing every 2 seconds.

---

## 📁 Repository Structure

```
kafka-weather-project/
├── config/
│   ├── __init__.py
│   └── settings.py          # Environment-aware connection settings & limits
├── db/
│   └── init.sql             # Auto-run SQL schema to construct tables
├── producers/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── weather_producer.py  # Python weather data generator
├── consumers/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── weather_consumer.py  # Optional CLI tool to inspect Kafka events
├── spark/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── pyspark_streaming.py # Spark Structured Streaming processor
├── dashboard/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app.py               # Streamlit web visualization app
├── docker-compose.yml       # Docker Compose service manifest
└── README.md                # This setup guide
```

---

## 🚀 Setup & Launch Instructions

### Prerequisites
- Install [Docker Desktop](https://www.docker.com/products/docker-desktop/) on your Windows host.
- Make sure Docker Desktop is running.

### 1. Launch the Pipeline (Docker Compose)
Open a terminal in the root of the project directory and execute:
```bash
docker compose up --build -d
```
*(This builds the python and Spark images, retrieves the database and Kafka packages, starts all services in the background, and runs the Spark job).*

### 2. Verify Container Health
Run the following to check that all containers are running successfully:
```bash
docker compose ps
```
You should see 5 healthy containers:
- `kafka` (Port 9092)
- `postgres` (Port 5432)
- `weather_producer`
- `spark_streaming_job`
- `streamlit_dashboard` (Port 8501)

### 3. Open the Dashboard
Open your web browser and go to:
```text
http://localhost:8501
```
Here, you'll watch live metrics cards, real-time Plotly charts, and alerts update automatically as the stream flows!

---

## 🔍 Optional Local Testing

If you want to manually run scripts or check outputs on your host machine:

### Inspect the Raw Kafka Stream
If you have Python installed locally, you can run our CLI consumer to monitor the Kafka messages directly from the broker:
1. Install dependencies locally:
   ```bash
   pip install -r consumers/requirements.txt
   ```
2. Start the consumer:
   ```bash
   python -m consumers.weather_consumer
   ```

### Check Database Content Directly
You can connect to the PostgreSQL database from any database client (such as DBeaver or pgAdmin) using:
- **Host**: `localhost`
- **Port**: `5432`
- **Database**: `weather_db`
- **Username**: `postgres`
- **Password**: `postgres`

Or view tables via the command line inside the container:
```bash
docker exec -it postgres psql -U postgres -d weather_db -c "SELECT * FROM weather_alerts LIMIT 10;"
```

---

## ⚙️ Configuration & Customization

You can fine-tune the pipeline properties by modifying `config/settings.py`:
- `SIMULATION_INTERVAL_SECONDS`: Speed up or slow down weather generation.
- `CITIES`: Add or remove cities from the simulator.
- `MIN_TEMP` / `MAX_TEMP`: Adjust the temperature boundaries.
- Spark alert threshold parameters can be adjusted inside `spark/pyspark_streaming.py` (defaults to `< 0.0` or `> 35.0`).

---

## 🧹 Tearing Down
To stop the streaming pipeline and destroy the containers and their networking, run:
```bash
docker compose down -v
```
*(The `-v` flag deletes the container volumes, resetting the database tables for a fresh start).*
