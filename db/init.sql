-- SQL Initialization script for PostgreSQL
-- Automatically run when the DB container starts up

-- 1. Raw weather updates stream log
CREATE TABLE IF NOT EXISTS raw_weather (
    id SERIAL PRIMARY KEY,
    city VARCHAR(50) NOT NULL,
    temperature NUMERIC(5, 2) NOT NULL,
    timestamp TIMESTAMP NOT NULL
);

-- 2. Aggregate metrics table containing running average temperature per city
CREATE TABLE IF NOT EXISTS weather_metrics (
    city VARCHAR(50) PRIMARY KEY,
    avg_temperature NUMERIC(5, 2) NOT NULL,
    last_updated TIMESTAMP NOT NULL
);

-- 3. High-priority weather anomaly alerts (Freeze alerts & Extreme Heat alerts)
CREATE TABLE IF NOT EXISTS weather_alerts (
    id SERIAL PRIMARY KEY,
    city VARCHAR(50) NOT NULL,
    temperature NUMERIC(5, 2) NOT NULL,
    alert_type VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP NOT NULL
);
