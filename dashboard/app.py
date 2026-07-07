"""
Streamlit Live Weather Dashboard.
Connects to PostgreSQL to read raw data, averages, and active alerts.
Visualizes the data in real-time using Plotly charts and auto-refreshes.
"""
import sys
import os
import time
import pandas as pd
import streamlit as st
import plotly.express as px
from sqlalchemy import create_engine

# Append project root directory to path to allow importing config package
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.append(project_root)

from config import settings

# Page Configuration for Premium Dashboard Look
st.set_page_config(
    page_title="Real-Time Weather Streaming Dashboard",
    page_icon="🌦️",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Custom Styling (Dark Glassmorphism UI vibes)
st.markdown("""
    <style>
    .main {
        background-color: #0e1117;
    }
    .metric-card {
        background-color: #1f2937;
        padding: 20px;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        text-align: center;
        border: 1px solid #374151;
    }
    .metric-val {
        font-size: 32px;
        font-weight: bold;
        color: #60a5fa;
    }
    .metric-title {
        font-size: 14px;
        color: #9ca3af;
        text-transform: uppercase;
        margin-bottom: 5px;
    }
    </style>
""", unsafe_allow_html=True)

# Initialize SQLAlchemy Database Connection Engine
@st.cache_resource
def get_db_engine():
    # settings.DB_SQL_URL resolves host to 'postgres' if inside docker, '127.0.0.1' if local
    return create_engine(settings.DB_SQL_URL)

engine = get_db_engine()

def load_data(query: str) -> pd.DataFrame:
    """Safely loads data from the database into a Pandas DataFrame."""
    try:
        with engine.connect() as conn:
            return pd.read_sql(query, conn)
    except Exception as e:
        # Fallback empty dataframe on startup if table doesn't exist yet
        return pd.DataFrame()

# Main Dashboard App Header
st.title("🌦️ Real-Time Weather Event Streaming Pipeline")
st.markdown("---")

# 1. Fetch raw logs, city metrics, and alerts from DB
raw_df = load_data("SELECT * FROM raw_weather ORDER BY timestamp DESC LIMIT 50")
metrics_df = load_data("SELECT * FROM weather_metrics ORDER BY city ASC")
alerts_df = load_data("SELECT * FROM weather_alerts ORDER BY timestamp DESC LIMIT 20")
total_logs_df = load_data("SELECT COUNT(*) as total FROM raw_weather")

total_logs = int(total_logs_df.iloc[0]["total"]) if not total_logs_df.empty else 0
total_alerts = len(alerts_df)

# --- ROW 1: Metrics KPI Cards ---
col1, col2, col3 = st.columns(3)

with col1:
    st.markdown(f"""
        <div class="metric-card">
            <div class="metric-title">📊 Total Telemetry Received</div>
            <div class="metric-val">{total_logs:,}</div>
        </div>
    """, unsafe_allow_html=True)
    
with col2:
    avg_temp = round(metrics_df["avg_temperature"].mean(), 1) if not metrics_df.empty else 0.0
    st.markdown(f"""
        <div class="metric-card">
            <div class="metric-title">🌡️ Global Average Temp</div>
            <div class="metric-val">{avg_temp} C</div>
        </div>
    """, unsafe_allow_html=True)
    
with col3:
    alert_color = "#ef4444" if total_alerts > 0 else "#10b981"
    st.markdown(f"""
        <div class="metric-card">
            <div class="metric-title">🚨 Active Anomalies</div>
            <div class="metric-val" style="color: {alert_color}">{total_alerts}</div>
        </div>
    """, unsafe_allow_html=True)
    
st.write("") # Spacing

# --- ROW 2: Charts (Side by Side) ---
chart_col1, chart_col2 = st.columns(2)

with chart_col1:
    st.subheader("📍 Avg Temperature per City")
    if not metrics_df.empty:
        fig_bar = px.bar(
            metrics_df,
            x="city",
            y="avg_temperature",
            labels={"avg_temperature": "Average Temperature (C)", "city": "City"},
            color="avg_temperature",
            color_continuous_scale="RdBu_r"
        )
        fig_bar.update_layout(template="plotly_dark", height=320, margin=dict(l=20, r=20, t=20, b=20))
        st.plotly_chart(fig_bar, use_container_width=True, key="bar_chart")
    else:
        st.info("Waiting for Spark stream to populate aggregates...")
        
with chart_col2:
    st.subheader("📈 Live Telemetry Stream (Last 30 Records)")
    if not raw_df.empty:
        # Reverse dataframe order for chronological plotting
        trend_df = raw_df.head(30).iloc[::-1]
        fig_line = px.line(
            trend_df,
            x="timestamp",
            y="temperature",
            color="city",
            labels={"temperature": "Temperature (C)", "timestamp": "Event Time"},
        )
        fig_line.update_layout(template="plotly_dark", height=320, margin=dict(l=20, r=20, t=20, b=20))
        st.plotly_chart(fig_line, use_container_width=True, key="line_chart")
    else:
        st.info("Waiting for data events from Kafka...")

# --- ROW 3: Alerts & Raw logs ---
sec_col1, sec_col2 = st.columns(2)

with sec_col1:
    st.subheader("🚨 Active Alerts (Extreme weather)")
    if not alerts_df.empty:
        # Highlight alerts in red
        st.dataframe(
            alerts_df[["city", "temperature", "alert_type", "timestamp"]],
            use_container_width=True,
            height=200
        )
    else:
        st.success("No extreme weather events detected. Everything is normal!")
        
with sec_col2:
    st.subheader("📜 Raw Stream Logs (Latest 20 Events)")
    if not raw_df.empty:
        st.dataframe(
            raw_df[["city", "temperature", "timestamp"]].head(20),
            use_container_width=True,
            height=200
        )
    else:
        st.info("Waiting for raw events...")
        
# Sleep 2 seconds before refreshing query data
time.sleep(2)
st.rerun()
