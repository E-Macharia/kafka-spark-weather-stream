FROM python:3.10-slim

WORKDIR /app

# Install dependencies first for Docker caching
COPY producers/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all project files to allow importing shared modules
COPY . .

# Set Python Path to resolve config packages correctly
ENV PYTHONPATH=/app

# Run python in unbuffered mode to emit logs in real-time
CMD ["python", "-u", "-m", "producers.weather_producer"]
