FROM python:3.10-slim

WORKDIR /app

# Install dependencies first for Docker caching
COPY dashboard/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all project files to allow importing shared modules
COPY . .

# Set Python Path to resolve config packages correctly
ENV PYTHONPATH=/app

# Expose Streamlit default port
EXPOSE 8501

# Run Streamlit serving on address 0.0.0.0
CMD ["streamlit", "run", "dashboard/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
