# Use Python 3.10 as base
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
# Note: Since there is no explicit requirements.txt, I'll generate one or assume basic deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . .

# Expose port for the log server
EXPOSE 5001

# Command to run both the bot and the log server
# We use a simple shell script to start both
CMD ["sh", "-c", "python3 serve_logs.py & python3 run_live.py"]
