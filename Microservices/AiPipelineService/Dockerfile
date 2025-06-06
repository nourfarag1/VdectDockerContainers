# Base Python image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install FFmpeg
RUN apt-get update && apt-get install -y ffmpeg && rm -rf /var/lib/apt/lists/*

# Copy Python dependency files and install dependencies
COPY requirements.txt .
# It's good practice to install dependencies before copying the rest of the code
# to leverage Docker layer caching if requirements don't change.
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Expose the port the app runs on
# This should match the port Uvicorn will use inside the container.
# The actual port mapping to the host is done in docker-compose.yaml.
EXPOSE 8000

# Command to run the application using Uvicorn
# Make sure 'main:app' matches your Python file name (main.py) and FastAPI instance name (app).
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
