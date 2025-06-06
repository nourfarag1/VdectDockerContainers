# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container at /app
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Create static directory before copying into it, if it might not exist
RUN mkdir -p static

# Copy the rest of the application code including the static directory
COPY . .

# Expose the port the app runs on (matching UI_PORT in main.py, default 8000)
EXPOSE 8000

# Define environment variables (can be overridden by docker-compose)
ENV PYTHONUNBUFFERED=1
ENV RABBITMQ_HOST=rabbitmq
ENV MINIO_URL=minio:9000
ENV CAMERA_ID_TO_PROCESS=default_camera_id

# Run main.py using uvicorn when the container launches
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"] 