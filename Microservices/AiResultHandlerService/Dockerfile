# Use a Python base image
FROM python:3.9-slim

# Install ffmpeg, which is essential for video merging
RUN apt-get update && apt-get install -y ffmpeg

# Set the working directory in the container
WORKDIR /app

# Copy the dependencies file to the working directory
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application's code to the working directory
COPY . .

# Set environment variable for unbuffered output
ENV PYTHONUNBUFFERED=1

# Expose the port for the Uvicorn web server
EXPOSE 5003

# The command to run the application using Uvicorn
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "5003"] 