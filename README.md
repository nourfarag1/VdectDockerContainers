# Real-Time Violence Detection - AI Microservices

This repository contains the containerized Python-based microservices for the Real-Time Violence Detection System. These services are responsible for consuming video streams, processing data chunks, and running AI models for inference.

**Note:** This repository is a component of a larger project. It is not intended to be run standalone. The entire system is orchestrated by the main backend API.

---

### ➡️ [View the Main Project Hub & Full Documentation Here](https://github.com/nourfarag1/Real-Time-Violence-Detection-API) ⬅️

---

## Services Included

*   **AIWorker:** Consumes video chunk tasks from a RabbitMQ queue, runs the AI model for inference, and uploads results.
*   **AIResultHandler:** Listens for results from the AI model and creates the final incident notifications.
*   **AIPipelineService:** Interacts directly with the RTMP stream server to manage and segment the live video.

## Tech Stack

*   **Language:** Python
*   **Framework:** Flask
*   **AI/CV:** OpenCV
*   **Containerization:** Docker

## Usage

These services are automatically built and launched via the `docker-compose.yml` file.
