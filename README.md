# Containerized Job Postings Pipeline

![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-009688?style=for-the-badge&logo=fastapi&logoColor=white)

A containerized data pipeline that scrapes LinkedIn job postings, extracts skills using an LLM API, and loads structured data into PostgreSQL, all orchestrated through Docker Compose with Kafka for distributed messaging.

## Table of Contents
- [Purpose](#purpose)
- [Architecture](#architecture)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
- [API Endpoints](#api-endpoints)
- [Configuration](#configuration)
- [Contributing](#contributing)
- [License](#license)

## Purpose

This repository was created to sharpen my expertise in:
- Event-driven architecture using Apache Kafka
- Distributed system design patterns
- Docker Compose for multi-container orchestration
- PostgreSQL integration for structured data storage
- Asynchronous processing and message-based communication
- Best practices for scalable, production-ready data pipelines

## Architecture

This project implements a modular, event-driven pipeline architecture with the following components:

1. **Web UI Container**: Frontend interface for users to interact with the system
2. **FastAPI Backend Container**: API service that communicates with Kafka and PostgreSQL
3. **Kafka Container**: Message broker that facilitates communication between services
4. **Scraper Container**: Consumes scraping requests from Kafka, executes LinkedIn job data extraction, and produces updates
5. **Loader Container**: Consumes loading requests from Kafka and ingests data into PostgreSQL all while producing updates
6. **PostgreSQL Database Container**: Persistent storage for structured job and skill data
7. **Docker Volume**: Shared storage between scraper and loader containers

![System Architecture](assets/architecture.png)

## Features

- **Event-Driven Architecture**: Loosely coupled components communicating via Kafka topics
- **Distributed Processing**: Each component processes messages independently and asynchronously
- **Scalable Design**: Each component can be scaled horizontally to handle increased load
- **Real-time Updates**: Pipeline status updates are streamed through Kafka
- **LLM Integration**: Extracts skills from job descriptions using an LLM API
- **Docker Volume Storage**: Intermediate data persistence between containers
- **Centralized Logging**: Global logging system used across all containers - outputs to terminal in development mode and to log files in production

![Demo](assets/demo.gif)

## Prerequisites

- Docker
- Docker Compose

## Installation

1. Clone the repository:

    ```bash
    git clone https://github.com/mohamedhayballa22/docker-data-pipeline.git
    cd docker-data-pipeline
    ```
2. Configure the environment variables in .env file (see Configuration)
3. Build and start the containers:

    ```bash
    docker compose up
    ```

## Usage

Once the containers are running:

1. Access the Web UI by navigating to http://localhost:3000 in your browser
2. Use the interface to trigger job scraping or query existing job skills data
3. Monitor the process through the logs:

    ```bash
    docker compose logs -f
    ```

## API Endpoints

The FastAPI backend provides the following endpoints:

- `GET /health`: Returns API status and Kafka connection information
- `GET /data`: Queries the database to return jobs and skills data
- `POST /trigger-job-pipeline`: Initiates the scraping process
    - Accepts job parameters like job titles, location, and filters
    - Publishes a message to Kafka for the scraper to consume
- `GET /jobs/{job_id}/status`: Returns the current status of a specific job from the in-memory job store
- `DELETE /jobs/{job_id}`: Deletes the specified job by ID
- `PATCH /jobs/{job_id}/progress`: Updates the progress field of a specific job
- `WebSocket /ws`: Real-time connection for status updates
    - Sends initial job statuses upon connection
    - Streams real-time job progress and status updates

## Configuration

Configure the pipeline using the following environment variables in the .env file:

```.env
# Environment (dev or prod)
ENVIRONMENT=dev

# Database
POSTGRES_USER=your_username
POSTGRES_PASSWORD=your_password
POSTGRES_DB=your_database_name
DATABASE_URL=postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@db:5432/${POSTGRES_DB}

# Scraper
GOOGLE_API_KEY=your_api_key
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git switch -c feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

##  License

This project is open source and available for anyone to use and build upon.

## Project structure:

```text
├── docker-compose.yml
├── kafka-setup
├── logger
│   ├── __init__.py
│   └── logger.py
├── api
│   ├── Dockerfile
│   ├── kafka_client.py
│   ├── main.py
│   ├── models.py
│   ├── requirements.txt
│   └── websockets.py
├── db
│   └── init.sql
├── loader
│   ├── Dockerfile
│   ├── kafka_client.py
│   ├── loader.py
│   ├── models.py
│   └── requirements.txt
├── scraper
│   ├── Dockerfile
│   ├── kafka_client.py
│   ├── requirements.txt
│   └── scraper.py
└── web-ui
    ├── Dockerfile
    ├── index.html
    ├── package-lock.json
    ├── package.json
    └── src
        ├── app.js
        └── styles.css
```