# Containerized Job Postings Pipeline

![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-009688?style=for-the-badge&logo=fastapi&logoColor=white)

A containerized data pipeline that scrapes LinkedIn job postings, extracts skills using LLM processing, and loads structured data into PostgreSQL, all orchestrated through Docker Compose.

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

This repository was created to experiment with:
- Pipeline architecture and data flow patterns
- Docker Compose for multi-container orchestration
- PostgreSQL integration for structured data storage
- Best practices for scalable end-to-end data projects

## Architecture

This project implements a modular, containerized data pipeline with the following components:

1. **Web UI Container**: Frontend interface for users to interact with the system
2. **Backend Container**: FastAPI service that triggers the scraping process and handles database queries
3. **Scraper Container**: Executes LinkedIn job data scraping operations and persists data to a Docker Volume
4. **Loader Container**: Ingests scraped data and loads it into PostgreSQL
5. **Database Container**: Houses the PostgreSQL database with job and skill data

![System Architecture](assets/architecture.png)

## Features

- **Modular Architecture**: Each component runs in an isolated container with clear responsibilities
- **Data Flow**: Structured pipeline from extraction to database storage
- **Background Processing**: API triggers scraping as background tasks to avoid blocking
- **LLM Integration**: Extracts skills from job descriptions using an LLM API
- **Scalable Design**: Components can be scaled independently as needed
- **Docker Volume Storage**: Intermediate data persistence between containers
- **Centralized Logging**: Global logging system used across all containers - outputs to terminal in development mode and to log files in production

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

The FastAPI backend provides two main endpoints:

- GET /data: Query the database for job and skill data
- Returns information from both jobs and jobskills tables
- POST /trigger-job-pipeline": Initiates the scraping process
    - Runs as a background task to prevent blocking
    - Communicates with the scraper container via Docker SDK

## Configuration

Configure the pipeline using the following environment variables in the .env file:

```.env
# Environment
ENVIRONMENT=dev

# Database
POSTGRES_USER=your_username
POSTGRES_PASSWORD=your_password
POSTGRES_DB=your_database_name
DATABASE_URL=postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@db:5432/${POSTGRES_DB}

# Scraper
GOOGLE_API_KEY=your_api_key
JOB_TITLES=software_engineer,data_scientist
LOCATION=Paris
TIME_FILTER="24h" #"24h", "1w" or "1m"
NUM_PAGES=5
MAX_JOBS=100
OUTPUT_DIR=/data
OUTPUT_FILENAME=jobs.json
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
.
├── docker-compose.yml
├── api
│   ├── Dockerfile  
│   ├── docker_sdk.py
│   ├── main.py
│   ├── models.py
│   └── requirements.txt
├── db
│   └── init.sql
├── loader
│   ├── Dockerfile
│   ├── loader.py
│   ├── models.py
│   └── requirements.txt
├── logger
│   ├── __init__.py
│   └── logger.py
├── scraper
│   ├── Dockerfile
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