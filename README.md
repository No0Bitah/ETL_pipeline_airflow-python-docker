# ETL Pipeline with Airflow, Python, and Docker

This repository contains an ETL (Extract, Transform, Load) data pipeline built using Apache Airflow, Python, and Docker. The pipeline extracts COVID-19 data from the European Centre for Disease Prevention and Control (ECDC), transforms it, and loads it into a PostgreSQL database.

## Overview

This project demonstrates a complete ETL workflow orchestrated by Apache Airflow and containerized with Docker. The pipeline processes COVID-19 case and death data for European countries, providing insights into the pandemic's progression.

## Architecture

The project is built with the following components:

- **Apache Airflow**: Workflow orchestration platform
- **Python**: Programming language for data processing
- **Docker & Docker Compose**: Containerization for consistent environments
- **PostgreSQL**: Database for storing processed data
- **Psycopg2**: PostgreSQL adapter for Python

## Data Source

The data is extracted from the European Centre for Disease Prevention and Control (ECDC), which provides COVID-19 statistics, including:

- Daily case counts
- Death counts
- Country-specific data
- Date information

The pipeline specifically targets the publicly available COVID-19 dataset from ECDC, which is accessed via their API.

## Project Structure

```
ETL_pipeline_airflow-python-docker/
├── dags/
│   ├── covid_pipeline_dag.py    # Main Airflow DAG definition
│   └── utils/
│       ├── countries.py         # Country code utilities
│       ├── dates.py             # Date handling functions
│       └── transformers.py      # Data transformation logic
├── docker-compose.yml           # Docker configuration
├── Dockerfile                   # Docker image definition
├── requirements.txt             # Python dependencies
└── scripts/
    ├── init.sh                  # Initialization script
    └── entrypoint.sh            # Docker entrypoint
```

## Workflow Steps

The ETL pipeline consists of the following steps:

1. **Extract**: Fetch COVID-19 data from ECDC API
2. **Transform**: Clean and process the data
   - Filter European countries
   - Format dates
   - Calculate additional metrics
3. **Load**: Store processed data in PostgreSQL database
4. **Visualize**: Generate basic visualizations (if enabled)

## Setup and Installation

### Prerequisites

- Docker and Docker Compose
- Git

### Installation Steps

1. Clone the repository:
   ```
   git clone https://github.com/No0Bitah/ETL_pipeline_airflow-python-docker.git
   cd ETL_pipeline_airflow-python-docker
   ```

2. Build and start the containers:
   ```
   docker-compose up -d
   ```

3. Access Airflow web interface:
   ```
   http://localhost:8080
   ```
   Default credentials:
   - Username: airflow
   - Password: airflow

4. Trigger the DAG manually or wait for the scheduled run

## Configuration

The project can be configured by modifying:

- Environment variables in the `docker-compose.yml` file
- DAG parameters in `dags/covid_pipeline_dag.py`

## Database Schema

The processed COVID-19 data is stored in a table with the following structure:

| Column        | Type    | Description                          |
|---------------|---------|--------------------------------------|
| country_code  | VARCHAR | ISO country code                     |
| country       | VARCHAR | Country name                         |
| date          | DATE    | Report date                          |
| cases         | INTEGER | Daily reported cases                 |
| deaths        | INTEGER | Daily reported deaths                |
| cumulative_cases | INTEGER | Total cases up to date            |
| cumulative_deaths | INTEGER | Total deaths up to date          |
| updated_at    | TIMESTAMP | Last update timestamp              |

## License

This project is available under the MIT License.

## Acknowledgments

- European Centre for Disease Prevention and Control for providing the COVID-19 data
- Apache Airflow community
- Docker community

## Contributors

- [No0Bitah](https://github.com/No0Bitah)
