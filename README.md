# Zomato Data Engineering Pipeline

A full-stack data engineering project built around the Zomato dataset, combining ingestion, transformation, orchestration, AI enrichment, and an analytics layer for restaurant performance and customer insights.

## Overview

This project demonstrates a modern data stack using:

- Python for pipelines and AI tasks
- Airflow for batch orchestration
- Snowflake as the warehouse layer
- dbt for staging and marts models
- Google Gemini for review enrichment and SQL generation
- Streamlit for interactive data exploration and natural-language querying

## Architecture

- `ai/` — AI utilities for review sentiment enrichment, retrieval, and SQL generation
- `airflow/` — orchestration, DAGs, Docker setup, and pipeline tasks
- `snowflake/` — Snowflake setup scripts for integrations, stages, raw tables, and loading
- `zomato/` — dbt project with models, tests, and sources
- `data/` — sample dataset used for local experiments and demos
- `aws/` — IAM policy examples for Snowflake external storage integration

## Business use cases

- City and restaurant revenue analysis
- Delivery SLA and performance tracking
- Customer segmentation and order behavior analysis
- Review topic and sentiment enrichment
- Natural-language question answering over the data warehouse

## Local setup

1. Create your local environment files from the examples:
   - `ai/.env.example`
   - `airflow/.env.example`
2. Fill in your Snowflake and Gemini credentials locally.
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Run the project components:
   - dbt models under `zomato/`
   - Airflow DAG under `airflow/`
   - AI apps under `ai/`

## Security

Sensitive credentials are intentionally kept local and are ignored by Git through [.gitignore](.gitignore). Do not commit `.env`, `CREDS`, or other secrets.

## License

This project is provided for educational and portfolio use.
