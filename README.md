# Zomato Data Engineering Pipeline

A full-stack data engineering project for the Zomato dataset, combining ingestion, transformation, orchestration, semantic enrichment, and a lightweight AI-assisted querying layer.

## Overview

This project builds a modern analytics stack around Zomato data using:

- Python for data processing and AI enrichment
- Airflow for orchestration
- Snowflake for analytics warehouse
- dbt for transformation and modeling
- Gemini-powered review enrichment and SQL generation
- Streamlit for conversational analytics

## Architecture

- `ai/` — AI utilities for sentiment enrichment, retrieval, and SQL generation
- `airflow/` — DAGs and orchestration configuration
- `snowflake/` — Snowflake setup, staging, and integration SQL
- `zomato/` — dbt project with models, tests, and sources
- `data/` — sample dataset used for local development and demo workflows

## Key capabilities

- Raw data ingestion orchestration via Airflow
- dbt staging and marts layer for analytics use cases
- Snowflake integration with raw + curated schemas
- LLM-based review classification and enrichment
- AI-assisted natural-language-to-SQL experience
- Business metrics for restaurant, delivery, and city performance

## Setup

1. Copy the example environment templates:
   - `ai/.env.example`
   - `airflow/.env.example`
2. Fill in your Snowflake and Gemini credentials locally.
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Run the project components as needed:
   - dbt project under `zomato/`
   - Airflow DAGs under `airflow/`
   - AI assistant apps under `ai/`

## Security

Sensitive credentials are not committed to this repository. Local environment files are ignored via `.gitignore`.

## License

This project is provided for educational and portfolio purposes.
