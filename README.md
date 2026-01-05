# Streamlit + Snowflake Mini Dashboard

A small Streamlit app that connects to Snowflake, runs a few SQL queries, and displays results as a simple dashboard.

## What it does
- Connects to Snowflake using the Snowflake Python connector
- Executes SQL queries directly in Snowflake
- Loads results into Pandas DataFrames
- Shows metrics, tables, and charts in Streamlit

## Tech stack
- Python
- Streamlit
- Snowflake (snowflake-connector-python)
- Pandas
- python-dotenv

## Setup

1) Create a `.env` file in the project root:

```bash
SNOWFLAKE_ACCOUNT=...
SNOWFLAKE_USER=...
SNOWFLAKE_PASSWORD=...
SNOWFLAKE_WAREHOUSE=...
SNOWFLAKE_DATABASE=...
SNOWFLAKE_SCHEMA=...
SNOWFLAKE_ROLE=...
```

2) Install dependencies:
```bash
pip install -r requirements.txt
``` 

3) Run the app:

```bash
streamlit run app.py
```
## Docker (optional)

Build the image:
```bash
docker build -t streamlit-snowflake .
```

run using your local `.env`:
```bash
docker run --env-file .env -p 8501:8501 streamlit-snowflake
```

## Notes
Credentials are provided via environment variables and are not committed to the repository.
