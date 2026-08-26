# Zomato AI Data Engineering End-to-End Project

A full-stack, cloud-native data engineering project that ingests Zomato-style food delivery data from raw CSVs, loads it into Snowflake, transforms it with dbt, orchestrates the pipeline with Airflow, and adds AI-powered analytics on top of the warehouse.

The project demonstrates a realistic medallion architecture:

Zomato dataset → Amazon S3 → Snowflake Raw → dbt staging + marts → Airflow orchestration → AI review enrichment + RAG + text-to-SQL

![Architecture](docs/architecture.png)

## Why this project

This repository is designed to show how a production-style data pipeline can be built end to end with:

- raw batch data ingestion
- cloud data lake integration with S3 and Snowflake
- transformation logic using dbt
- incremental warehouse modeling
- orchestrated batch pipelines with Airflow
- LLM-based enrichment and analytics on top of the warehouse

It is useful for learning modern data engineering patterns and for building a strong portfolio project.

---

## Tech stack

- Python
- Pandas
- Amazon S3
- Snowflake
- dbt (dbt-snowflake)
- Apache Airflow 3
- OpenAI API
- Streamlit

---

## Project overview

The pipeline ingests seven CSV datasets representing restaurants, users, food items, menu items, orders, order items, and customer reviews. The data is staged in S3, copied into Snowflake through a storage integration, transformed with dbt into staging and mart layers, and finally exposed to AI workflows.

### Data flow

1. Source CSVs are loaded into S3 under a raw folder structure.
2. Snowflake uses a storage integration and IAM trust policy to access the bucket.
3. Raw data is copied into Snowflake using COPY INTO commands.
4. dbt builds staging views and business-ready mart tables.
5. Airflow orchestrates the batch flow daily.
6. AI scripts enrich reviews and power question answering over reviews and warehouse data.

---

## Architecture

### High-level pipeline

```text
Source CSVs
   ↓
Amazon S3 (raw/<table>/)
   ↓
Snowflake RAW schema
   ↓
dbt staging models
   ↓
dbt gold marts + incremental facts
   ↓
Airflow DAG: reload_raw → dbt_build_core → enrich_reviews → dbt_build_ai
   ↓
AI analytics layer
   - review enrichment
   - RAG on reviews
   - text-to-SQL on warehouse
```

### Medallion design

- Raw: copied directly from S3 into Snowflake tables
- Staging: cleaned, typed, and conformed source data
- Marts: business-ready transformed datasets and KPIs
- AI: enrichment and semantic query layers built on top of the warehouse

---

## Repository structure

```text
.
├── ai/
│   ├── enrich_reviews.py
│   ├── example.env
│   ├── rag_chat.py
│   └── text_to_sql.py
├── airflow/
│   ├── dags/
│   │   └── zomato_batch.py
│   ├── Dockerfile
│   ├── docker-compose.yaml
│   └── example.env
├── aws/
│   └── iam/
│       ├── s3-read-policy.json
│       ├── snowflake-role-trust-policy-final.json
│       └── snowflake-role-trust-policy-initial.json
├── docs/
│   └── architecture.png
├── snowflake/
│   ├── 01_setup.sql
│   ├── 02_storage_integration.sql
│   ├── 03_stage_and_formats.sql
│   ├── 04_raw_tables.sql
│   └── 05_copy_into.sql
├── zomato/
│   ├── macros/
│   │   └── generate_schema_name.sql
│   ├── models/
│   │   ├── marts/
│   │   └── staging/
│   ├── README.md
│   ├── analyses/
│   ├── dbt_project.yml
│   ├── snapshots/
│   └── tests/
├── .gitignore
├── README.md
└── LICENSE
```

> Note: the dataset, generated files, and dbt build artifacts are intentionally not committed in this repo.

---

## Data model

The project works with seven core source datasets:

- restaurants
- users
- food
- menu
- orders
- order_items
- reviews

The order and review datasets are large and support analytical questions such as:

- revenue and order trends by city
- restaurant performance
- delivery SLA analysis
- customer sentiment and review insights
- payment and fulfilment behavior

---

## dbt layer

The dbt project under [zomato](zomato) includes staging and mart models that perform the transformation and business logic.

### Main concepts

- staging: clean and standardize raw source data
- dimensions: customers, restaurants, food, date dimension
- facts: order and order-item fact tables
- marts: aggregate and KPI-focused outputs like city revenue and SLA metrics
- tests: schema, uniqueness, null, relationships, and accepted values checks

The warehouse is organized around Snowflake schemas such as:

- RAW
- STAGING
- MARTS
- SNAPSHOTS
- AI

---

## AI layer

This project adds an AI layer on top of the warehouse with three capabilities:

### 1. Review enrichment

The script [ai/enrich_reviews.py](ai/enrich_reviews.py) reads raw reviews, sends them to OpenAI using `gpt-4o-mini`, and writes structured outputs such as:

- sentiment label
- sentiment score
- topic
- key issue

These enriched reviews are loaded into `ZOMATO.AI.REVIEW_ENRICHED` and used downstream in analytics.

### 2. RAG over reviews

The script [ai/rag_chat.py](ai/rag_chat.py) embeds review text and allows users to ask natural-language questions like:

- What are customers complaining about most?
- Which cities have the biggest delivery issues?
- Are people happy with food quality or pricing?

### 3. Text-to-SQL over the warehouse

The script [ai/text_to_sql.py](ai/text_to_sql.py) asks the LLM to translate plain-English questions into safe Snowflake SELECT queries and runs those queries against the marts layer.

---

## Airflow orchestration

The DAG in [airflow/dags/zomato_batch.py](airflow/dags/zomato_batch.py) orchestrates the batch flow:

```text
reload_raw → dbt_build_core → enrich_reviews → dbt_build_ai
```

This means the system executes the following steps in order:

1. load raw data from S3 into Snowflake
2. run dbt core model builds and tests
3. run LLM enrichment on reviews
4. build the AI-related dbt models

The Airflow setup uses Docker and injects Snowflake and OpenAI values through environment variables.

---

## Prerequisites

Before running this project, make sure you have:

- AWS account with S3 access
- Snowflake account and admin access to create warehouse/database/schema/role
- OpenAI API key
- Docker and Docker Compose installed
- Python 3.10+
- dbt installed or accessible through the Airflow image

---

## Snowflake setup

Use the scripts in the [snowflake](snowflake) folder in order:

1. [snowflake/01_setup.sql](snowflake/01_setup.sql) – create warehouse, database, schemas, and role
2. [snowflake/02_storage_integration.sql](snowflake/02_storage_integration.sql) – create storage integration for S3 access
3. [snowflake/03_stage_and_formats.sql](snowflake/03_stage_and_formats.sql) – create external stage and file format
4. [snowflake/04_raw_tables.sql](snowflake/04_raw_tables.sql) – create raw tables
5. [snowflake/05_copy_into.sql](snowflake/05_copy_into.sql) – copy data from S3 stage into Snowflake

For AWS, use the IAM policy and trust policy JSON files in [aws/iam](aws/iam) to connect Snowflake with S3 using a keyless integration.

---

## Environment variables

Create environment files based on the examples in:

- [airflow/example.env](airflow/example.env)
- [ai/example.env](ai/example.env)

Typical variables include:

```bash
SNOWFLAKE_ACCOUNT=
SNOWFLAKE_USER=
SNOWFLAKE_PASSWORD=
SNOWFLAKE_WAREHOUSE=
SNOWFLAKE_DATABASE=
SNOWFLAKE_SCHEMA=
OPENAI_API_KEY=
SAMPLE_N=5
```

---

## Run locally

### 1. Configure Snowflake and AWS

Run the SQL setup scripts in Snowsight, and configure the S3 storage integration with the IAM trust policy.

### 2. Run dbt

```bash
cd zomato
export SNOWFLAKE_ACCOUNT=...
export SNOWFLAKE_USER=...
export SNOWFLAKE_PASSWORD=...

dbt debug

dbt build --exclude tag:ai
```

### 3. Start Airflow

```bash
cd airflow
cp example.env .env

docker compose build
docker compose up -d
```

Then open:

- http://localhost:8080

Log in with the Airflow user created in Docker setup, unpause the DAG, and trigger it.

### 4. Run AI apps

```bash
export OPENAI_API_KEY=sk-...

python ai/enrich_reviews.py
streamlit run ai/rag_chat.py
streamlit run ai/text_to_sql.py
```

---

## Example questions supported by the AI layer

- Which cities have the highest GMV?
- What are the most common delivery complaints?
- Which restaurants have the highest cancellation rate?
- What is the average delivery time by city?
- Show restaurants with the best customer ratings

---

## Notes and best practices

- Never store credentials in code or Git.
- Update the IAM trust policy after retrieving Snowflake external IDs via `DESC INTEGRATION`.
- Keep the raw S3 folder structure aligned with the copy commands.
- Use dbt tests to validate model quality before promoting data.
- Be careful with AI usage costs: scripts support sample-limited review enrichment.

---

## Future enhancements

Potential next steps for this project include:

- adding a dashboard layer with Streamlit or Metabase
- moving the AI logic into a serverless or API-based service
- adding data quality monitoring and alerting
- automating deployment via CI/CD
- scaling the warehouse and ingestion pipeline for larger datasets

---

## License

This project is intended for learning and portfolio use. Please check the repository license before using it in production or sharing commercially.

---

## Acknowledgement

This project is built to showcase real-world data engineering practices: ingestion, transformations, orchestration, and AI augmentation on top of analytical data.

