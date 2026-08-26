# Zomato AI Data Engineering End-to-End Project

A full-stack, cloud-native data engineering project that ingests Zomato-style food delivery data from raw CSVs, loads it into Snowflake, transforms it with dbt, orchestrates the pipeline with Airflow, and adds AI-powered analytics on top of the warehouse.

The project demonstrates a realistic medallion architecture:

Zomato dataset → Amazon S3 → Snowflake Raw → dbt staging + marts → Airflow orchestration → AI review enrichment + RAG + text-to-SQL

![Architecture](docs/architecture.png)

This architecture shows how a business dataset evolves from raw operational records into trusted analytics and AI-ready insights. Raw CSVs are ingested into cloud storage, loaded into Snowflake, transformed using dbt, automated with Airflow, and then exposed to intelligent business queries.

## Project at a glance

This project is designed to show end-to-end ownership of a data platform, not just isolated scripts or notebooks. It covers the full lifecycle of a real business dataset:

- ingest raw operational data from CSV sources
- land it in cloud storage and warehouse layers
- apply transformation logic with dbt for trusted analytics
- automate workflows with Airflow for repeatable execution
- turn business data into action using AI-powered review analysis and natural-language querying

### Why this matters in a real business context

- helps restaurant chains and food delivery platforms understand performance by city, restaurant, and cuisine
- identifies customer pain points from reviews and delivery experience data
- supports faster decision-making using curated marts and KPI-focused models
- demonstrates how modern AI can sit on top of a reliable warehouse instead of replacing it

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

The repository includes a Zomato-style dataset in [data_set](data_set) with seven source CSV files: restaurants, users, food, menu, orders, order items, and customer reviews. The full dataset is also available in the Google Drive folder linked below, which is useful if you want to download the complete raw files locally for ingestion and testing.

### Dataset download

- Google Drive dataset link: https://drive.google.com/drive/folders/1_xcTtGbuZYmWUAdXz8YB4qjainPr6D6V?usp=sharing

### Included source files

- [data_set/restaurant.csv](data_set/restaurant.csv)
- [data_set/users.csv](data_set/users.csv)
- [data_set/food.csv](data_set/food.csv)
- [data_set/menu.csv](data_set/menu.csv)
- [data_set/orders.csv](data_set/orders.csv)
- [data_set/order_items.csv](data_set/order_items.csv)
- [data_set/reviews.csv](data_set/reviews.csv)

### Data dictionary

| Table | Purpose | Key fields |
| --- | --- | --- |
| restaurant | restaurant master data | restaurant id, city, cuisine, rating, address |
| users | customer profile data | user id, name, age, occupation, income |
| food | menu item catalog | food id, item name, veg/non-veg |
| menu | item-to-restaurant mapping and pricing | menu id, restaurant id, food id, price |
| orders | transactional order header data | order id, user id, city, subtotal, payment method, status |
| order_items | order line item details | order item id, order id, food id, price, quantity |
| reviews | customer feedback data | review id, order id, rating, comment, review date |

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
├── data_set/
│   ├── food.csv
│   ├── menu.csv
│   ├── order_items.csv
│   ├── orders.csv
│   ├── restaurant.csv
│   ├── reviews.csv
│   └── users.csv
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

> The source CSV dataset is included in the repository under [data_set](data_set), and the full raw dataset can also be downloaded from the Google Drive link above for local ingestion and testing.

---

## Data model

The project works with seven core source datasets included in [data_set](data_set):

- restaurants (`restaurant.csv`)
- users (`users.csv`)
- food (`food.csv`)
- menu (`menu.csv`)
- orders (`orders.csv`)
- order_items (`order_items.csv`)
- reviews (`reviews.csv`)

These files include restaurant metadata, user profiles, menu and food item attributes, order transactions, line-item details, and customer feedback, which support analytical questions such as:

- revenue and order trends by city
- restaurant performance
- delivery SLA analysis
- customer sentiment and review insights
- payment and fulfilment behavior

### Data model view

![Data Model](docs/Zomato_Data_Model.jpg)

This data model captures the operational reality of a food delivery business: customers place orders, restaurants fulfil those orders, menu items define the catalog, and reviews provide customer feedback at the transaction level. Structuring the data this way enables both operational analytics and AI-driven insight generation.

This data model represents the core business relationships in the platform:

- a user places many orders
- each order belongs to a restaurant and a city
- each order contains multiple order items
- each menu item is mapped to a restaurant and food catalog entry
- each review is linked to a user, restaurant, and order

This structure enables both transactional analysis and customer-experience analysis across the full delivery lifecycle.

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

## Presentation/demo storyline

A strong way to present this project in an interview or portfolio review is to frame it as a business problem with a technical solution:

1. The business has raw transaction and review data spread across multiple datasets.
2. We ingest and standardize that data into a warehouse using cloud storage and Snowflake.
3. We build clean, reusable data models with dbt so analysts and AI systems can trust the metrics.
4. We automate the workflow with Airflow so data refreshes are repeatable and production-like.
5. We add AI to convert customer sentiment into operational insight and natural-language analytics.

### Example business questions supported by the AI layer

- Which cities have the highest GMV?
- What are the most common delivery complaints?
- Which restaurants have the highest cancellation rate?
- What is the average delivery time by city?
- Show restaurants with the best customer ratings
- Which cuisines or restaurants have strong sentiment but weak delivery performance?

### Business value summary

This project proves that you can work across the full data stack and think beyond coding:

- data engineering: ingestion, storage, pipeline orchestration
- analytics engineering: transformation, modeling, testing, KPI design
- AI engineering: review enrichment, semantic search, text-to-SQL
- business understanding: operational and customer experience analysis

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

