# Zomato AI Data Engineering — End-to-End Project

A complete batch data pipeline that takes Zomato-style food delivery data from raw CSVs all the way to AI-powered analytics.

Zomato/Food Delivery Dataset → Amazon S3 → Snowflake → dbt → Airflow → AI (OpenAI)

The dataset lands in an S3 data lake and flows into Snowflake through a storage integration, where dbt transforms it through medallion layers — RAW (Bronze) tables loaded via COPY INTO, cleaned STAGING (Silver) views, and business-ready MARTS (Gold) with dimensions, incremental facts, and aggregate marts. Apache Airflow orchestrates the whole pipeline as one daily DAG. On top of the warehouse sits an AI lane powered by OpenAI: LLM enrichment turns free-text reviews into structured, queryable columns; RAG lets you chat with your reviews; and text-to-SQL lets you query the warehouse in plain English. Streamlit serves the dashboards and AI apps.

![Architecture](docs/architecture.png)

> 📂 Dataset + project slides: [Google Drive folder](https://drive.google.com/drive/folders/1_xcTtGbuZYmWUAdXz8YB4qjainPr6D6V?usp=sharing) — the raw CSVs are kept outside the repo because they are too large to commit to GitHub.

---

## What gets built

| Layer | Where | What |
| --- | --- | --- |
| Source | local/raw data source | restaurant, user, food, menu, order, order_item, and review data |
| Lake | Amazon S3 | one bucket with raw folder structure per table |
| Bronze | Snowflake RAW | COPY INTO from S3 through storage integration |
| Silver | Snowflake STAGING | dbt staging views for cleaning, typing, and standardization |
| Gold | Snowflake MARTS | dimensions, incremental facts, aggregate marts, KPI tables |
| AI | Snowflake AI | review enrichment, RAG, text-to-SQL |
| Orchestration | Airflow (Docker) | daily DAG for load → transform → enrich → AI mart |

---

## Tech stack

Python · Pandas · Amazon S3 · Snowflake · dbt (dbt-snowflake) · Apache Airflow 3 (Docker) · OpenAI (gpt-4o-mini, text-embedding-3-small) · Streamlit

---

## Repository structure

```text
├── ai/
│   ├── enrich_reviews.py
│   ├── example.env
│   ├── rag_chat.py
│   └── text_to_sql.py
├── airflow/
│   ├── Dockerfile
│   ├── docker-compose.yaml
│   ├── example.env
│   └── dags/
│       └── zomato_batch.py
├── aws/
│   └── iam/
│       ├── s3-read-policy.json
│       ├── snowflake-role-trust-policy-final.json
│       └── snowflake-role-trust-policy-initial.json
├── docs/
│   ├── architecture.png
│   └── Zomato_Data_Model.jpg
├── snowflake/
│   ├── 01_setup.sql
│   ├── 02_storage_integration.sql
│   ├── 03_stage_and_formats.sql
│   ├── 04_raw_tables.sql
│   └── 05_copy_into.sql
├── zomato/
│   ├── README.md
│   ├── analyses/
│   ├── dbt_project.yml
│   ├── macros/
│   ├── models/
│   ├── snapshots/
│   └── tests/
├── .gitignore
├── README.md
├── LICENSE
└── .venv/
```

> The raw data files are intentionally not committed to GitHub because they are too large. Download them from the Drive link above and place them in your local raw-data location before running the pipeline.

---

## 1. End-to-end pipeline flow

The project follows a clear production-style data pipeline from raw files to trusted analytics and AI-driven business insight.

![High-Level Pipeline](docs/high_level_pipeline.png)

### Flow in one sequence

1. Source datasets are collected as CSVs for restaurants, users, food, menu, orders, order items, and reviews.
2. The files are landed in Amazon S3 as raw data in a lake folder structure such as `s3://<BUCKET>/raw/<table>/`.
3. Snowflake reads the raw files through a storage integration and loads them into the RAW schema using COPY INTO.
4. dbt transforms the data in the staging and mart layers: cleaning, typing, deduplicating, standardizing, and building fact and dimension models.
5. Apache Airflow orchestrates the end-to-end process in one DAG so the load, dbt transformation, and AI enrichment run in a controlled sequence.
6. The AI layer enriches review text, enables RAG on reviews, and supports natural-language querying with text-to-SQL across the warehouse.

This pipeline turns raw operational records into trusted, business-ready analytics and actionable insights.

### Business questions this pipeline answers

- Which cities generate the most revenue?
- Which restaurants perform best by rating and delivery time?
- What are the top delivery issues reported by customers?
- Which food items or cuisines are most popular?
- How can AI help interpret sentiment and reveal insight from reviews?

---

## 2. Medallion architecture

![Medallion Architecture](docs/Medallion_architecture.png)

```mermaid
flowchart LR
    RAW[Raw Layer] --> STG[Staging Layer]
    STG --> MART[Marts / Facts / Dimensions]
    MART --> AI[AI / Enrichment / RAG / Text-to-SQL]
    AI --> BI[Business Intelligence]
```

The project follows a medallion architecture to keep the data pipeline clean and scalable:

- Raw layer: ingests source files and stores them in a raw landing zone.
- Staging layer: standardizes column names, cleans values, and prepares the data for modeling.
- Mart layer: creates business-ready tables used for analysis and reporting.
- AI layer: enriches review content and answers natural-language business questions.

---

## 3. Data model and schema design

The project models the relationships between the core business entities in the food delivery domain.

![Data Model](docs/Zomato_Data_Model.jpg)

This data model explains how the system works at the business level:

- a user places many orders
- an order belongs to a restaurant and a city
- an order contains multiple order items
- a menu item links to a food catalog item and restaurant pricing
- a review is associated with a user, restaurant, and order

This structure supports both operational analytics and customer-experience analysis.

```mermaid
erDiagram
    USERS ||--o{ ORDERS : places
    RESTAURANTS ||--o{ ORDERS : serves
    ORDERS ||--o{ ORDER_ITEMS : contains
    FOOD ||--o{ ORDER_ITEMS : includes
    RESTAURANTS ||--o{ MENU : has
    FOOD ||--o{ MENU : mapped_in
    USERS ||--o{ REVIEWS : writes
    RESTAURANTS ||--o{ REVIEWS : receives
    ORDERS ||--o{ REVIEWS : has
```

---

## 4. Snowflake setup and connection flow

The warehouse layer is implemented in Snowflake. The setup is done in sequence using the scripts in the [snowflake](snowflake) folder.

### Setup scripts order

1. [snowflake/01_setup.sql](snowflake/01_setup.sql) - create warehouse, database, schemas, and role
2. [snowflake/02_storage_integration.sql](snowflake/02_storage_integration.sql) - create storage integration for S3 access
3. [snowflake/03_stage_and_formats.sql](snowflake/03_stage_and_formats.sql) - create stage and file formats
4. [snowflake/04_raw_tables.sql](snowflake/04_raw_tables.sql) - create raw tables
5. [snowflake/05_copy_into.sql](snowflake/05_copy_into.sql) - copy data from S3 into Snowflake

### Example environment variables

Create your environment file from [ai/example.env](ai/example.env) and [airflow/example.env](airflow/example.env) with values like:

```bash
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_USER=your_user_name
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=ZOMATO_WH
SNOWFLAKE_DATABASE=ZOMATO
SNOWFLAKE_SCHEMA=STAGING
OPENAI_API_KEY=your_openai_key
SAMPLE_N=5
```

> Do not commit real credentials to GitHub.

---

## 5. dbt setup and staging layer

The analytics layer is built with dbt. The setup process is important because it creates the base project that connects to Snowflake and builds the transformation pipeline.

### Install dbt for Snowflake

```bash
pip install dbt-snowflake
```

### Initialize a dbt project

```bash
dbt init project_name
```

When prompted, select the required option:

- choose database type: Snowflake
- select option 1 for Snowflake

Example answers:

```text
Which database would you like to use?
1. snowflake

Enter your account identifier:
<from Snowflake -> Profile -> Connect -> Account Identifier>

Enter your user name:
<from Snowflake Profile -> Connect tool -> dev username>

Enter your password:
<your Snowflake password or SSO/keypair-based auth>

Enter your role:
DBT_ROLE

Enter your warehouse:
ZOMATO_WH

Enter your database:
ZOMATO

Enter your schema:
STAGING

Enter number of threads:
8
```

### Validate connection

```bash
cd project_name
dbt debug
```

### Staging layer setup

The staging layer is where raw tables are cleaned and normalized. In this project, [zomato/models/staging](zomato/models/staging) contains the staging models such as:

- stg_orders.sql
- stg_order_items.sql
- stg_restaurants.sql
- stg_reviews.sql
- stg_users.sql
- stg_food.sql
- stg_menu.sql

Typical tasks include:

- naming normalization
- type casting
- null handling
- deduplication
- column mapping
- business standardization

### Build the project

```bash
dbt build --exclude tag:ai
```

---

## 6. Airflow orchestration

The orchestration layer is implemented with Apache Airflow, and the DAG is defined in [airflow/dags/zomato_batch.py](airflow/dags/zomato_batch.py).

![Airflow Orchestration](docs/orchestration_airflow.png)

### Airflow flow

```mermaid
graph TD
    A[reload_raw] --> B[dbt_build_core]
    B --> C[enrich_reviews]
    C --> D[dbt_build_ai]
```

### Start Airflow locally

```bash
cd airflow
cp example.env .env
docker compose build
docker compose up -d
```

Then open:

```text
http://localhost:8080
```

Unpause the DAG and trigger it when ready.

---

## 7. AI layer and business intelligence

The AI layer sits on top of the warehouse and adds intelligence to the analytical data.


### 1. Review enrichment

The script [ai/enrich_reviews.py](ai/enrich_reviews.py) sends review text to OpenAI and produces fields like:

- sentiment label
- sentiment score
- topic
- key issue

### 2. RAG over reviews

The script [ai/rag_chat.py](ai/rag_chat.py) enables natural-language questions over review data.

### 3. Text-to-SQL over warehouse

The script [ai/text_to_sql.py](ai/text_to_sql.py) translates plain-English questions into safe SQL and queries the Snowflake marts layer.

---

## 8. Running it

```bash
# 1. Create or update Snowflake objects
# run SQL files in order from snowflake/

# 2. Install dbt package
pip install dbt-snowflake

# 3. Initialize dbt project
dbt init project_name

# 4. Validate dbt connection
cd project_name
dbt debug

# 5. Build models
dbt build --exclude tag:ai

# 6. Start Airflow
cd airflow
cp example.env .env
docker compose up -d

# 7. Run AI enrichment
export OPENAI_API_KEY=your_key
python ai/enrich_reviews.py

# 8. Run AI apps
streamlit run ai/rag_chat.py
streamlit run ai/text_to_sql.py
```

---

## 9. Example analytical questions supported by the project

- Which city has the highest revenue?
- Which restaurants have the highest ratings?
- Which cuisines are most popular?
- What are the top delivery complaints?
- Which restaurants have the worst delivery SLA?
- Which customers are most engaged by reviews and order value?

---

## 10. Why this project is valuable

This project demonstrates a complete modern data engineering workflow:

- ingestion and storage
- warehouse architecture
- transformation with dbt
- orchestration with Airflow
- AI enrichment on trusted data
- analytical and natural-language business intelligence

It also shows that the developer understands both the technical stack and the business problem behind the data.

---

## 11. Future enhancements

Potential next improvements include:

- dashboarding with Streamlit or Metabase
- data quality monitoring and alerts
- incremental dbt logic for large datasets
- CI/CD for dbt and SQL validation
- real-time or event-driven ingestion
- advanced AI summarization and sentiment trend analysis

---

## 12. Final note

This repository is best understood as a modern data platform end-to-end project where the goal is not only to move data but to create a reliable, understandable, and AI-enabled business intelligence system.

The complete value comes from combining:

- warehouse design
- dbt transformation
- orchestration automation
- AI enrichment
- business insight generation

That combination is what makes this project strong for learning, demos, and portfolio presentation.

---

## Acknowledgement

This project is built to showcase real-world data engineering practices: ingestion, transformations, orchestration, and AI augmentation on top of analytical data.

