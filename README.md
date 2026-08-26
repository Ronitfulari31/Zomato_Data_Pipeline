# Zomato AI Data Engineering — End-to-End Project

A complete batch data pipeline that takes Zomato-style food delivery data from raw CSVs all the way to AI-powered analytics.

From raw data to AI analytics

Zomato/Food Delivery Dataset → Amazon S3 → Snowflake → dbt → Airflow → AI (OpenAI)

The dataset lands in an S3 data lake and flows into Snowflake through a storage integration, where dbt transforms it through medallion layers — RAW (Bronze) tables loaded via COPY INTO, cleaned STAGING (Silver) views, and business-ready MARTS (Gold) with dimensions, incremental facts, and aggregate marts. Apache Airflow orchestrates the whole pipeline as one daily DAG. On top of the warehouse sits an AI lane powered by OpenAI: LLM enrichment turns free-text reviews into structured, queryable columns; RAG lets you chat with your reviews; and text-to-SQL lets you query the warehouse in plain English. Streamlit serves the dashboards and AI apps.

![Architecture](docs/architecture.png)

> 📂 Dataset + project slides: [Google Drive folder](https://drive.google.com/drive/folders/1_xcTtGbuZYmWUAdXz8YB4qjainPr6D6V?usp=sharing) — the raw CSVs are kept outside the repo because they are too large to commit to GitHub.

---

## What gets built

### Pipeline Layers

Layer

Platform / Location

Purpose / What It Contains

Source

Local raw data source

Restaurant, user, food, menu, order, order-item, and review datasets

Data Lake

Amazon S3

Stores raw source files using a separate folder structure for each table

Bronze

Snowflake RAW

Raw tables loaded from S3 using COPY INTO through Snowflake Storage Integration

Silver

Snowflake STAGING

dbt staging models for cleaning, type casting, deduplication, and standardization

Gold

Snowflake MARTS

Business-ready dimensions, incremental facts, aggregate marts, and KPI tables

AI

Snowflake AI / AI Layer

Review enrichment, RAG-based review analysis, and Text-to-SQL

Orchestration

Apache Airflow (Docker)

Schedules and controls the daily pipeline: Load → Transform → Enrich → AI Mart

Pipeline Flow

Source
   ↓
Amazon S3
   ↓
Snowflake RAW        → Bronze
   ↓
dbt STAGING          → Silver
   ↓
dbt MARTS            → Gold
   ↓
AI Layer
   ↑
Airflow orchestrates the entire workflow

2.2 Data Lake and Raw Data Preservation

Core concept
A data lake stores source data in a raw or near-raw form before analytical transformation.

Applied here
Amazon S3 is the raw data lake.

s3://<BUCKET>/raw/
├── restaurants/
├── users/
├── food/
├── menu/
├── orders/
├── order_items/
└── reviews/

Why it matters
Keeping the raw layer separate provides a source for reprocessing when downstream transformation logic changes.

2.3 ELT and Warehouse Loading

Core concept
ELT means Extract → Load → Transform. Data is loaded into the analytical platform before transformation.

Applied here

CSV
 ↓
S3
 ↓
Snowflake RAW
 ↓
dbt transformations
 ↓
STAGING / MARTS

Snowflake loading uses:

S3
 ↓
Storage Integration
 ↓
External Stage
 ↓
File Format
 ↓
COPY INTO
 ↓
RAW Tables

Why it matters
Snowflake performs the analytical storage/compute work while dbt manages SQL transformations inside the warehouse.

2.4 Medallion Architecture

Core concept
Medallion architecture separates data by increasing levels of refinement.

Bronze → Silver → Gold

Applied here

RAW       → Bronze → source-aligned data
STAGING   → Silver → cleaned / standardized data
MARTS     → Gold   → business-ready data
AI        → AI     → enriched / AI-facing workloads

Why it matters
Each layer has a clear responsibility and prevents business logic from being mixed with raw ingestion logic.

2.5 Data Quality and Cleaning

Core concept
Analytical results are only as reliable as the data used to produce them.

Applied here
dbt staging models clean and standardize source data through:

type casting

null handling

naming normalization

deduplication

column mapping

business standardization

Example:

RAW
rating = "4.5"
city = " Mumbai "

       ↓

STAGING
rating = 4.5
city = "Mumbai"

Important quality dimensions for this project include:

Dimension

Example

Completeness

Missing restaurant_id

Uniqueness

Duplicate order_id

Validity

Rating outside expected range

Consistency

Different city representations

Referential integrity

Order referencing missing entity

Type validity

Numeric value stored as text

These are the quality concepts considered by the pipeline. Automated validation should only be claimed where the corresponding dbt tests/checks are actually implemented.

2.6 Dimensional Modeling and Star Schema

Core concept
Dimensional modeling separates measurable business events from descriptive entities.

Applied here
The MARTS layer uses fact and dimension concepts for analytical reporting.

                 dim_customer
                      │
dim_restaurant ── fact_orders ── dim_date
                      │
                 dim_location

Typical analytical objects include:

Facts:
- fact_orders
- fact_order_items

Dimensions:
- dim_customer
- dim_restaurant
- dim_food
- dim_location
- dim_date

Why it matters
This makes business questions such as revenue, restaurant performance, customer behavior, and food popularity easier to query.

2.7 Fact Table Grain

Core concept
Grain defines what one row in a fact table represents.

Applied here

fact_orders
→ one row represents one order

fact_order_items
→ one row represents one item within an order

Why it matters
Clearly defining grain prevents incorrect counts and aggregations because one order may contain multiple order items.

2.8 Incremental Processing

Core concept
Incremental processing updates new or changed data instead of rebuilding the entire dataset.

Existing data
      +
New / changed data
      ↓
Incremental model
      ↓
Updated target

Applied here
The Gold architecture includes incremental fact processing.

Why it matters

less data processed per run

faster recurring workloads

lower compute requirements

better scalability

The README distinguishes the incremental concept from the full-refresh approach. Claim implementation only for the models that are actually configured as incremental in the repository.

2.9 dbt and Analytics Engineering

Core concept
dbt applies software-engineering practices to SQL-based analytical transformation.

Applied here

RAW
 ↓
STAGING
 ↓
MARTS
 ↓
AI-related models

dbt provides the transformation layer for:

modular SQL models

model dependencies

testing

documentation

version-controlled transformation logic

incremental models where configured

Where: zomato/models/, macros/, tests/, dbt_project.yml

2.10 Airflow Orchestration

Core concept
Orchestration coordinates when tasks execute and in what order.

Applied here

reload_raw
    ↓
dbt_build_core
    ↓
enrich_reviews
    ↓
dbt_build_ai

Airflow is responsible for workflow control; it is not the warehouse or transformation engine.

S3       → raw storage
Snowflake → storage + analytical compute
dbt      → transformation
Airflow  → orchestration

Where: airflow/dags/zomato_batch.py

2.11 Reliability and Recovery

Core concept
A pipeline should handle task failures without corrupting downstream processing.

Conceptually:

Task
 ↓
Failure
 ↓
Retry / Rerun
 ↓
Success
 ↓
Continue downstream

Important concepts include:

task dependencies

retries

failure isolation

reruns

backfills

idempotent processing

These concepts are relevant to the Airflow batch design.

2.12 Data Lineage

Core concept
Data lineage shows where a dataset originated and how it was transformed.

Example:

orders.csv
    ↓
S3
    ↓
Snowflake RAW
    ↓
stg_orders
    ↓
fact_orders
    ↓
Revenue / KPI Mart
    ↓
BI / AI

Why it matters
Lineage makes analytical results easier to understand, debug, and maintain.

2.13 AI as a Data Transformation Layer

Core concept
AI can be used as a transformation step that converts unstructured text into structured data.

Applied here

Review Text
    ↓
OpenAI
    ↓
Structured Attributes
    ↓
Snowflake AI Layer

Example:

"The food was amazing but delivery was extremely late."

        ↓

sentiment       = negative
sentiment_score = ...
topic           = delivery
key_issue       = late_delivery

Where: ai/enrich_reviews.py

This demonstrates an important data-engineering pattern:

Unstructured data → AI processing → structured analytical data

2.14 RAG

Core concept
Retrieval-Augmented Generation combines retrieval of relevant information with LLM generation.

Applied here

Reviews
   ↓
Embeddings
   ↓
Vector Search
   ↓
Relevant Reviews
   ↓
LLM
   ↓
Answer

Where: ai/rag_chat.py

Why it matters
The LLM can answer using relevant review information from the project rather than relying only on general model knowledge.

2.15 Text-to-SQL

Core concept
Text-to-SQL converts natural-language questions into SQL.

Applied here

Business Question
       ↓
Schema / Context
       ↓
LLM
       ↓
SQL
       ↓
Snowflake
       ↓
Result

Example:

"Which city has the highest revenue?"

becomes an analytical SQL query against the warehouse models.

Where: ai/text_to_sql.py

2.16 AI Grounding and Guardrails

Core concept
AI output should be constrained and validated before being used as data or executed against a warehouse.

Conceptually:

LLM Output
    ↓
Validation / Guardrails
    ↓
Approved Output
    ↓
Warehouse / Application

Relevant concepts include:

schema grounding

structured output

SQL validation

controlled query execution

error handling

These should be described as implemented only where the repository contains the corresponding validation logic.

2.17 Security and Access Control

Core concept
Data platforms should use controlled access and keep credentials outside source control.

Applied here

AWS IAM policies are maintained under aws/iam/

Snowflake roles are configured in warehouse setup

credentials are supplied through environment variables

real credentials are not committed to Git

Concepts demonstrated: IAM, RBAC, Least Privilege, Secret Management

2.18 End-to-End Concept Map

SOURCE CSVs
    │
    │ Batch Processing
    ▼
AMAZON S3
    │
    │ Data Lake / Raw Preservation
    ▼
SNOWFLAKE RAW
    │
    │ Bronze
    ▼
DBT STAGING
    │
    │ Cleaning / Quality / Standardization
    │ Silver
    ▼
DBT MARTS
    │
    │ Star Schema / Facts / Dimensions
    │ Grain / Incremental Processing
    │ Gold
    ▼
AI LAYER
    │
    ├── Review Enrichment
    │      Unstructured → Structured
    │
    ├── RAG
    │      Retrieval → LLM → Answer
    │
    └── Text-to-SQL
           Natural Language → SQL → Snowflake

Airflow operates as the orchestration/control plane across these stages, controlling scheduling, dependencies, and execution order.

Key takeaways

The project demonstrates a progression from raw data → trusted data → analytical models → AI-enabled analytics.

Store raw data safely before transformation.

Load and transform systematically using S3, Snowflake, and dbt.

Apply data quality and modeling principles before analytics.

Define fact grain and dimensional relationships for reliable metrics.

Use incremental processing where appropriate for scale.

Orchestrate dependencies with Airflow instead of manually running scripts.

Treat AI outputs as structured data that requires validation.

Expose trusted warehouse data through RAG and Text-to-SQL.

Technology vs Data Engineering Responsibility

Technology

Responsibility

Core Concept

CSV

Source

Batch source

Amazon S3

Raw storage

Data Lake

Snowflake

Analytical storage/compute

Cloud Data Warehouse

COPY INTO

Loading

Bulk Ingestion

dbt

Transformation

ELT / Analytics Engineering

RAW

Raw layer

Bronze

STAGING

Clean layer

Silver

MARTS

Business models

Gold

Airflow

Workflow control

Orchestration

OpenAI

Text processing

AI Transformation

Embeddings

Semantic representation

Vector Search

RAG

Context retrieval

Retrieval-Augmented Generation

Text-to-SQL

Natural-language analytics

NL → SQL

Streamlit

User interface

Analytics / AI Consumption

## 3. Medallion architecture

![Medallion Architecture](docs/Medallion_architecture.png)

The project follows a medallion architecture to keep the data pipeline clean and scalable:

- Raw layer: ingests source files and stores them in a raw landing zone.

- Staging layer: standardizes column names, cleans values, and prepares the data for modeling.

- Mart layer: creates business-ready tables used for analysis and reporting.

- AI layer: enriches review content and answers natural-language business questions.

---

## 4. Data model and schema design

The project models the relationships between the core business entities in the food delivery domain.

![Data Model](docs/Zomato_Data_Model.jpg)

This data model explains how the system works at the business level:

- a user places many orders

- an order belongs to a restaurant and a city

- an order contains multiple order items

- a menu item links to a food catalog item and restaurant pricing

- a review is associated with a user, restaurant, and order

This structure supports both operational analytics and customer-experience analysis.

## 5. Snowflake setup and connection flow

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

## 6. dbt setup and staging layer

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

## 7. Airflow orchestration

The orchestration layer is implemented with Apache Airflow, and the DAG is defined in [airflow/dags/zomato_batch.py](airflow/dags/zomato_batch.py).

### Airflow flow

![Airflow Orchestration](docs/orchestration_airflow.png)

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

## 8. AI layer and business intelligence

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

## 9. Running it

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

## 10. Example analytical questions supported by the project

- Which city has the highest revenue?

- Which restaurants have the highest ratings?

- Which cuisines are most popular?

- What are the top delivery complaints?

- Which restaurants have the worst delivery SLA?

- Which customers are most engaged by reviews and order value?

---

## 11. Why this project is valuable

This project demonstrates a complete modern data engineering workflow:

- ingestion and storage

- warehouse architecture

- transformation with dbt

- orchestration with Airflow

- AI enrichment on trusted data

- analytical and natural-language business intelligence

It also shows that the developer understands both the technical stack and the business problem behind the data.

---

## 12. Future enhancements

Potential next improvements include:

- dashboarding with Streamlit or Metabase

- data quality monitoring and alerts

- further optimization and expansion of incremental dbt processing

- CI/CD for dbt and SQL validation

- real-time or event-driven ingestion

- advanced AI summarization and sentiment trend analysis

---

**> Implementation scope: This README separates concepts demonstrated by the project from broader production practices. A concept is described as implemented only where the repository contains the corresponding configuration, model, script, or workflow. Concepts that are architectural considerations rather than completed features are explicitly presented as such.

13. Final note**

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