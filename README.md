# Tiki E-commerce ELT Pipeline Project

_________
## Overview
An automated data pipeline that tracks Tiki.vn product prices and seller metrics across multiple categories.

## Architecture & Tech stack
```mermaid
flowchart TD
    subgraph Row1[Ingest]
        direction LR
        A[Tiki API] --> B[extract.py] --> C[load.py]
    end
    subgraph Row2[Storage and Transform]
        direction LR
        D[("S3 Bronze")] --> E[Athena] --> F[("dbt staging")]
    end
    subgraph Row3[Model and Serve]
        direction LR
        G[dbt snapshots] --> H[("dbt marts")] --> I[QuickSight]
    end

    Row1 --> Row2
    Row2 --> Row3

    Airflow["Airflow DAG<br/>orchestrate toan bo, daily - in progress"]:::airflow -.-> Row1
    Airflow -.-> Row2

    classDef airflow fill:#fff,stroke:#999,stroke-dasharray: 5 5,color:#000
    style Row1 fill:none,stroke:#ccc
    style Row2 fill:none,stroke:#ccc
    style Row3 fill:none,stroke:#ccc
```

**Ingestion:**    Python (requests, tenacity)

**Storage:**      AWS S3 (Bronze - raw JSON)

**Warehouse:**     AWS Athena hoặc Redshift Serverless (Silver + Gold)

**Transform:**     dbt (dbt-athena hoặc dbt-redshift)

**Orchestration:** Airflow (Docker local)

**BI:**            Apache Superset (Docker local) hoặc AWS QuickSight

**CI/CD:**         GitHub Actions


## Structure
```
tiki-pipeline/
├── ingestion/
│   ├── extract.py          # fetch data từ Tiki API
│   ├── load.py             # upload lên S3
│   └── pipeline.py         # kết hợp extract + load, Airflow import
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/        # Silver layer
│   │   │   ├── stg_products.sql
│   │   │   ├── stg_sellers.sql
│   │   │   └── stg_categories.sql
│   │   └── marts/          # Gold layer - star schema
│   │       ├── dim_product.sql
│   │       ├── dim_seller.sql
│   │       ├── dim_category.sql
│   │       ├── dim_date.sql
│   │       └── fact_product_daily_snapshot.sql
│   ├── snapshots/          # SCD Type 2
│   │   └── dim_product_snapshot.sql
│   └── tests/
│       └── generic/
│
├── airflow/
│   ├── dags/
│   │   └── tiki_pipeline_dag.py
│   └── plugins/
│
├── .github/
│   └── workflows/
│       ├── ci.yml
│       └── cd.yml
│
├── .env.example
├── .gitignore
├── docker-compose.yml      # Airflow + Superset
├── requirements.txt
└── README.md
```
