# Digital Payments Fraud Detection Data Engineering Platform

## Project Overview

This project is a production-grade AWS Data Engineering platform built using Medallion Architecture (Bronze, Silver, Gold) for processing and analyzing digital payment fraud transactions.

The pipeline ingests raw transaction data into AWS S3, processes it using PySpark and Delta Lake, orchestrates workflows using Apache Airflow, performs data quality validation, and exposes analytics through Athena and Streamlit dashboards.

---

# Architecture

Raw CSV → S3 Raw Layer → Spark Bronze → Spark Silver → Spark Gold → Athena → Streamlit Dashboard

---

# Tech Stack

- AWS S3
- AWS Athena
- Apache Spark
- PySpark
- Delta Lake
- Apache Airflow
- Docker
- Streamlit
- Pandas
- Plotly
- Python

---

# Key Features

- Incremental ETL Processing
- Delta Lake MERGE Operations
- Medallion Architecture
- Data Quality Validation
- Quarantine Layer
- Audit Logging
- Athena Analytics
- Interactive Dashboard
- Dockerized Infrastructure
- Airflow Orchestration

---

# Project Structure

```bash
digital-payments-platform/
│
├── airflow/
├── spark/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── utils/
│
├── dashboard/
├── sql/
├── configs/
├── screenshots/
├── docker/
├── tests/
│
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

# Pipeline Layers

## Bronze Layer

- Raw ingestion from S3
- Schema enforcement
- Metadata generation
- Audit logging

## Silver Layer

- Data cleansing
- Validation framework
- Fraud enrichment
- Feature engineering
- Quarantine handling

## Gold Layer

- Aggregated fraud analytics
- KPI metrics
- Business reporting

---

# Data Quality Framework

The pipeline validates:

- Invalid transaction amounts
- Null transaction IDs
- Invalid balances
- Invalid transaction types

Bad records are stored separately in quarantine tables.

---

# Athena Analytics

Athena is used for:

- Fraud trend analysis
- KPI reporting
- Business intelligence queries

---

# Streamlit Dashboard

The dashboard provides:

- Fraud KPIs
- Transaction trends
- Fraud amount analytics
- Risk score monitoring
- Interactive visualizations

---

# Airflow Orchestration

Airflow DAG automates:

- Bronze ingestion
- Silver transformation
- Gold aggregation

---

# Delta Lake Features

- ACID Transactions
- MERGE Operations
- Incremental Processing
- Schema Evolution

---

# How to Run

## Start Docker Services

```bash
docker compose up -d
```

## Run Airflow DAG

```bash
airflow dags trigger digital_payments_pipeline
```

## Start Streamlit Dashboard

```bash
streamlit run app.py --server.port 8501 --server.address 0.0.0.0
```

---

# Future Improvements

- Real-time Kafka Streaming
- ML Fraud Detection Models
- EMR Deployment
- Terraform IaC
- CI/CD Automation
- CloudWatch Monitoring

---

# Dashboard Preview




---

# Author

Kiran
