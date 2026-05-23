# Digital Payments Analytics Table Reference

| Layer  | Logical Table Name          | S3 Path |
|--------|-----------------------------|---------|
| Bronze | bronze_transactions         | s3://digital-payments-kiran/bronze/paysim/transactions/ |
| Silver | silver_transactions         | s3://digital-payments-kiran/silver/paysim/transactions/ |
| Gold   | gold_fraud_daily_summary   | s3://digital-payments-kiran/gold/paysim/fraud_daily_summary/ |

---

## Purpose

### bronze_transactions
Raw ingested transactional data with ingestion metadata.

### silver_transactions
Validated, transformed, deduplicated transaction data with fraud risk engineering.

### gold_fraud_daily_summary
Business-level aggregated fraud analytics and KPI reporting table.