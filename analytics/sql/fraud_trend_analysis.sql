SELECT
    ingestion_date,
    total_transactions,
    total_frauds,
    ROUND(fraud_ratio_percent, 2) AS fraud_percentage,
    fraud_amount
FROM gold_fraud_daily_summary
ORDER BY ingestion_date;