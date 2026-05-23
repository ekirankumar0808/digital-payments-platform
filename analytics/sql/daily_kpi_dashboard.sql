SELECT
    ingestion_date,
    total_transactions,
    total_transaction_value,
    fraud_amount,
    fraud_ratio_percent,
    avg_risk_score,
    high_value_txns
FROM gold_fraud_daily_summary
ORDER BY ingestion_date;