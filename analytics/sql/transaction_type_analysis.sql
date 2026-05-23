SELECT
    transaction_type,
    COUNT(*) AS total_transactions,
    ROUND(SUM(amount), 2) AS total_amount,
    ROUND(AVG(amount), 2) AS avg_amount
FROM silver_transactions
GROUP BY transaction_type
ORDER BY total_amount DESC;