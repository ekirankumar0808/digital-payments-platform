SELECT
    sender_id,
    COUNT(*) AS txn_count,
    ROUND(SUM(amount), 2) AS total_amount_sent
FROM silver_transactions
GROUP BY sender_id
ORDER BY total_amount_sent DESC
LIMIT 20;