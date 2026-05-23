SELECT
    transaction_id,
    sender_id,
    receiver_id,
    amount,
    transaction_type,
    risk_score
FROM silver_transactions
WHERE risk_score >= 7
ORDER BY risk_score DESC, amount DESC;