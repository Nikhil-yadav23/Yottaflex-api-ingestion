-- 1. Run-level reconciliation
SELECT run_id, status, input_count, output_count, rejected_count, pages_fetched,
       duration_seconds, error_message
FROM pipeline_runs ORDER BY started_at DESC;

-- 2. Rejection reasons and counts
SELECT entity, reason, COUNT(*) AS rejected_rows
FROM quarantine GROUP BY entity, reason ORDER BY rejected_rows DESC;

-- 3. Referential integrity: expected result is zero
SELECT COUNT(*) AS orphan_orders
FROM orders o LEFT JOIN customers c ON c.customer_id = o.customer_id
WHERE c.customer_id IS NULL;

-- 4. Duplicate business keys: expected result is no rows
SELECT order_id, COUNT(*) AS row_count FROM orders GROUP BY order_id HAVING COUNT(*) > 1;

-- 5. Event history uniqueness: expected result is no rows
SELECT order_id, event_version, COUNT(*) AS row_count
FROM order_events GROUP BY order_id, event_version HAVING COUNT(*) > 1;

-- 6. Current order totals by status
SELECT status, COUNT(*) AS order_count, ROUND(SUM(amount), 2) AS total_amount
FROM orders GROUP BY status ORDER BY status;
