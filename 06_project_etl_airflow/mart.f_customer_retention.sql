INSERT INTO mart.f_customer_retention (
    new_customers_count,
    returning_customers_count,
    refunded_customer_count,
    period_name,
    period_id,
    item_id,
    new_customers_revenue,
    returning_customers_revenue,
    customers_refunded
)
WITH weekly_stats AS (
    SELECT
        DATE_TRUNC('week', fs.date_id) AS week_start,
        fs.item_id,
        fs.customer_id,
        COUNT(DISTINCT fs.date_id) AS orders_count,
        SUM(CASE WHEN fs.status = 'refunded' THEN 1 ELSE 0 END) AS refund_count,
        SUM(CASE WHEN fs.status = 'refunded' THEN -fs.payment_amount ELSE fs.payment_amount END) AS revenue
    FROM 
        mart.f_sales fs
    GROUP BY 
        DATE_TRUNC('week', fs.date_id),
        fs.item_id,
        fs.customer_id
),
weekly_aggregated AS (
    SELECT
        week_start,
        item_id,
        COUNT(DISTINCT CASE WHEN orders_count = 1 AND refund_count = 0 THEN customer_id END) AS new_customers_count,
        COUNT(DISTINCT CASE WHEN orders_count > 1 AND refund_count = 0 THEN customer_id END) AS returning_customers_count,
        COUNT(DISTINCT CASE WHEN refund_count > 0 THEN customer_id END) AS refunded_customer_count,
        SUM(CASE WHEN orders_count = 1 AND refund_count = 0 THEN revenue ELSE 0 END) AS new_customers_revenue,
        SUM(CASE WHEN orders_count > 1 AND refund_count = 0 THEN revenue ELSE 0 END) AS returning_customers_revenue,
        SUM(refund_count) AS customers_refunded
    FROM 
        weekly_stats
    GROUP BY 
        week_start,
        item_id
)
SELECT
    new_customers_count,
    returning_customers_count,
    refunded_customer_count,
    'weekly' AS period_name,
    EXTRACT(WEEK FROM week_start) AS period_id,
    item_id,
    new_customers_revenue,
    returning_customers_revenue,
    customers_refunded
FROM 
    weekly_aggregated;