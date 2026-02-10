-- =============================================================
-- Analytical Views for Power BI / Reporting
-- =============================================================
USE DATABASE DATALAKEHOUSE;
USE SCHEMA GOLD;

-- Monthly revenue trend
CREATE OR REPLACE VIEW v_monthly_revenue AS
SELECT
    d.year,
    d.month,
    d.month_name,
    COUNT(DISTINCT f.transaction_id)    AS total_orders,
    COUNT(DISTINCT f.customer_id)       AS unique_customers,
    ROUND(SUM(f.net_revenue), 2)        AS total_revenue,
    ROUND(AVG(f.net_revenue), 2)        AS avg_order_value
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;

-- Customer segment analysis
CREATE OR REPLACE VIEW v_customer_segments AS
SELECT
    c.segment,
    c.region,
    COUNT(DISTINCT c.customer_id)       AS customer_count,
    ROUND(SUM(f.net_revenue), 2)        AS total_revenue,
    ROUND(AVG(f.net_revenue), 2)        AS avg_revenue_per_order,
    COUNT(DISTINCT f.transaction_id)    AS total_orders
FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
GROUP BY c.segment, c.region;

-- Product performance
CREATE OR REPLACE VIEW v_product_performance AS
SELECT
    p.category,
    p.sub_category,
    COUNT(DISTINCT f.transaction_id)    AS total_orders,
    SUM(f.quantity)                     AS total_units_sold,
    ROUND(SUM(f.net_revenue), 2)        AS total_revenue,
    ROUND(SUM(f.gross_revenue - f.net_revenue), 2) AS total_discount_given
FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY p.category, p.sub_category;
