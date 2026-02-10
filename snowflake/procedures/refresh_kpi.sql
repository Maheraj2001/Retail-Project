-- =============================================================
-- Stored Procedure: Refresh KPI Daily Summary
-- Can be triggered via Snowflake Task or external orchestrator
-- =============================================================
USE DATABASE DATALAKEHOUSE;
USE SCHEMA GOLD;

CREATE OR REPLACE PROCEDURE sp_refresh_kpi_daily_summary()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    MERGE INTO kpi_daily_summary AS target
    USING (
        SELECT
            date_key,
            COUNT(DISTINCT transaction_id)              AS total_transactions,
            COUNT(DISTINCT customer_id)                 AS unique_customers,
            ROUND(SUM(net_revenue), 2)                  AS total_revenue,
            ROUND(AVG(net_revenue), 2)                  AS avg_order_value,
            ROUND(SUM(gross_revenue - net_revenue), 2)  AS total_discounts,
            CURRENT_TIMESTAMP()                         AS _computed_at
        FROM fact_sales
        GROUP BY date_key
    ) AS source
    ON target.date_key = source.date_key
    WHEN MATCHED THEN UPDATE SET
        total_transactions  = source.total_transactions,
        unique_customers    = source.unique_customers,
        total_revenue       = source.total_revenue,
        avg_order_value     = source.avg_order_value,
        total_discounts     = source.total_discounts,
        _computed_at        = source._computed_at
    WHEN NOT MATCHED THEN INSERT (
        date_key, total_transactions, unique_customers,
        total_revenue, avg_order_value, total_discounts, _computed_at
    ) VALUES (
        source.date_key, source.total_transactions, source.unique_customers,
        source.total_revenue, source.avg_order_value, source.total_discounts, source._computed_at
    );

    RETURN 'KPI refresh completed at ' || CURRENT_TIMESTAMP()::STRING;
END;
$$;

-- Schedule daily refresh
CREATE OR REPLACE TASK task_refresh_kpi
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 6 * * * America/New_York'
AS
    CALL sp_refresh_kpi_daily_summary();

ALTER TASK task_refresh_kpi RESUME;
