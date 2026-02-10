-- =============================================================
-- Gold Layer Tables — Star Schema
-- =============================================================
USE DATABASE DATALAKEHOUSE;
USE SCHEMA GOLD;

-- ---------- Dimension: Date ----------
CREATE OR REPLACE TABLE dim_date (
    date_key        DATE        PRIMARY KEY,
    year            INT         NOT NULL,
    month           INT         NOT NULL,
    day_of_week     INT         NOT NULL,
    month_name      VARCHAR(20) NOT NULL,
    quarter         INT         NOT NULL,
    is_weekend      BOOLEAN     NOT NULL
);

-- ---------- Dimension: Customer ----------
CREATE OR REPLACE TABLE dim_customer (
    customer_id     VARCHAR(50) PRIMARY KEY,
    customer_name   VARCHAR(200),
    segment         VARCHAR(50),
    region          VARCHAR(50),
    city            VARCHAR(100),
    state           VARCHAR(100),
    country         VARCHAR(100)
);

-- ---------- Dimension: Product ----------
CREATE OR REPLACE TABLE dim_product (
    product_id      VARCHAR(50) PRIMARY KEY,
    product_name    VARCHAR(200),
    category        VARCHAR(100),
    sub_category    VARCHAR(100),
    brand           VARCHAR(100)
);

-- ---------- Fact: Sales ----------
CREATE OR REPLACE TABLE fact_sales (
    transaction_id  VARCHAR(50) PRIMARY KEY,
    date_key        DATE        NOT NULL REFERENCES dim_date(date_key),
    customer_id     VARCHAR(50) NOT NULL REFERENCES dim_customer(customer_id),
    product_id      VARCHAR(50) NOT NULL REFERENCES dim_product(product_id),
    quantity        INT         NOT NULL,
    unit_price      DECIMAL(12,2) NOT NULL,
    discount        DECIMAL(5,4) DEFAULT 0,
    net_revenue     DECIMAL(14,2) NOT NULL,
    gross_revenue   DECIMAL(14,2) NOT NULL
)
CLUSTER BY (date_key);

-- ---------- KPI Summary ----------
CREATE OR REPLACE TABLE kpi_daily_summary (
    date_key            DATE PRIMARY KEY,
    total_transactions  INT,
    unique_customers    INT,
    total_revenue       DECIMAL(14,2),
    avg_order_value     DECIMAL(14,2),
    total_discounts     DECIMAL(14,2),
    _computed_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
