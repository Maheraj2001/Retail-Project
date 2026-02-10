# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — Business Aggregations
# MAGIC Builds business-ready aggregated tables from Silver layer.
# MAGIC - Star schema fact/dimension tables
# MAGIC - Pre-computed KPIs and metrics
# MAGIC - Optimized for BI consumption (Power BI, Snowflake)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, count, avg, min as _min, max as _max,
    current_timestamp, date_format, year, month, dayofweek,
    round as _round
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension Tables

# COMMAND ----------

def build_dim_date():
    """Build date dimension from Silver data."""
    date_df = (
        spark.sql("""
            SELECT DISTINCT date AS date_key
            FROM silver.transactions
            WHERE date IS NOT NULL
        """)
        .withColumn("year", year("date_key"))
        .withColumn("month", month("date_key"))
        .withColumn("day_of_week", dayofweek("date_key"))
        .withColumn("month_name", date_format("date_key", "MMMM"))
        .withColumn("quarter", ((month("date_key") - 1) / 3 + 1).cast("int"))
        .withColumn("is_weekend", dayofweek("date_key").isin(1, 7))
    )

    date_df.write.format("delta").mode("overwrite").saveAsTable("gold.dim_date")
    print(f"dim_date: {date_df.count()} rows")

def build_dim_customer():
    """Build customer dimension (current snapshot)."""
    customer_df = spark.sql("""
        SELECT
            customer_id,
            customer_name,
            segment,
            region,
            city,
            state,
            country
        FROM silver.customers
        WHERE _is_current = true
    """)

    customer_df.write.format("delta").mode("overwrite").saveAsTable("gold.dim_customer")
    print(f"dim_customer: {customer_df.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact Tables

# COMMAND ----------

def build_fact_sales():
    """Build sales fact table with measures."""
    fact_df = spark.sql("""
        SELECT
            t.transaction_id,
            t.date          AS date_key,
            t.customer_id,
            t.product_id,
            t.quantity,
            t.unit_price,
            t.discount,
            (t.quantity * t.unit_price * (1 - COALESCE(t.discount, 0))) AS net_revenue,
            (t.quantity * t.unit_price) AS gross_revenue
        FROM silver.transactions t
    """)

    (
        fact_df
        .write
        .format("delta")
        .mode("overwrite")
        .partitionBy("date_key")
        .saveAsTable("gold.fact_sales")
    )
    print(f"fact_sales: {fact_df.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-Computed KPI Summary

# COMMAND ----------

def build_kpi_summary():
    """Build daily KPI summary for dashboards."""
    kpi_df = spark.sql("""
        SELECT
            date_key,
            COUNT(DISTINCT transaction_id)              AS total_transactions,
            COUNT(DISTINCT customer_id)                 AS unique_customers,
            ROUND(SUM(net_revenue), 2)                  AS total_revenue,
            ROUND(AVG(net_revenue), 2)                  AS avg_order_value,
            ROUND(SUM(gross_revenue - net_revenue), 2)  AS total_discounts
        FROM gold.fact_sales
        GROUP BY date_key
    """).withColumn("_computed_at", current_timestamp())

    kpi_df.write.format("delta").mode("overwrite").saveAsTable("gold.kpi_daily_summary")
    print(f"kpi_daily_summary: {kpi_df.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Gold Layer Build

# COMMAND ----------

build_dim_date()
build_dim_customer()
build_fact_sales()
build_kpi_summary()

# COMMAND ----------

# Optimize Gold tables for query performance
spark.sql("OPTIMIZE gold.fact_sales ZORDER BY (customer_id, date_key)")
spark.sql("OPTIMIZE gold.kpi_daily_summary ZORDER BY (date_key)")

# COMMAND ----------

dbutils.notebook.exit("SUCCESS")
