# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Data Cleansing & Transformation
# MAGIC Transforms Bronze data into cleansed, conformed Silver tables.
# MAGIC - Deduplication
# MAGIC - Data type enforcement
# MAGIC - Null handling & validation
# MAGIC - SCD Type 2 for dimension tables

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, current_timestamp, md5, concat_ws, row_number, when, lit, coalesce
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("source_table", "", "Bronze source table")
dbutils.widgets.text("target_table", "", "Silver target table")
dbutils.widgets.text("primary_keys", "", "Comma-separated primary key columns")
dbutils.widgets.text("scd_type", "1", "SCD type: 1 (overwrite) or 2 (history)")

source_table = dbutils.widgets.get("source_table")
target_table = dbutils.widgets.get("target_table")
primary_keys = dbutils.widgets.get("primary_keys").split(",")
scd_type = dbutils.widgets.get("scd_type")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze Data

# COMMAND ----------

bronze_df = spark.read.table(f"bronze.{source_table}")
print(f"Bronze records: {bronze_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deduplication

# COMMAND ----------

def deduplicate(df: DataFrame, keys: list, order_col: str = "_ingestion_timestamp") -> DataFrame:
    """Remove duplicates keeping the latest record per key."""
    window = Window.partitionBy(*keys).orderBy(col(order_col).desc())
    return (
        df.withColumn("_row_num", row_number().over(window))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )

deduped_df = deduplicate(bronze_df, primary_keys)
print(f"After dedup: {deduped_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks

# COMMAND ----------

def apply_quality_checks(df: DataFrame) -> DataFrame:
    """Apply standard data quality rules."""
    # Remove records where all primary keys are null
    for key in primary_keys:
        df = df.filter(col(key).isNotNull())

    # Add hash key for change detection
    non_audit_cols = [c for c in df.columns if not c.startswith("_")]
    df = df.withColumn("_row_hash", md5(concat_ws("||", *[coalesce(col(c).cast("string"), lit("NULL")) for c in non_audit_cols])))

    return df

quality_df = apply_quality_checks(deduped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD Type 2 Merge (Dimension Tables)

# COMMAND ----------

def scd_type2_merge(source: DataFrame, target_table_name: str, keys: list):
    """Perform SCD Type 2 merge into target Delta table."""
    silver_df = (
        source
        .withColumn("_valid_from", current_timestamp())
        .withColumn("_valid_to", lit(None).cast("timestamp"))
        .withColumn("_is_current", lit(True))
    )

    if not DeltaTable.isDeltaTable(spark, f"silver.{target_table_name}"):
        silver_df.write.format("delta").saveAsTable(f"silver.{target_table_name}")
        return

    target = DeltaTable.forName(spark, f"silver.{target_table_name}")
    merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in keys])

    (
        target.alias("target")
        .merge(silver_df.alias("source"), f"{merge_condition} AND target._is_current = true")
        .whenMatchedUpdate(
            condition="target._row_hash != source._row_hash",
            set={
                "_valid_to": current_timestamp(),
                "_is_current": lit(False),
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

def scd_type1_merge(source: DataFrame, target_table_name: str, keys: list):
    """Perform SCD Type 1 upsert into target Delta table."""
    if not DeltaTable.isDeltaTable(spark, f"silver.{target_table_name}"):
        source.write.format("delta").saveAsTable(f"silver.{target_table_name}")
        return

    target = DeltaTable.forName(spark, f"silver.{target_table_name}")
    merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in keys])

    (
        target.alias("target")
        .merge(source.alias("source"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Merge

# COMMAND ----------

if scd_type == "2":
    scd_type2_merge(quality_df, target_table, primary_keys)
else:
    scd_type1_merge(quality_df, target_table, primary_keys)

print(f"Silver table silver.{target_table} updated successfully")

# COMMAND ----------

dbutils.notebook.exit("SUCCESS")
