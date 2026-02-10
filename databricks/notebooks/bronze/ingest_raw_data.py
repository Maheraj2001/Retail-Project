# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Raw Data Ingestion
# MAGIC Ingest raw data from source into Delta Lake (Bronze layer).
# MAGIC - No transformations applied
# MAGIC - Schema evolution enabled
# MAGIC - Audit columns added (ingestion timestamp, source file)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Widget parameters (set via ADF or Databricks workflow)
dbutils.widgets.text("source_path", "", "Source file path")
dbutils.widgets.text("target_table", "", "Target Delta table name")
dbutils.widgets.text("file_format", "csv", "Source file format")

source_path = dbutils.widgets.get("source_path")
target_table = dbutils.widgets.get("target_table")
file_format = dbutils.widgets.get("file_format")

# Storage paths
BRONZE_PATH = "abfss://bronze@${storage_account}.dfs.core.windows.net"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Raw Data

# COMMAND ----------

def read_source_data(path: str, fmt: str):
    """Read source data with schema inference and header detection."""
    reader = spark.read.format(fmt).option("inferSchema", "true")

    if fmt == "csv":
        reader = reader.option("header", "true").option("multiLine", "true")
    elif fmt == "json":
        reader = reader.option("multiLine", "true")

    return reader.load(path)

raw_df = read_source_data(source_path, file_format)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Audit Columns

# COMMAND ----------

bronze_df = (
    raw_df
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source_file", input_file_name())
    .withColumn("_batch_id", lit(dbutils.notebook.entry_point.getDbutils().notebook().getContext().currentRunId().toString()))
)

print(f"Records to ingest: {bronze_df.count()}")
bronze_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Bronze (Delta Lake)

# COMMAND ----------

(
    bronze_df
    .write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")  # Allow schema evolution
    .saveAsTable(f"bronze.{target_table}")
)

print(f"Successfully ingested data to bronze.{target_table}")

# COMMAND ----------

# Return success status for ADF pipeline
dbutils.notebook.exit("SUCCESS")
