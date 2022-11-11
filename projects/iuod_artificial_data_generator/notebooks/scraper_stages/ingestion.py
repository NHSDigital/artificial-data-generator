# Databricks notebook source
# MAGIC %run ../common/common_exports

# COMMAND ----------

from pyspark.sql import DataFrame, functions as F

# COMMAND ----------

def ingest(source_database: str, source_table: str) -> DataFrame:
  """Read the data from the source table
  
  Args:
      source_database (str):  Database name containing source table
      source_table (str): Name of source table
  
  Returns:
      pyspark.sql.DataFrame: DataFrame with data to scrape
    
  """
  source_table_fullname = f"{source_database}.{source_table}"
  dataset_df = (
    spark.table(source_table_fullname)
    .withColumn("TABLE_NAME", F.lit(source_table))
  )
  
  return dataset_df