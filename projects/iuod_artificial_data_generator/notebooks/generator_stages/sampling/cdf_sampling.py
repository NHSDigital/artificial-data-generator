# Databricks notebook source
# MAGIC %run ./field_definitions

# COMMAND ----------

from typing import Iterable
from pyspark.sql import DataFrame, functions as F

# COMMAND ----------

def sample_cdf(
  meta_df: DataFrame, 
  base_df: DataFrame, 
  variable_fields: Iterable[str], 
  value_field: str, 
  frequency_field: str, 
  bin_size: float=0.05
) -> DataFrame:
  """Generate data by sampling from a cumulative distribution over each field in the metadata.
  
  Args:
      meta_df (pyspark.sql.DataFrame): Contains metadata
      base_df (pyspark.sql.DataFrame): Contains rows to generate samples for
      variable_fields (Iterable[str]): Field labels, where each field has its own CDF to sample
      value_field (str): Values in the distribution (e.g. categories, bins etc)
      frequency_field (str): Frequencies to sum to compute the CDF
      bin_size (float (default = 0.05)): Bin size in the range_join, used for performance tuning
  
  Returns:
      pyspark.sql.DataFrame: Generated data
  """ 
  cumsum_col = cumsum_over_window(frequency_field, variable_fields, (frequency_field, value_field))
  total_col = total_over_window(frequency_field, variable_fields)
  cumdist_df = (
    meta_df
    .withColumn("CUMSUM", cumsum_col)
    .withColumn("TOTAL", total_col)
    .withColumn("CDF_BIN", get_cdf_bin(frequency_field=frequency_field))
  )
  
  # Select the sampled values from the cumulative distribution
  # Generate a random number and use to do a range join on the cumulative distribution
  # The sampled value is the one where the random number falls within the bin of the CDF corresponding
  # to that value
  generated_df = (
    base_df
    .withColumn("RAND", F.rand())
    .alias("left")
    .hint("range_join", bin_size)
    .join(
      cumdist_df.alias("right"), 
      how="inner", 
      on=[
        *[F.col(f"left.{field}") == F.col(f"right.{field}") for field in variable_fields],
        F.col("right.CDF_BIN.LOWER") <= F.col("left.RAND"),
        F.col("left.RAND") < F.col("right.CDF_BIN.UPPER"),
      ]
    )
  )
  
  # Clean up    
  ignore_fields = ["CUMSUM", "TOTAL", "CDF_BIN", "RAND", *[f"right.{field}" for field in variable_fields]]
  for field in ignore_fields:
    generated_df = generated_df.drop(F.col(field))
  
  return generated_df