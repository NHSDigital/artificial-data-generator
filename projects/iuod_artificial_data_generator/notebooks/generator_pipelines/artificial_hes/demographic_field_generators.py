# Databricks notebook source
# MAGIC %run ../../generator_stages/sampling/field_definitions

# COMMAND ----------

# MAGIC %run ../../generator_stages/field_generators

# COMMAND ----------

from typing import Iterable, Tuple
from pyspark.sql import DataFrame, Column, functions as F, Window as W


# region Prevent downstream linter highlights
create_frequency_field_generator = create_frequency_field_generator
cumsum_over_window = cumsum_over_window
total_over_window = total_over_window
get_cdf_bin = get_cdf_bin
union_dfs = union_dfs
get_pivot_field_values = get_pivot_field_values
# endregion


def demographic_categorical_field_generator(
  meta_df: DataFrame, 
  base_df: DataFrame, 
  variable_fields: Iterable[str], 
  index_fields: Tuple[str, str]
) -> DataFrame:
  """Generate a dataframe of values for fields with VALUE_TYPE='DEMOGRAPHIC_CATEGORICAL' in the metadata
  These are fields such as sex or ethnicity, which in principle have a single underlying value for a given
  patient, but which in practice can have multiple values recorded across different visits. We want to 
  sample in such a way that the patterns of 'errors' that we would see in real data are represented in 
  artificial data.

  Note: this logic is hes-specific.

  Args:
      meta_df (DataFrame): Metadata for the dataset
      base_df (DataFrame): Base data to sample values for, indexed over patients / events.
      variable_fields (Iterable[str]): Names of metadata fields that define the fields to samples (e.g. 
        we generally want to sample per combination of 'TABLE_NAME' and 'FIELD_NAME')
      index_fields (Tuple[str, str]): Fields in the base data that hold the patient / event indexes

  Returns:
      DataFrame: DataFrame of generated data (long-formatted), with a single value per unique combination of 
        variable_fields and index_fields.
  """
  
  # Sample value per row in the base dataframe
  frequency_field_generator = create_frequency_field_generator("DEMOGRAPHIC_CATEGORICAL", "VALUE_STRING_ARRAY", "WEIGHT_ARRAY")
  generated_df = frequency_field_generator(meta_df, base_df, variable_fields)
  
  # Create duplicate rows over the first index
  zeroth_index_window = W.partitionBy(*variable_fields, index_fields[0]).orderBy("RAND")
  generated_df = (
    generated_df
    .withColumn("RAND", F.rand())
    .withColumn("VALUE_STRING_ARRAY", F.first("VALUE_STRING_ARRAY").over(zeroth_index_window))
    .withColumn("WEIGHT_ARRAY", F.first("WEIGHT_ARRAY").over(zeroth_index_window))
  )
  
  # We need to pick a value from each of the sampled arrays of values according to the associated array of weights.
  # This is done by generating a random number for each sampled array, before exploding the array.
  # All the rows that result from each exploded array have the same value for the random number
  # so that we can use the random number to pick a single value from the array according to the CDF
  generated_df = (
    generated_df
    .withColumn("RAND", F.rand())  # Get random number for each array
    .select("*", F.posexplode_outer("VALUE_STRING_ARRAY").alias("ARRAY_POSITION", "VALUE_STRING"))  # Explode value array with position
    .withColumn("WEIGHT", F.element_at(F.col("WEIGHT_ARRAY"), F.col("ARRAY_POSITION") + 1))  # Get weight for each value according to position
  )
    
  # Build CDFs: we want to pick a single value for each distribution (i.e. per unique combination of variables and indexes)
  cumsum_weight_col = cumsum_over_window("WEIGHT", (*variable_fields, *index_fields), ("WEIGHT", "VALUE_STRING"))
  total_weight_col = total_over_window("WEIGHT", (*variable_fields, *index_fields))
  
  generated_df = (
    generated_df
    .withColumn("CUMSUM", cumsum_weight_col)
    .withColumn("TOTAL", total_weight_col)
    .withColumn("CDF_BIN", get_cdf_bin())
  )
  
  # Pick a value from the sampled array according to the CDF
  rand_in_cdf_bin = F.expr("(CDF_BIN.LOWER <= RAND) AND (RAND < CDF_BIN.UPPER)")
  generated_df = generated_df.filter(rand_in_cdf_bin)
  
  generated_df = generated_df.select(*variable_fields, *index_fields, "VALUE_STRING")
  
  return generated_df


def demographic_date_field_generator(
  meta_df: DataFrame, 
  base_df: DataFrame, 
  variable_fields: Iterable[str], 
  index_fields: Tuple[str, str],
) -> DataFrame:
  """Generate a dataframe of values for fields with VALUE_TYPE='DEMOGRAPHIC_DATE' in the metadata
  These are fields such as date of birth, which in principle have a single underlying value for a given
  patient, but which in practice can have multiple values recorded across different visits. We want to 
  sample in such a way that the patterns of 'errors' that we would see in real data are represented in 
  artificial data.

  Note: this logic is hes-specific.

  Args:
      meta_df (DataFrame): Metadata for the dataset
      base_df (DataFrame): Base data to sample values for, indexed over patients / events.
      variable_fields (Iterable[str]): Names of metadata fields that define the fields to samples (e.g. 
        we generally want to sample per combination of 'TABLE_NAME' and 'FIELD_NAME')
      index_fields (Tuple[str, str]): Fields in the base data that hold the patient / event indexes

  Returns:
      DataFrame: DataFrame of generated data (long-formatted), with a single value per unique combination of 
        variable_fields and index_fields.
  """
  # Sample value per row in the base dataframe
  frequency_field_generator = create_frequency_field_generator("DEMOGRAPHIC_DATE", "VALUE_STRING", "VALUE_NUMERIC")
  generated_df = frequency_field_generator(meta_df, base_df, variable_fields)
  
  # Create duplicate rows over the first index
  zeroth_index_window = W.partitionBy(*variable_fields, index_fields[0]).orderBy("RAND")
  generated_df = (
    generated_df
    .withColumn("RAND", F.rand())
    .withColumn("TRUE_VALUE", F.first("VALUE_STRING").over(zeroth_index_window))  # 'True' value for the group
    .withColumn("TRUE_INCIDENCE", F.first("VALUE_NUMERIC").over(zeroth_index_window))  # Incidence of 'true' value
    .withColumn("ALT_VALUE_ARRAY", F.collect_list("VALUE_STRING").over(zeroth_index_window))  # Alternative values for the group
  )
  
  # Pick an alternative value at random
  alt_value_col = F.element_at(F.shuffle(F.expr("filter(ALT_VALUE_ARRAY, x -> x != TRUE_VALUE)")), 1)
  # Handle case when all sampled values are the same
  alt_value_col = F.coalesce(alt_value_col, F.col("TRUE_VALUE"))
  
  # Apply noise
  noisy_value_col = F.when(F.expr("nvl(TRUE_INCIDENCE, 0.0)") < F.rand(), alt_value_col).otherwise(F.col("TRUE_VALUE"))
  generated_df = generated_df.withColumn("VALUE_STRING", noisy_value_col)
  
  generated_df = generated_df.select(*variable_fields, *index_fields, "VALUE_STRING")
  
  return generated_df


def demographic_field_generator(
  meta_df: DataFrame, 
  base_df: DataFrame,
  pivot_field: str, 
  index_fields: Iterable[str], 
  other_variable_fields: Iterable[str]=tuple(), 
) -> DataFrame: 
  """Generate a dataframe of values for fields with VALUE_TYPE='DEMOGRAPHIC_DATE' or 'DEMOGRAPHIC_CATEGORICAL' 
  in the metadata

  Args:
      meta_df (DataFrame): Metadata for the dataset
      base_df (DataFrame): Base data to sample values for, indexed over patients / events.
      pivot_field (str): Name of the field in the metadata that we want to pivot the resulting generated
        dataframe by. Generally the values in this field are the names of the fields in the dataset
        the artificial data represents
      index_fields (Tuple[str, str]): Fields in the base data that hold the patient / event indexes
      other_variable_fields (Iterable[str], optional): Fields that, in addition to the pivot field, define
        the unique fields in the metadata. Defaults to tuple().

  Returns:
      DataFrame: DataFrame of generated data (wide-formatted). The unique values within the pivot_field defining the columns.
      There is a single row per unique combination of index_fields and other_variable_fields 
  """
  # HES-specific
  filtered_meta_df = meta_df.filter(F.col("VALUE_TYPE").isin(["DEMOGRAPHIC_CATEGORICAL", "DEMOGRAPHIC_DATE"]))
  
  # Generate values for the different value types and union the results
  variable_fields = [pivot_field, *other_variable_fields]
  generator_args = (filtered_meta_df, base_df, variable_fields, index_fields)
  generated_long_df = union_dfs([
    demographic_categorical_field_generator(*generator_args).select(*variable_fields, *index_fields, F.col("VALUE_STRING").alias("VALUE")),
    demographic_date_field_generator(*generator_args)       .select(*variable_fields, *index_fields, F.col("VALUE_STRING").alias("VALUE")),
  ])
  
  # Pivot to wide format
  pivot_field_values = get_pivot_field_values(filtered_meta_df, pivot_field)
  generated_wide_df = (
    generated_long_df
    .groupBy(*base_df.columns)
    .pivot(pivot_field, pivot_field_values)
    .agg(F.first("VALUE"))
  )
  
  return generated_wide_df