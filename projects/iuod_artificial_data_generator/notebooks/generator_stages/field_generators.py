# Databricks notebook source
# MAGIC %run ./sampling/cdf_sampling

# COMMAND ----------

from functools import partial, reduce
from typing import Iterable, Callable, List

from pyspark.sql import DataFrame, functions as F

# Prevent spark from auto broadcasting the metadata
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

# COMMAND ----------

union_dfs = partial(reduce, DataFrame.unionByName)


def get_pivot_field_values(df: DataFrame, pivot_field: str) -> List[str]:
  """Get a list of the distinct values in a field to be used in the values argument of a
  call to DataFrame.pivot. These values will be the columns in the pivoted dataframe.

  Args:
      df (pyspark.sql.DataFrame): DataFrame to be pivoted
      pivot_field (str): Field to pivot by. Distinct values will be the columns of the pivoted dataframe

  Returns:
      List[str]: Distinct values in the pivot field
  """
  pivot_field_values = (
    df
    .select(pivot_field)
    .distinct()
    .collect()
  )
  pivot_field_values = [x[pivot_field] for x in pivot_field_values]
  return pivot_field_values


def generate_empty_samples(
  metadata_df, 
  sample_id_df, 
  variable_cols=("TABLE_NAME", "FIELD_NAME"),
) -> DataFrame:
  """ Cross-joins the variable names in the metadata with the rows of sample_id_df,
  so that each row in sample_id_df is associated with each distinct combination
  of variables in the metadata. 
  
  This doesnt actually do the sampling, it simply builds a tidy dataframe that 
  the samples are generated onto (see the example).
  
  Args:
      metadata_df (pyspark.sql.DataFrame): Contains metadata
      sample_id_df (pyspark.sql.DataFrame): Contains indexes to generate samples for
    
  Returns:
      pyspark.sql.DataFrame:  Cariable names in the metadata cross-joined with the rows of sample_id_df
    
  Example:
      >> metadata_df 
      | TABLE_NAME | FIELD_NAME | ... |
      | ---------- | ---------- | --- |
      |  table1    |   field1   | ... |
      |  table1    |   field2   | ... |
      |  table2    |   field1   | ... |
      
      >> sample_id_df 
      | SAMPLE_ID |
      | --------- |
      |     1     |
      |     2     |
      
      >> generate_empty_samples(metadata_df, sample_id_df)
      | SAMPLE_ID | TABLE_NAME | FIELD_NAME |
      | --------- | ---------- | ---------- |
      |     1     |  table1    |   field1   |
      |     1     |  table1    |   field2   |
      |     1     |  table2    |   field1   |
      |     2     |  table1    |   field1   |
      |     2     |  table1    |   field2   |
      |     2     |  table2    |   field1   |
        
  """  
  variable_df = (
    metadata_df
    .select(*variable_cols)
    .distinct()
  )
  
  sample_df = sample_id_df.crossJoin(variable_df.hint("broadcast"))
  
  return sample_df


def continuous_field_generator(meta_df: DataFrame, base_df: DataFrame, variable_fields: Iterable[str]) -> DataFrame:
  """Generate a dataframe of (long-formatted) values for fields with VALUE_TYPE='CONTINUOUS' in the metadata. 
  This is done by uniform sampling from bins defining the percentiles of a continuous distribution (i.e. 
  first we pick a bin with 1% width, then we sample a value from within that bin).

  Args:
      meta_df (pyspark.sql.DataFrame): Metadata for the dataset
      base_df (pyspark.sql.DataFrame): Base data to sample values for, indexed over patients / events.
      variable_fields (Iterable[str]): Names of metadata fields that define the fields to samples (e.g. 
        we generally want to sample per combination of 'TABLE_NAME' and 'FIELD_NAME')

  Returns:
      DataFrame: DataFrame of generated data (long-formatted), with a single value per 
        unique combination of variable_fields.
  """
  filtered_meta_df = meta_df.filter(F.col("VALUE_TYPE") == "CONTINUOUS")
  sample_df = generate_empty_samples(filtered_meta_df, base_df, variable_cols=variable_fields)

  # Sampling from percentile distributions
  bin_bounds_col = F.expr("transform(slice(VALUE_NUMERIC_ARRAY, 1, size(VALUE_NUMERIC_ARRAY)-1), (x, i) -> struct(x AS LOWER, VALUE_NUMERIC_ARRAY[i+1] AS UPPER))")
  sampled_bin_col = F.element_at(F.shuffle(bin_bounds_col), 1)
  sampled_value_col = F.col("SAMPLED_BIN.LOWER") + F.rand() * (F.col("SAMPLED_BIN.UPPER") - F.col("SAMPLED_BIN.LOWER"))

  generated_df = (
    filtered_meta_df.filter("VALUE_TYPE == 'CONTINUOUS'")
    .join(sample_df, how="inner", on=variable_fields)
    .withColumn("SAMPLED_BIN", sampled_bin_col)  # This needs to be attached using withColumn before sampled_value_col. Trying to do in a single step gives strange results!
    .withColumn("VALUE_NUMERIC", sampled_value_col)
  )
  
  generated_df = generated_df.select(*sample_df.columns, "VALUE_NUMERIC")
  
  return generated_df


def create_frequency_field_generator(
  value_type: str, 
  *value_fields: Iterable[str]
) -> Callable[[DataFrame, DataFrame, Iterable[str]], DataFrame]:
  """Creates a function that generates a dataframe of (long-formatted) values for fields by sampling from a 
  frequency distribution (i.e. of value, weight pairs).

  Args:
      value_type (str): Defines filter on the VALUE_TYPE column in the metadata, which specifies which type of 
        metadata this generator operates on (e.g. 'CONTINUOUS', 'DATE', 'DISCRETE')
      *value_fields (Iterable[str]): Names of columns in the metadata which contain the values that the frequency
        distribution is over (each set of values is paired with a weight which defines its frequency)

  Returns:
      Callable[[DataFrame, DataFrame, Iterable[str]], DataFrame]: Generates a dataframe of values by sampling from a 
        frequency distribution
  """
  value_type_condition_col = F.col("VALUE_TYPE") == value_type
  value_struct_col = F.struct(*value_fields)
    
  def _field_generator(meta_df: DataFrame, base_df: DataFrame, variable_fields: Iterable[str]) -> DataFrame:
    """Generates a dataframe of values for fields by sampling from a frequency distribution (i.e. value, weight pairs).

    Args:
        meta_df (pyspark.sql.DataFrame): Metadata for the dataset
        base_df (pyspark.sql.DataFrame): Base data to sample values for, indexed over patients / events.
        variable_fields (Iterable[str]): Names of metadata fields that define the fields to samples (e.g. 
          we generally want to sample per combination of 'TABLE_NAME' and 'FIELD_NAME')

    Returns:
        DataFrame: DataFrame of generated data (long-formatted), with a single value per 
          unique combination of variable_fields.
    """
    filtered_meta_df = meta_df.filter(value_type_condition_col)
    sample_df = generate_empty_samples(filtered_meta_df, base_df, variable_cols=variable_fields)
    
    filtered_meta_df = filtered_meta_df.withColumn("VALUE_STRUCT", value_struct_col)
    
    generated_df = sample_cdf(
      filtered_meta_df, 
      sample_df, 
      value_field="VALUE_STRUCT", 
      frequency_field="WEIGHT",
      variable_fields=variable_fields
    )
    
    # Select the sampled values, explode the struct after sampling
    generated_df = generated_df.selectExpr(*sample_df.columns, "VALUE_STRUCT.*")

    return generated_df
  
  return _field_generator


categorical_field_generator = create_frequency_field_generator("CATEGORICAL", "VALUE_STRING")
date_field_generator        = create_frequency_field_generator("DATE", "VALUE_DATE")
discrete_field_generator    = create_frequency_field_generator("DISCRETE", "VALUE_NUMERIC")


def field_generator(
  meta_df: DataFrame, 
  base_df: DataFrame,
  pivot_field: str,
  other_variable_fields: Iterable[str]=tuple()
) -> DataFrame:
  """Generates a dataframe of (wide-formatted) values for fields by sampling from metadata.

  Args:
      meta_df (pyspark.sql.DataFrame): Metadata for the dataset
      base_df (pyspark.sql.DataFrame): Base data to sample values for, indexed over patients / events.
      pivot_field (str): Name of the field in the metadata that we want to pivot the resulting generated
        dataframe by. Generally the values in this field are the names of the fields in the dataset
        the artificial data represents
      other_variable_fields (Iterable[str], optional): Fields that, in addition to the pivot field, define
        the unique fields in the metadata. Defaults to tuple().

  Returns:
      DataFrame: DataFrame of generated data (wide-formatted), with a single value per 
        unique combination of pivot_field and other_variable_fields.
  """
  filtered_meta_df = meta_df.filter(F.col("VALUE_TYPE").isin(["CATEGORICAL", "DISCRETE", "DATE", "CONTINUOUS"]))
    
  variable_fields = [pivot_field, *other_variable_fields]
  generator_args = (filtered_meta_df, base_df, variable_fields)
  generated_long_df = union_dfs([
    categorical_field_generator(*generator_args).select(*variable_fields, *base_df.columns, F.col("VALUE_STRING") .alias("VALUE")),
    date_field_generator(       *generator_args).select(*variable_fields, *base_df.columns, F.col("VALUE_DATE")   .alias("VALUE")),
    discrete_field_generator(   *generator_args).select(*variable_fields, *base_df.columns, F.col("VALUE_NUMERIC").alias("VALUE")),
    continuous_field_generator( *generator_args).select(*variable_fields, *base_df.columns, F.col("VALUE_NUMERIC").alias("VALUE")),
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