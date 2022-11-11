# Databricks notebook source
# MAGIC %run ./field_generators

# COMMAND ----------

from typing import Iterable
from pyspark.sql import DataFrame, functions as F

# COMMAND ----------

def relationship_generator(meta_df: DataFrame, base_df: DataFrame, variable_fields: Iterable[str]) -> DataFrame:
  """Generate a dataframe of IDs according to the cardinal relationships between fields described by the metadata.
  
  Args:
      meta_df (pyspark.sql.DataFrame): Contains relationship metadata. This is a frequency distribution of 
        cardinality, weight pairs - the weight for a given cardinality describes how likely it is the relationship
        between fields has that cardinality in the real data
      base_df (pyspark.sql.DataFrame): DataFrame containing a single column of IDs
      variable_fields (Iterable[str]): Name of fields in meta_df describing the variables to which the values relate
  
  Returns:
      pyspark.sql.DataFrame: Pairs of values for ID fields with the cardinal relationships described by the metadata.
        The generated IDs are stored in a column called 'VALUE_NUMERIC'
  
  """
  _relationship_generator = create_frequency_field_generator("RELATIONSHIP", "VALUE_NUMERIC")
  generated_df = _relationship_generator(meta_df, base_df, variable_fields)
  
  # Assign array of ids with length based on sampled value for the mapping
  # ids are created in chunks to avoid out of memory exceptions for large mapping values
  chunk_size = 100
  step_expr = f"int(least(greatest(1, VALUE_NUMERIC), {chunk_size}))"
  # Use the output ratio threshold to place a cap on the maximum cardinality that will be generated
  # If this cap is exceeded then the executors will fall over!
  output_ratio_threshold = spark.conf.get("spark.databricks.queryWatchdog.outputRatioThreshold")
  max_lower_bound_expr = f"int(least(coalesce(VALUE_NUMERIC, 0), {output_ratio_threshold}))"
  
  # Define the lower bounds for the id ranges
  id_lower_bound_col = F.expr(f"sequence(0, {max_lower_bound_expr}, {step_expr})")
  
  # Zip the bounds to get pairs of (lower, upper) for the ranges to generate
  id_bounds_col = F.expr("transform(slice(ID_LOWER_BOUNDS, 1, size(ID_LOWER_BOUNDS)-1), (x, i) -> struct(x AS LOWER, ID_LOWER_BOUNDS[i+1] AS UPPER))")
  
  generated_df = (
    generated_df
    .withColumn("ID_LOWER_BOUNDS", id_lower_bound_col)
    .withColumn("ID_BOUNDS", id_bounds_col)
    .select("*", F.expr("inline(ID_BOUNDS)"))
    .withColumn("VALUE_NUMERIC", F.expr("explode(sequence(LOWER, UPPER-1, 1))"))
  )
  
  return generated_df
  