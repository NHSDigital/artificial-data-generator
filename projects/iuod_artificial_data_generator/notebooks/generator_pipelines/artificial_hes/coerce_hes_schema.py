# Databricks notebook source
# MAGIC %run ../../dataset_definitions/hes/hes_schemas

# COMMAND ----------

from pyspark.sql import DataFrame, functions as F

# region Prevent downstream linting highlights
get_hes_schema = get_hes_schema
# endregion

def coerce_hes_schema(df: DataFrame, hes_dataset: str) -> DataFrame:
  """Transform the schema of a dataframe to match the schema of a given HES dataset

  Args:
      df (DataFrame): DataFrame to transform
      hes_dataset (str): HES dataset with the target schema. Either hes_apc, hes_op or hes_ae

  Returns:
      DataFrame: New DataFrame transformed to the target schema
  """
  current_fields = {f.name: f for f in df.schema.fields}
  coerced_fields = []

  for field in get_hes_schema(hes_dataset).fields:
    target_datatype = field.dataType
      
    if field.name not in current_fields:
      print(f"Warning: field `{field.name}` missing from schema! Adding field and filling with nulls")
      value_col = F.lit(None)
    else:
      value_col = F.col(field.name)

    coerced_fields.append(value_col.cast(target_datatype).alias(field.name))
    
  coerced_df = df.select(*coerced_fields)
  
  return coerced_df

# COMMAND ----------

