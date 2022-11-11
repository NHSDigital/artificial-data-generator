# Databricks notebook source
from pyspark.sql import DataFrame, types as T, functions as F


def coerce_schema(df: DataFrame, target_schema: T.StructType) -> DataFrame:
  """Transform the fields of a dataframe to a target schema (on a field-by-field basis).
  
  Fields that exist in the target schema but which are not present in the original dataframe
  will be added as nulls if the field is nullable. Otherwise a ValueError is raised

  Fields that exist in the original dataframe but not the target schema will be excluded
  from the result

  Args:
      df (pyspark.sql.DataFrame): Dataframe to transform
      target_schema (pyspark.sql.types.StructType): Intended schema of the result

  Raises:
      ValueError: If a non-nullable field exists in the target schema which is not in 
      the original dataframe

  Returns:
      DataFrame: transformed dataframe
  """
  selected_fields = []
  
  for field in target_schema.fields:
    if field.name in df.columns:
      value_col = F.col(field.name)
    else:
      if field.nullable:
        value_col = F.lit(None)
      else:
        raise ValueError(f"Field with name `{field.name}` is not nullable in target schema, but no column exists on provided dataframe!")
        
    selected_fields.append(value_col.cast(field.dataType).alias(field.name))
    
  return df.select(*selected_fields)