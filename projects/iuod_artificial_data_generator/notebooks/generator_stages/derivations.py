# Databricks notebook source
from typing import Iterable, Tuple
from pyspark.sql import DataFrame, Column


def with_derived_fields(df: DataFrame, *derived_fields: Iterable[Tuple[str, Column]]) -> DataFrame: 
  """Add columns to a dataframe, with values possibly derived from existing columns.

  Args:
      df (DataFrame): DataFrame to add fields to
      *derived_fields (Iterable[Tuple[str, Column]]): Pairs of field_name, column_spec defining the columns to add

  Returns:
      DataFrame: New DataFrame with additional derived fields

  Notes:
      If it is not possible to add a particular column due to an exception, then a printout will appear
      and the column will be skipped. The resulting dataframe will not have this column added to it.
  """
  for field_name, col_spec in derived_fields:
    try:
      df = df.withColumn(field_name, col_spec)
    except Exception as e:
      # TODO: replace generic Exception
      print(f"Failed to derive field `{field_name}`: {e}")
      continue
  
  return df