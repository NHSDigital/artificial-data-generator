# Databricks notebook source
# MAGIC %run ./aggregation_parameters

# COMMAND ----------

# MAGIC %run ../../common/coerce_schema

# COMMAND ----------

# MAGIC %run ../schemas/meta_schema

# COMMAND ----------

from functools import reduce
from typing import Iterable, Callable, Dict, Union

from pyspark.sql import functions as F, DataFrame, Column


# region Prevent downstream linting highlights
coerce_schema = coerce_schema  
AggregationParametersType = AggregationParametersType
get_meta_schema = get_meta_schema
# endregion


def df_aggregation_reducer(
  df: DataFrame, 
  aggregation_params: AggregationParametersType
) -> DataFrame:
  """Apply a sequence of aggregations, filters and derivations to a dataframe

  Args:
      df (DataFrame): DataFrame to aggregate
      aggregation_params (AggregationParametersType): Defines the aggregations.
        This is an iterable of keyed objects, where each element has the keys
        'grouping_fields', 'agg_cols', 'derivations' and 'filters': the first 2 
        are used in the DataFrame.groupBy(...).agg(...); the 3rd defines columns
        to add to the dataframe after the aggregation; and the 4th is a set 
        of filter columns to apply to the output 

  Returns:
      DataFrame: Aggregated data
  """
  def _reducer(acc, x):
    acc = (
      acc
      .groupBy(*x["grouping_fields"])
      .agg(*x["agg_cols"])
    )
    
    for derived_col in x.get("derivations", []):
      col_name = acc.select(derived_col).columns[0]
      acc = acc.withColumn(col_name, derived_col)
      
    for condition_col in x.get("filters", []):
      acc = acc.filter(condition_col)

    return acc

  agg_df = reduce(_reducer,  aggregation_params, df)
  
  return agg_df


def create_summariser(
  value_type: str, 
  aggregation_params: AggregationParametersType
) -> Callable[[DataFrame], DataFrame]:
  """Create a function which summarises (long-formatted) data with a given type.

  Args:
      value_type (str): Describes type of data to summarise (e.g. 'CATEGORICAL')
      aggregation_params (AggregationParametersType): Defines the aggregations to summarise the data

  Returns:
      Callable[[DataFrame], DataFrame]: Summarises long-formatted data
  """
  value_type_condition_col = F.col("VALUE_TYPE") == value_type

  def _summariser(df: DataFrame) -> DataFrame:
    filtered_df = df.filter(value_type_condition_col)
    aggregated_df = df_aggregation_reducer(filtered_df, aggregation_params)
    meta_df = coerce_schema(aggregated_df, get_meta_schema())
    return meta_df

  return _summariser