# Databricks notebook source
from typing import Dict, List, Union
from pyspark.sql import functions as F, Column


BASE_GROUPING_FIELDS = ["TABLE_NAME", "FIELD_NAME", "VALUE_TYPE"]
weight_col = F.count(F.lit(1)).cast("double").alias("WEIGHT")
AggregationParametersType = List[Dict[str, Union[List[str], List[Column]]]]


def round_to_nearest_interval(col: Column, interval: int=5) -> Column:
  """Build a new pyspark column by rounding a given column to a specified interval.

  Args:
      col (Column): Original column to be rounded
      interval (int, optional): Interval to round to. Defaults to 5.

  Returns:
      Column: Rounded column
  """
  rounded_col = (F.round(col / interval, 0) * interval)
  return rounded_col


def get_frequency_aggregation_params(
  value_field: str, 
  frequency_rounding: int, 
  min_frequency: int
) -> AggregationParametersType:
  """Get the parameters required to aggregate frequency distributions and apply
  disclosure controls to the resulting frequencies.

  Args:
      value_field (str): Values to calculate the frequency of
      frequency_rounding (int): Rounding applied to frequencies
      min_frequency (int): Cutoff below which frequencies / values are excluded

  Returns:
      AggregationParametersType: Parameters to do the aggregations
  """
  
  # Need to make sure that filtering is based on actual weights, rather than rounded weights
  # Using rounded weights could let potentially disclosive values through!
  weight_above_cutoff_col = weight_col >= min_frequency
  rounded_weight_col = round_to_nearest_interval(weight_col, interval=frequency_rounding).alias("WEIGHT")
  
  return [
    dict(
      grouping_fields=[
        *BASE_GROUPING_FIELDS,
        value_field,
      ],
      agg_cols=[rounded_weight_col],
      filters=[weight_above_cutoff_col],  # NOTE: if min_frequency = 1, any rows where the frequency is small enough to be rounded to 0 will be removed
    )
  ]


def get_categorical_aggregation_params(frequency_rounding: int, min_frequency: int) -> AggregationParametersType:
  """Get the parameters required to build the frequency distributions for categorical variables and apply
  disclosure controls to the resulting frequencies.

  Args:
      frequency_rounding (int): Rounding applied to frequencies
      min_frequency (int): Cutoff below which frequencies / values are excluded

  Returns:
      AggregationParametersType: Parameters to do the aggregations
  """
  return get_frequency_aggregation_params("VALUE_STRING", frequency_rounding, min_frequency)


def get_discrete_aggregation_params(frequency_rounding: int, min_frequency: int) -> AggregationParametersType:
  """Get the parameters required to build the frequency distributions for discrete variables and apply
  disclosure controls to the resulting frequencies.

  Args:
      frequency_rounding (int): Rounding applied to frequencies
      min_frequency (int): Cutoff below which frequencies / values are excluded

  Returns:
      AggregationParametersType: Parameters to do the aggregations
  """
  return get_frequency_aggregation_params("VALUE_NUMERIC", frequency_rounding, min_frequency)


def get_date_aggregation_params(frequency_rounding: int, min_frequency: int) -> AggregationParametersType:
  """Get the parameters required to build the frequency distributions for date variables and apply
  disclosure controls to the resulting frequencies.

  Args:
      frequency_rounding (int): Rounding applied to frequencies
      min_frequency (int): Cutoff below which frequencies / values are excluded

  Returns:
      AggregationParametersType: Parameters to do the aggregations
  """
  return get_frequency_aggregation_params("VALUE_DATE", frequency_rounding, min_frequency)


def get_continuous_aggregation_params():
  """Get the parameters required to calculate the distribution of percentiles for continuous variables
  for aggregation.

  Returns:
      AggregationParametersType: Parameters to do the aggregations
  """
  # TODO: remove nulls from percentiles
  # TODO: incidence (also need to put in generator function)
  return [
    dict(
      grouping_fields=BASE_GROUPING_FIELDS,
      agg_cols=[
        F.array([F.expr(f"approx_percentile(VALUE_NUMERIC, {i / 100})") for i in range(2, 100)]).alias("VALUE_NUMERIC_ARRAY"),
      ]
    )
  ]
  