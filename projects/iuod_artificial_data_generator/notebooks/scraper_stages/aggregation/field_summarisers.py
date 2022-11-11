# Databricks notebook source
# MAGIC %run ./summariser_factory

# COMMAND ----------

# MAGIC %run ./aggregation_parameters

# COMMAND ----------

from functools import reduce
from typing import Optional, Iterable, Callable

from pyspark.sql import functions as F, DataFrame


# region Prevent downstream linting highlights
create_summariser = create_summariser
get_categorical_aggregation_params = get_categorical_aggregation_params
get_date_aggregation_params = get_date_aggregation_params
get_discrete_aggregation_params = get_discrete_aggregation_params
get_continuous_aggregation_params = get_continuous_aggregation_params
# endregion


def field_summariser(
  long_df: DataFrame, 
  frequency_rounding: int, 
  min_frequency: int, 
  dataset_summarisers: Optional[Iterable[Callable]]=None
) -> DataFrame:
  """Summarise fields in a long-formatted dataframe with value type 'CATEGORICAL', 
  'DATE', 'DISCRETE' or 'CONTINUOUS'.

  Args:
      long_df (pyspark.sql.DataFrame): Long-formatted data to summarise
      frequency_rounding (int): Rounding applied to frequencies
      min_frequency (int): Cutoff below which frequencies are removed.
      dataset_summarisers (Optional[Iterable[Callable]], optional): Additional summariser
        functions for dataset-specific operations. Defaults to None.

  Returns:
      pyspark.sql.DataFrame: Summarised data (long-formatted)
  """
  disclosure_control_params = (frequency_rounding, min_frequency)
  
  if dataset_summarisers is None:
    dataset_summarisers = []
  
  summarisers = [
    create_summariser("CATEGORICAL", get_categorical_aggregation_params(*disclosure_control_params)),
    create_summariser("DATE",        get_date_aggregation_params(*disclosure_control_params)),
    create_summariser("DISCRETE",    get_discrete_aggregation_params(*disclosure_control_params)),
    create_summariser("CONTINUOUS",  get_continuous_aggregation_params()),
    *dataset_summarisers,
  ]
  
  meta_dfs = map(lambda summariser: summariser(long_df), summarisers)
  meta_df = reduce(DataFrame.union, meta_dfs)
  
  return meta_df