# Databricks notebook source
# MAGIC %run ../../dataset_definitions/hes/hes_patient_table

# COMMAND ----------

# MAGIC %run ../../scraper_stages/aggregation/summariser_factory

# COMMAND ----------

# MAGIC %run ./demographic_aggregation_parameters

# COMMAND ----------

# MAGIC %run ./constants/disclosure_control_parameters

# COMMAND ----------

from typing import List, Callable
from pyspark.sql import DataFrame

# region Prevent downstream linting highlights
HES_FREQUENCY_ROUNDING = HES_FREQUENCY_ROUNDING
HES_MIN_FREQUENCY = HES_MIN_FREQUENCY
HES_PATIENT_KEY = HES_PATIENT_KEY
get_demographic_categorical_aggregation_params = get_demographic_categorical_aggregation_params
get_demographic_date_aggregation_params = get_demographic_date_aggregation_params
create_summariser = create_summariser
# endregion


def demographic_categorical_summariser(long_df: DataFrame) -> DataFrame:
  """Summarise fields in a long-formatted dataframe which have VALUE_TYPE='DEMOGRAPHIC_CATEGORICAL'
  These are fields such as sex or ethnicity, which in principle have a single underlying value for a given
  patient, but which in practice can have multiple values recorded across different visits. We want to 
  sample in such a way that the patterns of 'errors' that we would see in real data are represented in 
  artificial data.

  Note: this logic is hes-specific.
  
  Args:
      long_df (DataFrame): Long-formatted data to summarise

  Returns:
      DataFrame: Summarised data (long-formatted)
  """
  aggregation_params = get_demographic_categorical_aggregation_params(
    HES_PATIENT_KEY, 
    frequency_rounding=HES_FREQUENCY_ROUNDING, 
    min_frequency=HES_MIN_FREQUENCY
  )
  summariser = create_summariser("DEMOGRAPHIC_CATEGORICAL", aggregation_params)
  return summariser(long_df)


def demographic_date_summariser(long_df: DataFrame) -> DataFrame:
  """Summarise fields in a long-formatted dataframe which have VALUE_TYPE='DEMOGRAPHIC_DATE'
  These are fields such as date of birth, which in principle have a single underlying value for a given
  patient, but which in practice can have multiple values recorded across different visits. We want to 
  sample in such a way that the patterns of 'errors' that we would see in real data are represented in 
  artificial data.

  Note: this logic is hes-specific.

  Args:
      long_df (DataFrame): Long-formatted data to summarise

  Returns:
      DataFrame: Summarised data (long-formatted)
  """
  aggregation_params = get_demographic_date_aggregation_params(
    HES_PATIENT_KEY, 
    frequency_rounding=HES_FREQUENCY_ROUNDING, 
    min_frequency=HES_MIN_FREQUENCY
  )
  summariser = create_summariser("DEMOGRAPHIC_DATE", aggregation_params)
  return summariser(long_df)


def get_demographic_summarisers() -> List[Callable[[DataFrame], DataFrame]]:
  return [
    demographic_categorical_summariser,
    demographic_date_summariser,
  ]

# COMMAND ----------

