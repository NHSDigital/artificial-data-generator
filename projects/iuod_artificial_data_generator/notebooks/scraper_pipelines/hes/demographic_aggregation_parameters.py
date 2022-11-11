# Databricks notebook source
# MAGIC %run ../../scraper_stages/aggregation/aggregation_parameters

# COMMAND ----------

from pyspark.sql import functions as F

# region Prevent downstream linting highlights
AggregationParametersType = AggregationParametersType
get_frequency_aggregation_params = get_frequency_aggregation_params
BASE_GROUPING_FIELDS = BASE_GROUPING_FIELDS
weight_col = weight_col
# endregion


def get_demographic_categorical_aggregation_params(
  patient_index_field: str, 
  frequency_rounding: int, 
  min_frequency: int
) -> AggregationParametersType:
  """Get parameters defining the aggregations required to summarise 'DEMOGRAPHIC_CATEGORICAL'
  fields in long-formatted data. The result is a frequency distribution over weighted vectors,
  where the inner weights describe the proportion of events that a patient has recorded a given value
  and the outer weights indicate the number of patients with that pattern of inner weights across
  the dataset.
  
  Note: this logic is hes-specific.

  Args:
      patient_index_field (str): Field that indexes patients
      frequency_rounding (int): Rounding to apply to frequencies
      min_frequency (int): Cutoff below which frequencies are removed

  Returns:
      AggregationParametersType: Parameters defining the aggregations
  """
  # Aggregation 1: count total rows and distinct values per distinct value per patient
  params1 = dict(
    grouping_fields=[
      *BASE_GROUPING_FIELDS, 
      patient_index_field,
    ],
    agg_cols=[
      F.collect_list(F.coalesce("VALUE_STRING", F.lit("null"))).alias("VALUE_STRING_ARRAY"),  # Need nulls to be strings for aggregation
      weight_col,
    ],
    derivations=[
      F.array_sort(F.array_distinct("VALUE_STRING_ARRAY")).alias("DISTINCT_VALUE_STRING_ARRAY"),
      F.expr("transform(DISTINCT_VALUE_STRING_ARRAY, x -> round(size(filter(VALUE_STRING_ARRAY, y -> y == x)) / WEIGHT, 1))").alias("WEIGHT_ARRAY"),  # Normalised weight per value
      F.expr("transform(DISTINCT_VALUE_STRING_ARRAY, x -> CASE WHEN x != 'null' THEN x END)").alias("VALUE_STRING_ARRAY"),  # Replace nulls within the array of values
    ]
  )

  # Aggregation 2: count patients per distinct combinations of values and weights
  params2, = get_frequency_aggregation_params(
    "VALUE_STRING_ARRAY", 
    frequency_rounding=frequency_rounding, 
    min_frequency=min_frequency
  )
  params2["grouping_fields"].append("WEIGHT_ARRAY")
  
  return [
    params1,
    params2,
  ]


def get_demographic_date_aggregation_params(
  patient_index_field: str, 
  frequency_rounding: int, 
  min_frequency: int
) -> AggregationParametersType:
  """Get parameters defining the aggregations required to summarise 'DEMOGRAPHIC_DATE'
  fields in long-formatted data. The result is a frequency distribution over dates
  with an associated incidence, which indicates the likelihood that a patient is measured with
  a value different from a given value. E.g. a patient's true DOB is 01/02/2022 but it
  could be recorded as 02/01/2022 or 01/02/2021.
  
  Note: this logic is hes-specific.

  Args:
      patient_index_field (str): Field that indexes patients
      frequency_rounding (int): Rounding to apply to frequencies
      min_frequency (int): Cutoff below which frequencies are removed

  Returns:
      AggregationParametersType: Parameters defining the aggregations
  """
  # Aggregation 1: frequency of each value per patient
  params1 = dict(
    grouping_fields=[
      *BASE_GROUPING_FIELDS, 
      patient_index_field,
      "VALUE_STRING",  # Demographic dates (e.g. MYDOB) are strings with date-like information
    ],
    agg_cols=[weight_col],
  )

  # Aggregation 2: collect values and weights per per patient
  # The 'VALUE_NUMERIC' column for demographic dates contains the incidence which is 
  # defined as the mean proportion of patients that have the given 'VALUE_STRING' as the 
  # mode value and where the specific record is associated with a that mode value 
  # (i.e. the value is recorded 'correct')
  params2 = dict(
    grouping_fields = [
      *BASE_GROUPING_FIELDS, 
      patient_index_field,
    ],
    agg_cols = [
      F.collect_list("VALUE_STRING").alias("VALUE_STRING_ARRAY"),  # All values
      F.collect_list("WEIGHT").alias("WEIGHT_ARRAY"),  # All weights
      F.sum("WEIGHT").alias("TOTAL_WEIGHT"),  # Total weight for the patient
      F.max("WEIGHT").alias("MAX_WEIGHT"),  # Largest weight for a value
    ],
    derivations = [
      F.expr("int(array_position(WEIGHT_ARRAY, MAX_WEIGHT))").alias("MAX_WEIGHT_INDEX"),
      F.expr("element_at(VALUE_STRING_ARRAY, MAX_WEIGHT_INDEX)").alias("VALUE_STRING"),
      (F.col("MAX_WEIGHT") / F.col("TOTAL_WEIGHT")).alias("VALUE_NUMERIC"),  # Proportion of records for a patient equal to the mode value
    ],
  )

  # Aggregation 3: count patients with each mode value and compute the average proportion of episodes where the value differs from the mode 
  params3, = get_frequency_aggregation_params(
    "VALUE_STRING", 
    frequency_rounding=frequency_rounding, 
    min_frequency=min_frequency
  )
  params3["agg_cols"].append(F.mean("VALUE_NUMERIC").alias("VALUE_NUMERIC"))  # Average deviation from the mode value
  
  return [
    params1,
    params2,
    params3,
  ]
  