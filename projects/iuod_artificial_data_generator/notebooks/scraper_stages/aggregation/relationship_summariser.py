# Databricks notebook source
# MAGIC %run ../wide_to_long

# COMMAND ----------

# MAGIC %run ./summariser_factory

# COMMAND ----------

from pyspark.sql import DataFrame, functions as F

# region Prevent downstream linting highlights
BASE_GROUPING_FIELDS = BASE_GROUPING_FIELDS
AggregationParametersType = AggregationParametersType
weight_col = weight_col
create_summariser = create_summariser
# endregion


def get_relationship_aggregation_params(pk_field: str, fk_field: str) -> AggregationParametersType:
  """Get the parameters required to build the frequency distributions of cardinal relationships
  between a primary key and foreign key

  Args:
      pk_field (str): Name of the primary key field
      fk_field (str): Name of the foreign key field

  Returns:
      AggregationParametersType: Parameters to do the aggregations
  """
  # Count the number of 1-to-N mappings (where N >= 0) between foreign and primary keys
  # Null primary keys contribute towards 1-to-0 counts
  # Null foreign keys are ignored
  cardinality_col = F.approx_count_distinct(pk_field)
  return [
    dict(
      grouping_fields=[*BASE_GROUPING_FIELDS, fk_field],
      agg_cols=[cardinality_col.alias("VALUE_NUMERIC")],
      filters=[~F.isnull(fk_field)]  # TODO: Handle nulls separately
    ),
    dict(
      grouping_fields=[*BASE_GROUPING_FIELDS, "VALUE_NUMERIC"],
      agg_cols=[weight_col],
    ),
  ]


def summarise_relationships(wide_df: DataFrame, pk_field: str, fk_field: str) -> DataFrame:
  """Summarise the frequency distributions of cardinal relationships between primary key
  and foreign key fields in wide-formatted data.

  Args:
      wide_df (DataFrame): DataFrame holding the relationships to summarise
      pk_field (str): Name of the primary key field
      fk_field (str): Name of the foreign key field

  Returns:
      DataFrame: Summarised relationship data
  """
  wide_df = (
    wide_df
    .withColumn("FIELD_NAME", F.lit(pk_field))
    .withColumn("VALUE_TYPE", F.lit("RELATIONSHIP"))
    .select(*BASE_GROUPING_FIELDS, pk_field, fk_field)
  )
  
  aggregation_params = get_relationship_aggregation_params(pk_field, fk_field)
  summariser = create_summariser("RELATIONSHIP", aggregation_params)
  meta_df = (
    summariser(wide_df)
    .withColumn("VALUE_STRING", F.lit(fk_field))
  )
  
  return meta_df