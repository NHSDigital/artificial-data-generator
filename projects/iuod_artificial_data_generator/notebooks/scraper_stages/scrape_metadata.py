# Databricks notebook source
# MAGIC %run ./ingestion

# COMMAND ----------

# MAGIC %run ./wide_to_long

# COMMAND ----------

# MAGIC %run ./schemas/meta_schema

# COMMAND ----------

# MAGIC %run ./aggregation/field_summarisers

# COMMAND ----------

# MAGIC %run ./aggregation/relationship_summariser

# COMMAND ----------

# MAGIC %run ../dataset_definitions/relational_helpers

# COMMAND ----------

from typing import Optional, Iterable, Dict, Callable
from pyspark.sql import DataFrame, functions as F

# region Prevent downstream linting highlights
spark = spark
Table = Table
wide_to_long = wide_to_long
field_summariser = field_summariser
get_primary_key = get_primary_key
get_foreign_keys = get_foreign_keys
summarise_relationships = summarise_relationships
get_meta_schema = get_meta_schema
ingest = ingest
# endregion


def scrape_fields(
  wide_df: DataFrame, 
  type_overrides: Iterable[Dict[str, str]], 
  frequency_rounding: int, 
  min_frequency: int,
  dataset_summarisers: Optional[Iterable[Callable]] = None,
  unpivoted_fields: Optional[Iterable[str]] = None
)-> DataFrame:
  """Scrape the field metadata (i.e. frequency / percentile distributions) from wide-formatted data

  Args:
      wide_df (DataFrame): Wide-formatted data to scrape
      type_overrides (Iterable[Dict[str, str]]): Overrides for metadata types, used in place of automatic types
      frequency_rounding (int): Rounding applied to frequency-typed fields
      min_frequency (int): Cutoff below which frequencies / values are removed
      dataset_summarisers (Optional[Iterable[Callable]], optional): Dataset-specific summariser functions. Passed through
        to summarise_fields. Defaults to None.
      unpivoted_fields (Optional[Iterable[str]], optional): Fields to preserve in the original wide_df. Defaults to None.

  Returns:
      DataFrame: Field metadata
  """
  
  if unpivoted_fields is None:
    unpivoted_fields = []
    
  unpivoted_fields = list(set(["TABLE_NAME", *unpivoted_fields]))
  
  # Pivot to long format
  long_df = wide_to_long(wide_df, preserved_field_names=unpivoted_fields)

  # Associate metadata types (e.g. 'categorical')
  # TODO: what happens if fields don't appear in type overrides?
  type_overrides_df = spark.createDataFrame(type_overrides, "FIELD_NAME: string, VALUE_TYPE: string")
  long_df = (
    long_df
    .join(type_overrides_df.hint("broadcast"), on="FIELD_NAME", how="left")
    .withColumn("VALUE_TYPE", F.coalesce(F.col("VALUE_TYPE"), F.lit("CATEGORICAL")))  # Fill nulls
  )

  # Summarise values
  meta_df = field_summariser(
    long_df, 
    frequency_rounding=frequency_rounding, 
    min_frequency=min_frequency, 
    dataset_summarisers=dataset_summarisers,
  )

  return meta_df

  
def scrape_relationships(wide_df: DataFrame, table: Table) -> DataFrame:
  """Scrape the relationship metadata (i.e. frequency distributions of cardinal relationshios) 
  from wide-formatted data

  Args:
      wide_df (DataFrame): Wide-formatted data to scrape
      table (Table): Holds the names of primary and foreign key fields

  Returns:
      DataFrame: Relationship metadata
  """
  primary_key_field = get_primary_key(table)
  
  for foreign_key_field in get_foreign_keys(table):
    index_fields = [foreign_key_field.name, primary_key_field.name]
    index_df = wide_df.select("TABLE_NAME", *index_fields)
    meta_df = summarise_relationships(index_df, primary_key_field.name, foreign_key_field.name)
    return meta_df
  
  else:
    empty_meta_df = spark.createDataFrame([], get_meta_schema())
    return empty_meta_df


def scrape_metadata(
  database_name: str,
  source_table: Table, 
  type_overrides: Iterable[Dict[str, str]], 
  frequency_rounding: int, 
  min_frequency: int,
  excluded_fields: Iterable[str],
  included_fields: Optional[Iterable[str]] = None,
  dataset_summarisers: Optional[Iterable[Callable]] = None,
) -> DataFrame:
  """Scrape field and relationship metadata from a table

  Args:
      database_name (str): Database containing the table to scrape
      source_table (Table): Table to scrape
      type_overrides (Iterable[Dict[str, str]]): Overrides for metadata types, used in place of automatic types
      frequency_rounding (int): Rounding applied to frequency-typed fields
      min_frequency (int): Cutoff below which frequencies / values are removed
      dataset_summarisers (Optional[Iterable[Callable]], optional): Dataset-specific summariser functions. Passed through
        to summarise_fields. Defaults to None.

  Returns:
      DataFrame: Scraped metadata
  """
  dataset_df = ingest(database_name, source_table.qualifier)
  
  field_meta_df = scrape_fields(
    dataset_df,
    type_overrides, 
    frequency_rounding=frequency_rounding, 
    min_frequency=min_frequency,
    dataset_summarisers=dataset_summarisers
  )
  
  relational_meta_df = scrape_relationships(dataset_df, source_table)
  
  meta_df = field_meta_df.unionByName(relational_meta_df)
  
  return meta_df