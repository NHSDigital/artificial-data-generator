# Databricks notebook source
# MAGIC %run ../../../dependencies/spark_rstr

# COMMAND ----------

# MAGIC %run ../../../dataset_definitions/relational_helpers

# COMMAND ----------

# MAGIC %run ./id_field_regex_patterns

# COMMAND ----------

# MAGIC %run ./id_field_lists

# COMMAND ----------

from typing import Iterable, Dict, List

from pyspark.sql import DataFrame, Column, functions as F, Window as W

# region Prevent downstream linter highlights
get_parent_table = get_parent_table
SparkRstr = SparkRstr
ID_FIELD_PATTERNS = ID_FIELD_PATTERNS
APC_ID_FIELDS = APC_ID_FIELDS
AE_ID_FIELDS = AE_ID_FIELDS
OP_ID_FIELDS = OP_ID_FIELDS
Table = Table
# endregion


def _get_dataset_id_fields(field_names: Iterable[str]) -> Dict[str, Column]:
  rstr = SparkRstr()
  dataset_id_fields = set(field_names).intersection(ID_FIELD_PATTERNS.keys())
  return {field: rstr.xeger(ID_FIELD_PATTERNS[field]).alias(field) for field in dataset_id_fields}


def _get_apc_id_fields() -> Column:
  return _get_dataset_id_fields(APC_ID_FIELDS)


def _get_ae_id_fields() -> Column:
  return _get_dataset_id_fields(AE_ID_FIELDS)


def _get_op_id_fields() -> Column:
  return _get_dataset_id_fields(OP_ID_FIELDS)


def get_id_fields(dataset: str) -> List[Column]:
  id_fields_functions = {
    "hes_ae": _get_ae_id_fields,
    "hes_apc": _get_apc_id_fields,
    "hes_op": _get_op_id_fields,
  }
  _get_id_fields = id_fields_functions.get(dataset, lambda: [])
  id_fields = _get_id_fields()
  return id_fields


def with_hes_id_fields(df: DataFrame, dataset: str, table: Table, index_fields: Iterable[str]) -> DataFrame:
  demographic_table = get_parent_table(table)
  demographic_field_names = [f.name for f in demographic_table.fields]

  # Split demographic and episode id fields so that demographic ids can be carried over
  id_fields = get_id_fields(dataset)
  demographic_id_fields = [id_fields.pop(field_name) for field_name in demographic_field_names if field_name in id_fields]
  episode_id_fields = [field_value_col for _, field_value_col in id_fields.items()]

  demographic_id_window = W.partitionBy(index_fields[0]).orderBy(index_fields[1])

  df_with_id_fields = (
    df
    .withColumn("DEMOGRAPHIC_IDS", F.first(F.struct(*demographic_id_fields)).over(demographic_id_window))
    .select(*df.columns, "DEMOGRAPHIC_IDS.*", *episode_id_fields)
    .drop(*index_fields)
  )
  
  return df_with_id_fields