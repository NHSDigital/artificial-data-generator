# Databricks notebook source
# MAGIC %run ./sequential_field_lists

# COMMAND ----------

from typing import Iterable, List, Tuple
from pyspark.sql import Column, functions as F


def sort_fields_l2r(*fields: Iterable[str]) -> List[Tuple[str, Column]]:  # TODO: pull this out into common functions
  sorted_array = F.array_sort(F.array(*fields))
  sorted_value_fields = [(x, F.element_at(sorted_array, i+1)) for i, x in enumerate(fields)]
  return sorted_value_fields


def get_apc_sequential_fields() -> List[Tuple[str, Column]]:
  sorted_fields = sort_fields_l2r(*APC_SEQUENTIAL_DATE_FIELDS)
  return sorted_fields


def get_op_sequential_fields() -> List[Tuple[str, Column]]:
  sorted_fields = sort_fields_l2r(*OP_SEQUENTIAL_DATE_FIELDS)
  return sorted_fields


def get_ae_sequential_fields() -> List[Tuple[str, Column]]:
  sorted_fields = sort_fields_l2r(*AE_SEQUENTIAL_DATE_FIELDS)
  return sorted_fields