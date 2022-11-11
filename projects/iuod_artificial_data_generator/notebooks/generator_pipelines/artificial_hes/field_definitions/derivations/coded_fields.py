# Databricks notebook source
# MAGIC %run ./coded_field_lists

# COMMAND ----------

from typing import Iterable, Union, List, Tuple
from pyspark.sql import Column, functions as F


def _concat_coded_fields(*fields: Union[Iterable[str], Iterable[Column]]) -> Column:
  coalesced_fields = [F.coalesce(field, F.lit('')) for field in fields]
  concat_field_raw = F.concat_ws(",", *coalesced_fields)
  concat_field = F.regexp_replace(concat_field_raw, ',*$', '')
  return concat_field


def _count_coded_fields(*fields: Iterable[str]) -> Column:
  count_field_raw = F.expr(f"cardinality(filter(array({','.join(fields)}), x -> x is not null AND x not in ('&', '-')))")
  count_field = (
    F.when(count_field_raw > 24, 24)
    .when(count_field_raw > 0, count_field_raw)
    .otherwise(None)
  )
  return count_field


def _get_opertn_4_fields() -> List[str]:
  return opertn_4_fields  # Global constant from coded_field_lists


def get_opertn_34_fields() -> List[Tuple[str, Column]]:
  opertn_4_fields = _get_opertn_4_fields()
  opertn_3_fields = [(f"OPERTN_3_{i+1:02}", F.substring(x, 0, 3)) for i, x in enumerate(opertn_4_fields)]
  opertn_3_concat = ("OPERTN_3_CONCAT", _concat_coded_fields(*next(zip(*opertn_3_fields))))  # Extract just the column defintions
  opertn_4_concat = ("OPERTN_4_CONCAT", _concat_coded_fields(*opertn_4_fields))
  opertn_count = ("OPERTN_COUNT", _count_coded_fields(*opertn_4_fields))
  return [
    *opertn_3_fields,
    opertn_3_concat,
    opertn_4_concat,
    opertn_count,
  ]


def _get_diag_4_fields(limit: int=20) -> List[str]:
  # NOTE: OP and APC have a different number of DIAG fields
  return diag_4_fields[:limit]  # Global constant from coded_field_lists


def get_diag_34_fields(limit: int=20) -> List[Tuple[str, Column]]:
  diag_4_fields = _get_diag_4_fields(limit=limit)
  diag_3_fields = [(f"DIAG_3_{i+1:02}", F.substring(x, 0, 3)) for i, x in enumerate(diag_4_fields)]
  diag_3_concat = ("DIAG_3_CONCAT", _concat_coded_fields(*next(zip(*diag_3_fields))))  # TODO: replace null 0th element with "R69" before concat
  diag_4_concat = ("DIAG_4_CONCAT", _concat_coded_fields(*diag_4_fields))  # TODO: replace null 0th element with "R69X" before concat
  diag_count = ("DIAG_COUNT", _count_coded_fields(*diag_4_fields))
  return [
    *diag_3_fields,
    diag_3_concat,
    diag_4_concat,
    diag_count,
  ]

# COMMAND ----------

