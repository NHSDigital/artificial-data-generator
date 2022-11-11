# Databricks notebook source
from typing import Iterable, Union
from pyspark.sql import Column, functions as F, Window as W


def cumsum_over_window(
  sum_col: Column, 
  partition_cols: Union[Iterable[str], Iterable[Column]], 
  order_cols: Union[Iterable[str], Iterable[Column]],
) -> Column:
  """Calculate the cumulative sum of a column over a window.
  
  Args:
      sum_col (pyspark.sql.Column): Column to sum over the partitions
      partition_cols (Union[Iterable[str], Iterable[pyspark.sql.Column]]): Partitions to sum within
      order_cols (Union[Iterable[str], Iterable[pyspark.sql.Column]]): Ordering for the cumulative sum
    
  Returns:
      pyspark.sql.Column: Column of within-partition cumulative sums
  """
  w = (
    W.partitionBy(*partition_cols)
    .orderBy(*order_cols)  # Ordering by value will be slow!
    .rangeBetween(W.unboundedPreceding, 0)
  )
  return F.sum(sum_col).over(w)


def total_over_window(sum_col: Column, partition_cols: Union[Iterable[str], Iterable[Column]]) -> Column:
  """Calculate the total sum of a column over a window.
  
  Parameters:
      sum_col (pyspark.sql.Column): Column to sum over the partitions
      partition_cols (Union[Iterable[str], Iterable[pyspark.sql.Column]]): Partitions to sum within
    
  Returns: 
      pyspark.sql.Column: Column of within-partition sums
    
  """
  w = (
    W.partitionBy(*partition_cols)
    .rangeBetween(W.unboundedPreceding, W.unboundedFollowing)
  )
  return F.sum(sum_col).over(w)


def get_cdf_bin(
  frequency_field: str="WEIGHT", 
  cumsum_field: str="CUMSUM", 
  total_field: str="TOTAL"
) -> Column:
  lower_bound_col = (F.col(cumsum_field) - F.col(frequency_field)) / F.col(total_field)
  upper_bound_col = F.col(cumsum_field) / F.col(total_field)
  cdf_bin_col = F.struct(
    lower_bound_col.alias("LOWER"), 
    upper_bound_col.alias("UPPER")
  )
  return cdf_bin_col