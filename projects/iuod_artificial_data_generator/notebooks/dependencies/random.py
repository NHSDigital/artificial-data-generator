# Databricks notebook source
from __future__ import annotations
from typing import Union, Iterable
import itertools
import random

import numpy as np
from sklearn.neighbors import KernelDensity

from pyspark.sql import Column, functions as F, types as T

# COMMAND ----------

def weighted_choice_column(weighted_values: dict[Union[str, int], float], seed: int=None) -> Column:
  """Create a DataFrame column by sampling from weighted values
  
  Parameters
  ----------
  weighted_values: dict[Union[str, int], float]
    Values to sample mapped to probabilities/frequencies.
  seed: int (default=None)
      Random seed
  
  Returns
  -------
  pyspark.sql.Column
    Randomly chosen values

  Examples
  --------
  >>> # df is a pyspark DataFrame
  >>> # Add a column with a random choice between 1 & 2
  >>> df = df.withColumn('choice', weighted_choice_column({1: 0.5, 2: 0.5}))
  | ... | choice |
  | ... |   1    |
  | ... |   2    |
  | ... |  ...   |
  | ... |   2    |
  """
  values, weights = zip(*weighted_values.items())
  weights = list(map(float, weights))
    
  rv = sum(weights) * (F.lit(1.0) - F.rand(seed=seed))
  cum_weights = map(F.lit, itertools.accumulate(weights))
  
  # Choose the values based on the weights
  # Note: instead of using array, removing null values and picking the 0th element, you could use F.coalesce. However,
  # this was found to give skewed results with the resulting random values (why?), so the implementation here is a workaround
  choices_col = F.array(*itertools.starmap(lambda cum_weight, value: F.when(rv < cum_weight, F.lit(value)), zip(cum_weights, values)))
  choices_col = F.array_except(choices_col, F.array(F.lit(None)))[0]

  return choices_col.alias(f'weighted_choice({weighted_values}, seed={seed})')


def uniform_choice_column(values: list[Union[int, str, float]], seed: int=None) -> Column:
  """Create a Column by sampling from values with equal probabilities.
  
  Parameters
  ----------
  values: list[Union[str, int, float]]
    Values to sample
  seed: int
      Random seed
  
  Returns
  -------
  pyspark.sql.Column
    Randomly chosen values
    
  Examples
  --------
  >>> # df is a pyspark DataFrame
  >>> # Add a column with a random choice between 1 & 2
  >>> df = df.withColumn('choice', uniform_choice_column([1, 2]))
  | ... | choice |
  | ... |   1    |
  | ... |   2    |
  | ... |  ...   |
  | ... |   2    |
  """
  weighted_values = dict(zip(values, itertools.repeat(1)))
  return weighted_choice_column(weighted_values).alias(f'uniform_choice({values}, seed={seed})')


def random_integer_column(
  start: Union[int, str, Column], 
  stop: Union[int, str, Column], 
  seed: int=None
) -> Column:
  """Create a column of random integers within a given range.
  
  Parameters
  ----------
  start: Union[int, str, Column]
    Start of range (inclusive). If start is an int then it is used as a literal value column.
    If it is a string it is taken to refer to a column name. Else it is used directly
    as a column expression.
  stop: Union[int, str, Column]
    End of range (inclusive). Similar interpretation to start.
  seed: int (default=None)
    Seed for the random number generator
  
  Returns
  -------
  pyspark.sql.Column
    Random numbers in the given range
  """
  alias = f'random_integer({start}, {stop}, seed={seed})'
  
  if isinstance(start, int):
    start = F.lit(start)
  elif isinstance(start, str):
    start = F.col(start)

  if isinstance(stop, int):
    stop = F.lit(stop)
  elif isinstance(stop, str):
    stop = F.col(stop)
    
  # Extend limits to ensure boundaries are equally likely
  stop = stop + F.lit(0.999999)
    
  return F.floor(start + ((stop - start) * F.rand(seed=seed))).cast(T.IntegerType()).alias(alias)

# COMMAND ----------

class SparkRandom(object):
  """Provides interface to methods analogous to those provided by Python's 
  built-in random module but for operating on columns of a PySpark DataFrame.
  """
  def __init__(self, seed=None):
    self._seed = seed
    
  def seed(self, x):
    self._seed = x
    
  def choice(self, values: list[Union[str, int]]) -> Column:
    return uniform_choice_column(list(values), seed=self._seed)
  
  def choices(self, population: list[Union[str, int]], weights: list[Union[int, float]]) -> Column:
    return weighted_choice_column(dict(*zip(population, weights)), seed=self._seed)
    
  def randint(self, start: Union[int, str, Column], stop: Union[int, str, Column]) -> Column:
    return random_integer_column(start, stop, seed=self._seed)
  
  def shuffle(self, col: Union[str, Column]) -> Column:
    return F.shuffle(col)
  
  
spark_random = SparkRandom()