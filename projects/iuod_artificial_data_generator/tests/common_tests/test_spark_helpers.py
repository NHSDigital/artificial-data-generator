# Databricks notebook source
# MAGIC %run ../../notebooks/common/spark_helpers

# COMMAND ----------

# MAGIC %run ../test_helpers

# COMMAND ----------

from pyspark.sql import DataFrame, functions as F, Column, types as T, Window as W
from typing import Union, Callable

# COMMAND ----------

test_columns_equal = FunctionTestSuite()


def create_test_columns_equal_df():
  df = spark.createDataFrame(
          [
            ("a", 10, "a", 10, 10., 10.), 
            ("b", 20, "b", 20, 20., 20.), 
            ("c", 30, "c", 31, 31., 31.), 
            ("d", 40, "d", 12, 12., 12.),
            ("e", 45, "e", 40, 40., 40.), 
            ("f", 5 , "f", 30, 30., 30.), 
            ("g", 25, "g", 20, 20., 20.),
            ("h", 15, "h", 40, 40., 40.)
          ], 
          ["COL1", "COL2", "COL3", "COL4", "COL5", "COL6"]
      )
  return df


@test_columns_equal.add_test
def compares_columns_as_expected():
  
  df = create_test_columns_equal_df()
  
  assert not columns_equal(df, "COL2", "COL4"), "COL2 and COL4 are equal!"
  assert columns_equal(df, "COL1", "COL3"), "COL1 and COL3 are not equal!"
  assert columns_equal(df, "COL5", "COL6"), "COL5 and COL6 are not equal!"
  
  
@test_columns_equal.add_test
def handles_unmatched_dtypes():
  
  df = create_test_columns_equal_df()
  
  assert not columns_equal(df, "COL1", "COL2"), "COL1 and COL2 are equal!"
  assert not columns_equal(df, "COL4", "COL5"), "COL4 and COL5 are equal!"


test_columns_equal.run()

# COMMAND ----------

test_columns_approx_equal = FunctionTestSuite()


def create_test_columns_approx_equal_df():
  df = (
      spark.createDataFrame(
        [
          (1.0, 1.10, 1.10, "a", 1, 1.10),
          (1.0, 1.11, 1.11, "b", 1, 1.11),
          (1.0, 1.12, 1.12, "c", 1, 1.12),
          (1.0, 1.13, 1.13, "d", 1, 1.13),
          (1.0, 1.14, 1.14, "e", 1, 1.14),
          (1.0, 1.15, 1.2 , "d", 1, 1.2),
        ], 
        ["COL1", "COL2", "COL3", "COL4", "COL5", "COL6"]
      )
      .withColumn("COL1d", F.col("COL1").cast("decimal(30,20)"))
      .withColumn("COL2d", F.col("COL2").cast("decimal(30,20)"))
      .withColumn("COL3d", F.col("COL3").cast("decimal(30,20)"))
      .withColumn("COL6d", F.col("COL6").cast("decimal(30,20)")) # Create DecimalType columns
      .withColumn("COL1do", F.col("COL1").cast(T.DoubleType()))
      .withColumn("COL2do", F.col("COL2").cast(T.DoubleType()))
      .withColumn("COL3do", F.col("COL3").cast(T.DoubleType()))
      .withColumn("COL6do", F.col("COL6").cast(T.DoubleType())) # Create DoubleType columns
    )
  
  return df


@test_columns_approx_equal.add_test
def compares_float_columns_as_expected():
  
  df = create_test_columns_approx_equal_df()
  
  assert columns_approx_equal(df, "COL1", "COL2", 0.15), "COL1 and COL2 are not approximately equal!"
  assert columns_approx_equal(df, "COL3", "COL6", 0.0), "COL3 and COL6 are not approximately equal!"
  assert not columns_approx_equal(df, "COL1", "COL3", 0.15), "COL1 and COL3 are approximately equal!"
  
  
@test_columns_approx_equal.add_test
def compares_decimal_columns_as_expected():
  
  df = create_test_columns_approx_equal_df()
  
  assert columns_approx_equal(df, "COL1d", "COL2d", 0.15), "COL1d and COL2d are not approximately equal!"
  assert not columns_approx_equal(df, "COL1d", "COL2d", 0.1), "COL1d and COL2d are approximately equal!"
  assert columns_approx_equal(df, "COL3d", "COL6d", 0.0), "COL3d and COL6d are not approximately equal!"

  
@test_columns_approx_equal.add_test
def compares_decimal_columns_as_expected():
  
  df = create_test_columns_approx_equal_df()
  
  assert columns_approx_equal(df, "COL1do", "COL2do", 0.15), "COL1do and COL2do are not approximately equal!"
  assert not columns_approx_equal(df, "COL1do", "COL2do", 0.1), "COL1do and COL2do are approximately equal!"
  assert columns_approx_equal(df, "COL3do", "COL6do", 0.0), "COL3do and COL6do are not approximately equal!"
  

@test_columns_approx_equal.add_test
def handles_unmatched_dtypes():
  
  df = create_test_columns_approx_equal_df()
  
  assert not columns_approx_equal(df, "COL1", "COL4", 0.15), "COL1 and COL4 are approximately equal!"
  assert not columns_approx_equal(df, "COL1", "COL5", 0.15), "COL1 and COL5 are approximately equal!"
 

test_columns_approx_equal.run()

# COMMAND ----------

test_dataframes_approx_equal = FunctionTestSuite()


def create_test_dataframes_approx_equal_df():
  rows1 = [
          (1.0, 1.10, 1.10, "a", 1, 1.10),
          (1.0, 1.11, 1.11, "b", 1, 1.11),
          (1.0, 1.12, 1.12, "c", 1, 1.12),
          (1.0, 1.13, 1.13, "d", 1, 1.13),
          (1.0, 1.14, 1.14, "e", 1, 1.14),
          (1.0, 1.15, 1.2 , "d", 1, 1.2 ),
        ]

  rows2 = [
          (1.0, 1.34, 1.10, "a", 1, 1.10),
          (1.0, 1.12, 1.11, "b", 1, 1.11),
          (1.0, 1.15, 1.12, "c", 1, 1.12),
          (1.0, 1.13, 1.13, "d", 1, 1.13),
          (1.0, 1.17, 1.14, "e", 1, 1.14),
          (1.0, 1.01, 1.2 , "d", 1, 1.2 ),
        ]

  columns = ["COL1", "COL2", "COL3", "COL4", "COL5", "COL6"]

  new_row = spark.createDataFrame([(1.0, 1.15, 1.2 , "f", 1, 1.2)], columns)

  df1 = spark.createDataFrame(rows1, columns)
  df2 = df1.union(new_row)  # df1 with an extra row
  df3 = (spark.createDataFrame(rows1, columns)  # df1 with an extra column
         .withColumn("COL7", F.lit(2)))
  df4 = spark.createDataFrame(rows2, columns)  # df1 with COL2 modified
  
  return df1, df2, df3, df4


@test_dataframes_approx_equal.add_test
def compares_dfs_with_unequal_schemas_as_expected():
  
  df1, df2, df3, df4 = create_test_dataframes_approx_equal_df()

  assert not dataframes_approx_equal(df1, df3, 0.0), "df1 and df3 are approximately equal!"
  
  
@test_dataframes_approx_equal.add_test
def compares_dfs_with_unequal_row_nos_as_expected():
  
  df1, df2, df3, df4 = create_test_dataframes_approx_equal_df()

  assert not dataframes_approx_equal(df1, df2, 1.0), "df1 and df2 are approximately equal!"
  
  
@test_dataframes_approx_equal.add_test
def compares_dfs_with_floats_as_expected():
  
  df1, df2, df3, df4 = create_test_dataframes_approx_equal_df()
  
  assert dataframes_approx_equal(df1, df4, 0.22), "df1 and df4 are not approximately equal!"
  assert dataframes_approx_equal(df1, df1, 0.0),  "df1 and df1 are not approximately equal!"
  assert not dataframes_approx_equal(df1, df4, 0.21), "df1 and df4 are approximately equal!"
  
  
@test_dataframes_approx_equal.add_test
def compares_dfs_with_decimals_as_expected():
  
  df1, df2, df3, df4 = create_test_dataframes_approx_equal_df()

  df1 = (df1
    .withColumn("COL2d", F.col("COL2").cast("decimal(30,20)"))
        )
  df4 = (df4
    .withColumn("COL2d", F.col("COL2").cast("decimal(30,20)"))
        )
    
  assert dataframes_approx_equal(df1, df4, 0.25), "df1 and df4 are not approximately equal!"

  
@test_dataframes_approx_equal.add_test
def compares_dfs_with_decimals_as_expected():
  
  df1, df2, df3, df4 = create_test_dataframes_approx_equal_df()

  df1 = (df1
    .withColumn("COL2do", F.col("COL2").cast(T.DoubleType()))
        )
  df4 = (df4
    .withColumn("COL2do", F.col("COL2").cast(T.DoubleType()))
        )
    
  assert dataframes_approx_equal(df1, df4, 0.25), "df1 and df4 are not approximately equal!"
  
  
test_dataframes_approx_equal.run()