# Databricks notebook source
# Source: yanai repo (DPS) - path: src/dsp/common/spark_helpers.py


from pyspark.sql import DataFrame, functions as F, Column, types as T, Window as W
from typing import Union, Callable


def dataframe_empty(dataframe: DataFrame) -> bool:
    """
    Check whether a dataframe contains any rows

    Args:
        dataframe: The dataframe to check for rows

    Returns:
        Whether the given dataframe is empty
    """
    return dataframe.select(F.lit(1)).head() is None


def datatypes_equal(first: T.DataType, second: T.DataType, check_nullability: bool = True):
    """
    Determine whether a pair of Spark data types are equivalent

    Args:
        first: The first data type to compare
        second: The second data type to compare
        check_nullability: Whether to consider the nullability of struct fields when determining whether they are
            equivalent

    Returns:
        Whether the two data types given are equivalent
    """
    if isinstance(first, T.StructType) and isinstance(second, T.StructType):
        if check_nullability:
            return first == second

        if len(first.fields) != len(second.fields):
            return False

        for first_field, second_field in zip(first.fields, second.fields):
            if first_field.name != second_field.name \
                    or not datatypes_equal(first_field.dataType, second_field.dataType, check_nullability=check_nullability):
                return False

        return True

    if type(first) != type(second):  # pylint: disable=unidiomatic-typecheck
        return False

    if isinstance(first, T.ArrayType):
        return datatypes_equal(first.elementType, second.elementType, check_nullability)

    return first == second


def dataframes_equal(first: DataFrame, second: DataFrame, check_nullability: bool = True) -> bool:
    """
    Determine whether two dataframes have the same schema and contain the same record. At present, this method
    disregards the order that the records occur, only considering whether equivalent records are present.

    Args:
        first: The first data frame to compare
        second: The second data frame to compare
        check_nullability: Whether to consider the nullability of struct fields  of the schema of the dataframes
            when determining whether they are equivalent

    Returns:
        Whether the dataframes are equivalent
    """
    if not datatypes_equal(first.schema, second.schema, check_nullability=check_nullability):
        return False

    second_in_order = second.select(*first.columns)
    diff1 = first.subtract(second_in_order)
    diff2 = second_in_order.subtract(first)
    if dataframe_empty(diff1.union(diff2)):
        return True

    diff1.cache()
    diff2.cache()
    try:
        print("Records in first not in second:")
        diff1.show(truncate=False)
        print("Records in second not in first:")
        diff2.show(truncate=False)
    finally:
        diff1.unpersist()
        diff2.unpersist()

    return False


def dataframe_is_subset(first: DataFrame, second: DataFrame, check_nullability: bool = True) -> bool:
    """
    Determine whether two dataframes have the same schema the first dataframes records are a subset of the second dataframes records.
    At present, this method disregards the order that the records occur, only considering whether equivalent records are present.

    Args:
        first: The first data frame to compare
        second: The second data frame to compare
        check_nullability: Whether to consider the nullability of struct fields  of the schema of the dataframes
            when determining whether they are equivalent

    Returns:
        Whether the first dataframes records are a subset of the second dataframes records
    """
    if not datatypes_equal(first.schema, second.schema, check_nullability=check_nullability):
        return False

    diff = first.subtract(second.select(*first.columns))
    diff.show()

    return dataframe_empty(diff)


# Note: functions below have been added and did not appear in the source

def _compare_columns(
  df: DataFrame, 
  col1: str, 
  col2: str, 
  condition: Union[Callable[[str, str], str], Callable[[Column, Column], Column]],
  check_nullability: bool = True,
  verbose: bool=False
) -> bool:
  schema = df.select(col1, col2).schema
  col1_datatype = schema.fields[0].dataType
  col2_datatype = schema.fields[1].dataType

  if not datatypes_equal(col1_datatype, col2_datatype, check_nullability=check_nullability):
      return False

  unequal_rows_df = (
    df
    .filter(~condition(col1, col2))
    .select(col1, col2)
  )
  
  if dataframe_empty(unequal_rows_df):
      return True

  if verbose:
      print(f"Colums {col1} and {col2} are not equal")
      print(f"Showing rows with different values for {col1} and {col2}")
      unequal_rows_df.show()

  return False


def columns_equal(
  df: DataFrame,
  col1: str,
  col2: str,
  check_nullability: bool = True,
  verbose: bool=False
) -> bool:
    """Check equality between two columns within a dataframe.
    Note: The check is null-safe.

    Args:
        df (pyspark.sql.DataFrame): dataframe to check
        col1 (str): first column in the comparison
        col2 (str): second column in the comparison
        check_nullability (bool, optional): whether to check nullability in the 
        column datatypes. Defaults to True.
        verbose (bool, optional): whether to include additional printed output
        if the check fails. Defaults to False.

    Returns:
        bool: Indicates whether the columns and their datatypes are equal
    """
    def _columns_equal_condition(col1, col2):
        return F.col(col1).eqNullSafe(F.col(col2))  # Comparison is null safe
      
    return _compare_columns(df, col1, col2, _columns_equal_condition, check_nullability=check_nullability, verbose=verbose)
      
    
def columns_approx_equal(
  df: DataFrame, 
  col1: str, 
  col2: str, 
  rel_precision: float, # TODO: relative or absolute? relative for now
  check_nullability: bool = True,
  verbose: bool=False
) -> bool:
    """Check equality upto a relative tolerance between two columns 
    within a dataframe.
    Note: The check is null-safe.

    Args:
        df (pyspark.sql.DataFrame): dataframe to check
        col1 (str): first column in the comparison
        col2 (str): second column in the comparison
        rel_precision (float): relative precision of the approximate comparison
        check_nullability (bool, optional): whether to check nullability in the 
        column datatypes. Defaults to True.
        verbose (bool, optional): whether to include additional printed output
        if the check fails. Defaults to False.

    Returns:
        bool: Indicates whether the columns and their datatypes are equal
    """
    def _columns_approx_equal_condition(col1, col2):
        rel_diff = F.coalesce(F.abs(F.col(col1) - F.col(col2)) / F.col(col1), F.lit(-1))  # Comparison is null safe
        return rel_diff <= F.lit(rel_precision)

    return _compare_columns(df, col1, col2, _columns_approx_equal_condition, check_nullability=check_nullability, verbose=verbose)
  
  
SPARK_NUMERIC_TYPES = (T.FloatType, T.DoubleType, T.DecimalType)


def dataframes_approx_equal(
  df1: DataFrame,
  df2: DataFrame,
  rel_precision: float,
  check_nullability: bool=True
) -> bool:
    """Check for equality between dataframes upto a relative tolerance.

    Args:
        df1 (DataFrame): first dataframe in the comparison
        df2 (DataFrame): second dataframe in the comparison
        rel_precision (float): relative precision for the approximate comparison
        check_nullability (bool, optional): whether to check the nullability 
        when comparing datatypes. Defaults to True.

    Returns:
        bool: indicates whether the dataframes contents and datatypes are approximately equal
    """
    # Check for strict equality
    if dataframes_equal(df1, df2, check_nullability=check_nullability):
        return True
        
    # Check schemas are equal
    df1_sorted_cols = df1.select(sorted(df1.columns))
    df2_sorted_cols = df2.select(sorted(df2.columns))
    
    if not datatypes_equal(df1_sorted_cols.schema, df2_sorted_cols.schema, check_nullability=check_nullability):
        return False
    
    # Extract numeric / non-numeric column names
    numeric_cols = [f.name for f in df1.schema.fields if isinstance(f.dataType, SPARK_NUMERIC_TYPES)]
    non_numeric_cols = [f.name for f in df1.schema.fields if f.name not in numeric_cols]
    
    # Check that non-numeric columns are equal
    if not dataframes_equal(df1.select(*non_numeric_cols), df2.select(*non_numeric_cols), check_nullability=check_nullability):
        return False
    
    # Create dataframe from input df containing only rows where an identical row does not exist in the other input df
    diff1 = df1_sorted_cols.subtract(df2_sorted_cols)
    diff2 = df2_sorted_cols.subtract(df1_sorted_cols)
    
    diff1.cache()
    diff2.cache()
    
    # Check that the number of unmatched rows are equal
    if diff1.count() != diff2.count():
        diff1.unpersist()
        diff2.unpersist()
        return False
        
    # Define partition by values of non-numeric columns  
    # Generate JOIN_SEED using this partition to correctly pair rows in diff1 and diff2 for comparison
    # Perform inner join
    join_key_window = W.partitionBy(*non_numeric_cols).orderBy(*numeric_cols, F.rand())
    diff1 = diff1.withColumn("JOIN_SEED", F.row_number().over(join_key_window))
    diff2 = diff2.withColumn("JOIN_SEED", F.row_number().over(join_key_window))
    diff_joined = (
        diff1.alias("diff1")
        .join(diff2.alias("diff2"), how="inner", on=[*non_numeric_cols, "JOIN_SEED"])
    )
    
    # Check that columns are approx equal
    for col in numeric_cols:
        if not columns_approx_equal(diff_joined, f"diff1.{col}", f"diff2.{col}", rel_precision, check_nullability=check_nullability):
            diff1.unpersist()
            diff2.unpersist()
            return False
    
    diff1.unpersist()
    diff2.unpersist() 
    return True