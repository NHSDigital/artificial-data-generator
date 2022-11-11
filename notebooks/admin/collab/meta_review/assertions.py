# Databricks notebook source
from pyspark.sql import DataFrame, Column, functions as F
from nhsdccl.util import DFCheckpoint


class StatisticalDisclosureControlException(Exception):
  def __init__(self, message="Statistical disclosure control check failed!"):
    return super().__init__(message)
  
  
def modulo(col_name: str, x: int) -> Column:
  """ Spark SQL modulo expression as a PySpark Column.
  """
  return F.expr(f"{col_name} % {x}")
  
  
def assert_statistical_disclosure_controls(
  meta_df: DataFrame,
  freq_col: Column, 
  threshold: int = 100, 
  rounding: int = 100
) -> None:
  """ 
  Check that the statistical disclosure controls are correctly applied to the metadata.
  By default the threshold and rounding on frequencies is set to 100 to avoid accidental disclosures.

  Parameters
  ----------
  meta_df: DataFrame
    Metadata
  freq_col: Column
    Column of `meta_df` holding the frequencies
  threshold: int (default = 100)
    Minimum frequency for a value to appear in the metadata
  rounding: int (default = 100)
    Frequencies should be rounded to this nearest value
    
  Returns
  -------
  None

  Raises
  ------
  StatisticalDisclosureControlException
    Raised if any frequencies do not obey the disclosure controls

  """
  # Filters for rows that break the SDCs
  frequency_below_threshold = F.col("FREQUENCY") < F.lit(threshold)
  frequency_not_divisible_by_rounding = modulo("FREQUENCY", rounding) != F.lit(0)
  
  # Find rows that break the SDCs
  sdc_check_df = (
    meta_df
    .withColumn("FREQUENCY", freq_col)
    .filter(frequency_below_threshold | frequency_not_divisible_by_rounding)
  )
    
  # Check that there are no rows breaking the SDCs
  with DFCheckpoint(sdc_check_df) as df_cp:
    try:
      assert df_cp.df.first() is None
  
    except AssertionError as e:
      print("Frequency check failed for the following rows: ")
      df_cp.df.show()
      raise StatisticalDisclosureControlException
    
