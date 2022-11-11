# Databricks notebook source
from typing import Union
from pyspark.sql import DataFrame, Column, functions as F


PHONE_NUMBER_PATTERN = r"^(((\+44\s?\d{4}|\(?0\d{4}\)?)\s?\d{3}\s?\d{3})|((\+44\s?\d{3}|\(?0\d{3}\)?)\s?\d{3}\s?\d{4})|((\+44\s?\d{2}|\(?0\d{2}\)?)\s?\d{4}\s?\d{4}))(\s?\#(\d{4}|\d{3}))?$"
EMAIL_ADDRESS_PATTERN = r"([A-Za-z0-9]+[.-_])*[A-Za-z0-9]+@[A-Za-z0-9-]+(\.[A-Z|a-z]{2,})+"
POSTCODE_PATTERN = r"([Gg][Ii][Rr] 0[Aa]{2})|((([A-Za-z][0-9]{1,2})|(([A-Za-z][A-Ha-hJ-Yj-y][0-9]{1,2})|(([A-Za-z][0-9][A-Za-z])|([A-Za-z][A-Ha-hJ-Yj-y][0-9][A-Za-z]?))))\s?[0-9][A-Za-z]{2})"
GT_6_ALPHA_CHARS_PATTERN = r"^([A-Za-z]{6,})$"


def filter_matches(df: DataFrame, col: Union[Column, str], pattern: str) -> DataFrame:
  """
  Filter rows of a DataFrame based on a regex pattern match within a specified column.
  
  Parameters
  ----------
  df : pyspark.sql.DataFrame
    DataFrame to filter
  col : Union[pyspark.sql.Column, str]
    Column to look for matches within
  pattern : str
    Pattern to be matched
    
  Returns
  -------
  pyspark.sql.DataFrame
    Filtered DataFrame based on pattern matches
    
  """
  extracted_value = F.regexp_extract(col, pattern, 0)
  is_match = F.length(extracted_value) > 0
  match_df = df.filter(is_match)
  return match_df
