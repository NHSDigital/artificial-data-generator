# Databricks notebook source
# MAGIC %run ../../common/common_exports

# COMMAND ----------

# MAGIC %run ../wide_to_long

# COMMAND ----------

import re
from typing import List, Set, Dict, Tuple, Optional

from pyspark.sql import DataFrame, functions as F

# COMMAND ----------

def classify_meta_types(
  dataframe: DataFrame, 
  threshold_distinct_count: int = 100, 
  possible_date_format: List[str] = ["yyyyMMdd", "dd/MM/yy", "dd/MM/yyyy", 'ddMMyy'],
  default_finder_flag: bool = True,
  sample_frac: float=0.01
) -> Dict[str, List[str]]:
  """Identifies columns in the dataframe into date, categorical and continuous types.
  
  Parameters
  ----------
  dataframe: DataFrame
    The Dataframe that contains the data
    
  threshold_distinct_count: int = 100
    The threshold limit to seperate numerical categorical data and continuous data
    
  possible_date_format: List[str] = ['ddmmyy']
    At the identifying columns with dates stage. This input is used for identifying dates formats that is not in the form YYYY-MM-DD. 
    The user inputs any custom date formats for the function to detect. The formats are in the form https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html
    
  default_finder_flag: bool = True
    At the identifying columns with dates stage. It is assumed in that dates are of the form YYYY-MM-DD. 
    This is a flag on if the user wants the functions to locate default dates. Setting it as false may reduce false positives. 
    
  sample_frac: float = 0.01
    Fraction of rows to use in the identification. Useful for large datasets.
    
  Returns
  -------
  Dict[str,List[str]]
    A dict listing Date (date), Categorical (cat), Continuous (cont)
    
  Eg:
  dataframe = spark.sql('select * from hes.hes_ae_1213')
  classify_meta_types(dataframe)
  
  """
  
  # Sample a subset of the rows (for large datasets)
  df = dataframe.sample(fraction=float(sample_frac), seed=float(42))
  
  unclassified_cols = df.columns
  
  # Stage 1 - identify date columns
  possible_date_cols = [col for col, dtype in df.dtypes if dtype in ['date', 'string', 'timestamp']]
  possible_date_cols = [col for col in possible_date_cols if col in unclassified_cols]
  date_cols = finding_dates_types(
    df, 
    possible_date_cols, 
    possible_date_format,
    default_finder_flag
  )
  unclassified_cols = [col for col in unclassified_cols if col not in date_cols]
  
  # Stage 2 - identify string categorical columns
  categorical_cols = finding_cols_with_letters(df, unclassified_cols)
  unclassified_cols = [col for col in unclassified_cols if col not in categorical_cols]
  
  # Stage 3 - identify continuous columns
  continuous_cols = finding_continous_cols(df, unclassified_cols, threshold_distinct_count)
  unclassified_cols = [col for col in unclassified_cols if col not in continuous_cols]
  
  # Stage 4 - assign remaining columns to categorical
  categorical_cols = [*categorical_cols, *unclassified_cols]
  
  output = {
    "DATE": date_cols,
    "CATEGORICAL": categorical_cols,
    "CONTINUOUS": continuous_cols
  }
  
  return output

# COMMAND ----------

import itertools

def finding_continous_cols(
  dataframe: DataFrame,
  cols_to_analyse: List[str],
  threshold_distinct_count: int
) -> List[str]:
  
  """Collects continous columns
  
  Parameters
  ----------
  dataframe: DataFrame
    The Dataframe that contains the data
    
  cols_to_analyse: List[str]
    List of columns to analyse
    
  threshold_distinct_count: int
    The threshold limit to seperate numerical categorical data and continuous data
    
  Returns
  -------
  List[str]
    List of continous columns
    
  """
  if not cols_to_analyse:
    return []
  
  df = dataframe
  
  # Compute distinct counts for the sample frame
  distinct_count_cols = (F.approx_count_distinct(x).alias(x) for x in cols_to_analyse)
  distinct_count_df = df.agg(*distinct_count_cols)
  
  # Find fields with a distinct count above the threshold
  distinct_count_df = melt(distinct_count_df, value_field_names=cols_to_analyse, value_alias="DISTINCT_COUNT")
  continuous_cols_stage1 = (
    distinct_count_df
    .where(F.col("DISTINCT_COUNT") > threshold_distinct_count)
    .collect()
  )
  continuous_cols_stage1 = [row.FIELD_NAME for row in continuous_cols_stage1]
  remaining_cols = list(set(cols_to_analyse).difference(set(continuous_cols_stage1)))
  
  # Find the remaining columns with a decimal point
  decimal_cols = []
  # Filter cols where 'floor' is a valid operation
  for col in remaining_cols:
    try:
      decimal_col = (F.col(col) - F.floor(F.col(col))).alias(col)
      df.select(decimal_col)  # This will error if decimal_col is invalid
      decimal_cols.append(decimal_col)
    except:
      continue
    
  # Melt the columns
  decimal_df = melt(df.select(*decimal_cols), value_field_names=remaining_cols, value_alias="REMAINING_DECIMAL")
  continuous_cols_stage2 = (
    decimal_df
    .where(F.col("REMAINING_DECIMAL") > 0)
    .select("FIELD_NAME")
    .distinct()
    .collect()
  )
  continuous_cols_stage2 = [row.FIELD_NAME for row in continuous_cols_stage2]
  
  #Combine
  continous_cols = continuous_cols_stage1 + continuous_cols_stage2
  
  return continous_cols


def finding_dates_types(
  dataframe: DataFrame,
  possible_col_dates: List[str], 
  possible_date_format: List[str],
  default_finder_flag: bool = True
) -> List[str]:  
  """Collects date columns
  
  Parameters
  ----------
  dataframe: DataFrame
    The Dataframe that contains the data
    
  possible_col_dates: List[str]
    List of cols to analyse
    
  possible_date_format: List[str]
    A list of custom date formats
    
  default_finder_flag: bool = True
    Assumes the date format is YYYY-MM-DD
    
  Returns
  -------
  List[str]
    List of continous columns
    
  """
  df = dataframe
  
  # Remove columns that are DateType or TimestampType
  date_cols = [col_name for col_name in possible_col_dates if isinstance(df.select(col_name).schema[0].dataType, (T.DateType, T.TimestampType))]
  possible_col_dates = list(set(possible_col_dates).difference(date_cols))
  
  long_df = melt(df.select(*possible_col_dates), value_field_names=possible_col_dates, value_alias="VALUE")
  
  # yyyy-MM-dd has to be first in the following list, since that is the default for to_date:
  # to_date will not be null (regardless of format) if it is passed a (non-null) column which is 
  # already a date, so the coalesce step below will return the first value in the list
  # if it is passed a date; i.e. when dates are given we should fall back to 
  # the format used by Spark to ensure the assigned format is correct
  common_date_formats = [*(["yyyy-MM-dd"] if default_finder_flag else []), *possible_date_format]
  is_date_col = F.coalesce(*[F.to_date("VALUE", fmt) for fmt in common_date_formats]).isNotNull()
  string_date_cols = (
    long_df
    .where(is_date_col)
    .select("FIELD_NAME")
    .distinct()
    .collect()
  )
  date_cols = [*date_cols, *[row.FIELD_NAME for row in string_date_cols]]
  
  return date_cols
  
  
def finding_cols_with_letters(
  dataframe: DataFrame,
  current_col_dates: List[str]
) -> List[str]:
  """Collects Categorical columns that contains letters
  
  Parameters
  ----------
  dataframe: DataFrame
    The Dataframe that contains the data
    
  possible_col_dates: List[str]
    List of cols to analyse
    
  Returns
  -------
  List[str]
    List of Categorical columns with letters
    
  """
  df = dataframe
  
  long_df = melt(df.select(*current_col_dates), value_field_names=current_col_dates, value_alias="VALUE")
  
  #Need to look at the letters - to do this directly with fixed null to int
  is_letter_col = F.col("VALUE").isNotNull() & F.col("VALUE").cast('long').isNull()
  categorical_cols = (
    long_df
    .where(is_letter_col)
    .select("FIELD_NAME")
    .distinct()
    .collect()
  )
  categorical_cols = [row.FIELD_NAME for row in categorical_cols]
  return categorical_cols


# COMMAND ----------

# Reworked classifier from the GP data pipeline - needs testing
# from typing import Iterable, Tuple

# from pyspark.sql import DataFrame, Column, functions as F, Window as W, types as T


# def extract_numerical_variables(
#   df: DataFrame, 
#   var_fields: Iterable[str]=("TABLE_NAME", "FIELD_NAME"), 
#   value_field: str="VALUE"
# ) -> Tuple[DataFrame, DataFrame]:
#   """
#   """
#   high_cardinality_vars_df = extract_high_cardinality_variables(df, var_fields=var_fields, value_field=value_field)
#   decimal_value_vars_df = extract_decimal_value_variables(df, var_fields=var_fields, value_field=value_field)
#   continuous_vars_df = (
#     high_cardinality_vars_df
#     .join(decimal_value_vars_df, how="leftsemi", on=list(var_fields))
#   )
#   discrete_vars_df = (
#     high_cardinality_vars_df
#     .join(decimal_value_vars_df, how="leftanti", on=list(var_fields))
#   )
#   return continuous_vars_df, discrete_vars_df


# def extract_high_cardinality_variables(
#   df: DataFrame, 
#   var_fields: Iterable[str]=("TABLE_NAME", "FIELD_NAME"), 
#   value_field: str="VALUE"
# ) -> DataFrame:
#   """
#   """
#   distinct_value_count = F.approx_count_distinct(value_field, rsd=0.1).alias("DISTINCT_VALUE_COUNT")

#   distinct_count_df = (
#     df.groupBy(*var_fields)
#     .agg(distinct_value_count)
#   )

#   distinct_count_threshold = 10000
#   distinct_count_over_threshold = F.col("DISTINCT_VALUE_COUNT") > distinct_count_threshold

#   high_cardinality_vars_df = (
#     distinct_count_df
#     .filter(distinct_count_over_threshold)
#     .select(*var_fields)
#   )
  
#   return high_cardinality_vars_df


# def extract_decimal_value_variables(
#   df: DataFrame, 
#   var_fields: Iterable[str]=("TABLE_NAME", "FIELD_NAME"), 
#   value_field: str="VALUE"
# ) -> DataFrame:
#   """
#   """
#   double_value = F.col(value_field).cast(T.DoubleType())
#   castable_to_double = double_value.isNotNull()
#   double_count = F.sum(castable_to_double.cast(T.IntegerType()))

#   has_decimal_places = (double_value - F.floor(double_value)) > 0.
#   decimal_count = F.sum(has_decimal_places.cast(T.IntegerType()))

#   decimal_count_df = (
#     df
#     .filter(F.col(value_field).isNotNull())
#     .groupBy(*var_fields)
#     .agg(
#       F.count(F.lit(1)).alias("ROW_COUNT"),
#       double_count.alias("DOUBLE_COUNT"),
#       decimal_count.alias("DECIMAL_COUNT"),
#     )
#   )

#   all_castable_to_double = F.col("ROW_COUNT") == F.col("DOUBLE_COUNT")
#   decimal_fraction_threshold = 0.5
#   decimal_fraction_above_threshold = F.col("DECIMAL_COUNT") / F.col("ROW_COUNT") > decimal_fraction_threshold

#   decimal_vars_df = (
#     decimal_count_df
#     .filter(all_castable_to_double)
#     .filter(decimal_fraction_above_threshold)
#     .select(*var_fields)
#   )
  
#   return decimal_vars_df


# def contains_letter(col_name: str) -> Column:
#   """
#   """
#   first_letter_col = F.regexp_extract(col_name, r"[A-Za-z](?<!(E-10)|(E10))$", 0)
#   return F.when(F.col(col_name).isNotNull(), F.length(first_letter_col) > 0).alias(f"contains_letter({col_name})")


# def extract_categorical_variables(
#   df: DataFrame, 
#   var_fields: Iterable[str]=("TABLE_NAME", "FIELD_NAME"), 
#   value_field: str="VALUE"
# ) -> DataFrame:
#   """
#   """
#   letter_value_vars_df = (
#     df
#     .filter(contains_letter(value_field))
#     .select(*var_fields)
#     .distinct()
#   )
#   return letter_value_vars_df


# # Date formats does not include "yyyyMMdd", "ddMMyyyy", "MMyyyy" since these cause too many non-date 
# # fields to be labelled as dates (e.g. 6-character org codes). 
# # If there is a field with a non-date data type, but where the metadata should be scraped as if it
# # contains a date, then use a manual override on a case-by-case basis
# COMMON_DATE_FORMATS = ["yyyy-MM-dd HH:mm:ss", "yyyyMMdd'T'HHmmss", "yyyy-MM-dd", "dd/MM/yyyy"]  
# row_count = F.count(F.lit(1)).alias("ROW_COUNT")
# is_date_type = F.col("DATA_TYPE").isin(["date", "timestamp"])
  
  
# def total_row_count(*partition_cols: Iterable[str]) -> Column:
#   """
#   """
#   return F.sum("ROW_COUNT").over(W.partitionBy(*partition_cols))


# def try_get_date_format(col_name: str) -> Column:
#   """
#   """
#   date_format = F.coalesce(*[F.when(F.to_date(col_name, fmt).isNotNull(), F.lit(fmt)) for fmt in COMMON_DATE_FORMATS])
#   return date_format
  

# def extract_date_variables(
#   long_df: DataFrame, 
#   var_fields: Iterable[str]=("TABLE_NAME", "FIELD_NAME"), 
#   value_field: str="VALUE"
# ) -> DataFrame:
#   """
#   """
#   date_format_df = (
#     long_df
#     .filter(F.col(value_field).isNotNull())
#     .withColumn("DATE_FORMAT", try_get_date_format(value_field))
#   )
  
#   date_type_vars_df = extract_date_type_variables(date_format_df, *var_fields, "DATE_FORMAT")

#   non_date_type_df = date_format_df.filter(~is_date_type)
#   date_value_vars_df = extract_date_value_variables(non_date_type_df, *var_fields)
  
#   date_vars_df = date_type_vars_df.union(date_value_vars_df)
  
#   return date_vars_df


# def extract_date_type_variables(long_df: DataFrame, *var_fields: Tuple[str, ...]) -> DataFrame:
#   """
#   """
#   date_type_vars_df = (
#     long_df
#     .filter(is_date_type)
#     .select(*var_fields)
#     .distinct()
#   )
#   return date_type_vars_df
  

# def extract_date_value_variables(long_df: DataFrame, *var_fields: Tuple[str, ...]) -> DataFrame:
#   """
#   """
#   date_format_count_df = (
#     long_df
#     .groupBy(*var_fields, "DATE_FORMAT")
#     .agg(row_count)
#     .withColumn("TOTAL_ROW_COUNT", total_row_count(*var_fields))
#   )
  
#   date_value_vars_df = (
#     date_format_count_df
#     .filter(F.col("ROW_COUNT") / F.col("TOTAL_ROW_COUNT") > 0.5)
#     .filter(F.col("DATE_FORMAT").isNotNull())
#     .select(*var_fields, "DATE_FORMAT")
#     .distinct()
#   )
  
#   return date_value_vars_df


# def get_long_form_data_types_column(wide_df: DataFrame) -> Column:
#   field_names, data_types = zip(*((F.lit(f.name), F.lit(f.dataType.jsonValue())) for f in wide_df.schema.fields))
#   data_type_map = F.map_from_arrays(F.array(*field_names), F.array(*data_types))
#   return F.element_at(data_type_map, F.col("FIELD_NAME"))


# def classify_meta_types(
#   wide_df: DataFrame, 
#   var_fields: Iterable[str]=("FIELD_NAME",), 
#   value_field: str="VALUE"
# ) -> DataFrame:
#   """
#   """
#   long_df = melt(wide_df, value_field_names=wide_df.columns)
    
#   # Preserve original data type
#   long_df = long_df.withColumn("DATA_TYPE", get_long_form_data_types_column(wide_df))

  
#   date_vars_df = extract_date_variables(long_df, var_fields=var_fields, value_field=value_field)
#   remaining_df = long_df.join(date_vars_df, how="leftanti", on=list(var_fields))
  
#   categorical_vars_df = extract_categorical_variables(remaining_df, var_fields=var_fields, value_field=value_field)
#   remaining_df = remaining_df.join(categorical_vars_df, how="leftanti", on=list(var_fields))
  
#   continuous_vars_df, discrete_vars_df = extract_numerical_variables(remaining_df, var_fields=var_fields, value_field=value_field)
  
#   unclassified_df = (
#     remaining_df.select(*var_fields).distinct()
#     .join(continuous_vars_df.union(discrete_vars_df), how="leftanti", on=list(var_fields))
#   )

#   meta_types_df = (
#     date_vars_df.withColumn("META_TYPE", F.lit("date")).drop("DATE_FORMAT")
#     .union(continuous_vars_df.withColumn("META_TYPE", F.lit("continuous")))
#     .union(discrete_vars_df.withColumn("META_TYPE", F.lit("discrete")))
#     .union(categorical_vars_df.union(unclassified_df).withColumn("META_TYPE", F.lit("categorical")))
#   )

#   return meta_types_df