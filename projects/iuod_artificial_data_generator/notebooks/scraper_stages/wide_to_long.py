# Databricks notebook source
# MAGIC %run ../common/coerce_schema

# COMMAND ----------

# MAGIC %run ./schemas/long_schema

# COMMAND ----------

from typing import Iterable
from itertools import groupby
from functools import reduce

from pyspark.sql import (
  DataFrame, 
  types as T, 
  functions as F,
)

# region Prevent downstream linting highlights
coerce_schema = coerce_schema  
get_long_schema = get_long_schema  
# endregion
  
DATA_TYPE_VALUE_ALIAS_MAP = {
  "double": "VALUE_NUMERIC",
  "integer": "VALUE_NUMERIC",
  "string": "VALUE_STRING",
  "date": "VALUE_DATE",
  "timestamp": "VALUE_TIMESTAMP",
}


def wide_to_long(wide_df: DataFrame, preserved_field_names: Iterable[str]=tuple()) -> DataFrame:
  """Transform from wide- to long-formatted data. This uses melt() under the hood to transform
  the data to the schema specified by get_long_schema() which is in schemas/long_schema.py 

  Args:
      wide_df (DataFrame): Wide data to transform
      preserved_field_names (Iterable[str], optional): Fields on the wide data to preserve during the 
        transformation. Values will be duplicated across the resulting rows in the long data. Defaults to tuple().

  Returns:
      DataFrame: Long-formatted data
  """
  fields_sorted_by_data_type = sorted(wide_df.schema.fields, key=get_data_type_as_json)
  fields_grouped_by_data_type = groupby(fields_sorted_by_data_type, key=get_data_type_as_json)
  
  long_dfs = []

  for data_type, field_group in fields_grouped_by_data_type:
    value_field_names = [field.name for field in field_group]
    value_alias = DATA_TYPE_VALUE_ALIAS_MAP.get(data_type, "VALUE_STRING")
    long_dfs.append(
      melt(
        wide_df, 
        value_alias=value_alias,
        value_field_names=value_field_names,
        preserved_field_names=preserved_field_names,
      )
    )
    
  target_schema = T.StructType([
    *get_long_schema().fields, 
    *wide_df.select(*preserved_field_names).schema.fields
  ])
  long_df = reduce(DataFrame.union, map(lambda df: coerce_schema(df, target_schema), long_dfs))
  
  return long_df
  

def get_data_type_as_json(field: T.StructField) -> str:
  """Get the dataType attribute of a pyspark StructField as a json string. 
  Similar types are converged on a single value (e.g. all integer-like types return 'integer')

  Args:
      field (T.StructField): Field to extract the dataType from

  Returns:
      str: Data type of the given field as a json string
  """
  data_type = field.dataType
  
  if isinstance(data_type, (T.DecimalType, T.FloatType)):
    return "double"
    
  elif isinstance(data_type, (T.LongType, T.ShortType, T.ByteType)):
    return "integer"
  
  else:
    return data_type.jsonValue()


def melt(
  df: DataFrame, 
  value_field_names: Iterable[str], 
  variable_alias: str="FIELD_NAME", 
  value_alias: str="VALUE", 
  preserved_field_names: Iterable[str] = None
):
  """Similar to pandas.melt function. Unpivot a pyspark DataFrame from wide to long format.
  
  Args:
      df (pyspark.sql.DataFrame): DataFrame to unpivot
      value_field_names (Iterable[str]): Names of the columns in df to unpivot
      variable_alias (str (default="FIELD_NAME")): Name to give to the column in the unpivoted data containing the names of the unpivoted fields
      value_alias (str (default="VALUE")): Name to give to the column in the unpivoted data containing the values of the unpivoted fields
      preserved_field_names (Iterable[str] (default=None)): Fields from the original DataFrame to preserve on the final melted DataFrame 
        (these columns will have their values copied across to each row in the melted DataFrame)
  
  Returns:
      pyspark.sql.DataFrame: Unpivoted data
    
  Notes:
      Since pyspark is sensitive to types, the values in the unpivoted columns will be casted to a common
      coercible type. E.g. if the columns are a mixture of doubles and integers, but all numeric, then 
      they will be casted to doubles. If no common type can be found, the colunms will be casted to strings.
  
  """
  if set(value_field_names).difference(set(df.columns)):
    raise ValueError("One or more value fields not in columns of DataFrame")
    
  value_fields = [f for f in df.schema.fields if f.name in value_field_names]
  
  if not preserved_field_names:
    preserved_field_names = []
    
  if not value_fields:
    # Create empty dataframe with correct schema
    empty_df = df.select(F.lit("").alias(value_alias), F.lit("").alias(variable_alias), *preserved_field_names).limit(0)
    return empty_df

  # Need to cast the types of the values to be stacked
  if all_has_data_type(value_fields, T.DateType):
    cast_type = T.DateType()
  elif any_has_data_type(value_fields, T.StringType) | any_has_data_type(value_fields, T.DateType):
    cast_type = T.StringType()
  elif any_has_data_type(value_fields, T.DoubleType):
    cast_type = T.DoubleType()
  else:
    cast_type = value_fields[0].dataType
  
  vars_vals_col = F.array([
    F.struct(
      F.lit(f.name).alias(variable_alias), 
      F.col(f.name).cast(cast_type).alias(value_alias)
    ) for f in value_fields
  ])
  
  melted_df = (
    df
    .withColumn("VARIABLE_VALUES", F.explode(vars_vals_col))
    .selectExpr(*preserved_field_names, "VARIABLE_VALUES.*")
  )
  
  return melted_df


def has_data_type(data_type):
  def _has_data_type(field):
    return isinstance(field.dataType, data_type)
  return _has_data_type

  
def map_has_data_type(fields, data_type):
  return map(has_data_type(data_type), fields)


def any_has_data_type(fields, data_type):
  return any(map_has_data_type(fields, data_type))


def all_has_data_type(fields, data_type):
  return all(map_has_data_type(fields, data_type))
  