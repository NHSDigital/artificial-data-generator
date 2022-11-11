# Databricks notebook source
from pyspark.sql import types as T


def get_meta_schema() -> T.StructType:
  return T.StructType([
    T.StructField("TABLE_NAME",            T.StringType(),                 True),  # This should ideally be false but that breaks the schema uplift. Left as TODO
    T.StructField("FIELD_NAME",            T.StringType(),                 True),  # This should ideally be false but that breaks the schema uplift. Left as TODO
    T.StructField("VALUE_TYPE",            T.StringType(),                 True),  # This should ideally be false but that breaks the schema uplift. Left as TODO
    T.StructField("VALUE_STRING",          T.StringType(),                 True),
    T.StructField("VALUE_NUMERIC",         T.DoubleType(),                 True),
  #   T.StructField("VALUE_TIMESTAMP",       T.TimestampType(),              True),
    T.StructField("VALUE_DATE",            T.DateType(),                   True),
    T.StructField("VALUE_STRING_ARRAY",    T.ArrayType(T.StringType()),    True),
    T.StructField("VALUE_NUMERIC_ARRAY",   T.ArrayType(T.DoubleType()),    True),
  #   T.StructField("VALUE_TIMESTAMP_ARRAY", T.ArrayType(T.TimestampType()), True),
  #   T.StructField("VALUE_DATE_ARRAY",      T.ArrayType(T.DateType()),      True),
    T.StructField("WEIGHT",                T.DoubleType(),                 True),
    T.StructField("WEIGHT_ARRAY",          T.ArrayType(T.DoubleType()),    True),
  ])