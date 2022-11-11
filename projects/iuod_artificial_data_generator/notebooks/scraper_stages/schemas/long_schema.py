# Databricks notebook source
from pyspark.sql import types as T


def get_long_schema() -> T.StructType:
    return T.StructType([
        T.StructField("FIELD_NAME",      T.StringType(), False),
        T.StructField("VALUE_STRING",    T.StringType(), True),
        T.StructField("VALUE_NUMERIC",   T.DoubleType(), True),
        T.StructField("VALUE_DATE",      T.DateType(),   True),
        T.StructField("VALUE_TIMESTAMP", T.TimestampType(),  True),
    ])