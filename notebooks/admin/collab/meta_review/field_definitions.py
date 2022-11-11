# Databricks notebook source
from pyspark.sql import functions as F


table_name_col = F.col("TABLE_NAME")
field_name_col = F.col("FIELD_NAME")

is_categorical_col = F.col("VALUE_TYPE") == "CATEGORICAL"
is_discrete_col = F.col("VALUE_TYPE") == "DISCRETE"
is_date_col = F.col("VALUE_TYPE") == "DATE"
is_continuous_col = F.col("VALUE_TYPE") == "CONTINUOUS"

is_demographic_categorical_col = F.col("VALUE_TYPE") == "DEMOGRAPHIC_CATEGORICAL"
is_demographic_date_col = F.col("VALUE_TYPE") == "DEMOGRAPHIC_DATE"

is_relationship_col = F.col("VALUE_TYPE") == "RELATIONSHIP"