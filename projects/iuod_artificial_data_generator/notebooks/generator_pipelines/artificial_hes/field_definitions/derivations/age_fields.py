# Databricks notebook source
from typing import Union
from pyspark.sql import Column, functions as F

AGE_IN_DAYS_CUTOFF = 120 * 365

mydob_date = F.to_date("MYDOB", "MMyyyy")


def get_fractional_age_field(ref_date: Union[str, Column], dob: Union[str, Column]) -> Column:
  age_in_days = F.datediff(ref_date, dob)
  return (
    F.when(age_in_days.isNull(), F.expr("null"))
    .when(age_in_days < 1, 0.002)
    .when(age_in_days <= 6, 0.010)
    .when(age_in_days <= 28, 0.048)
    .when(age_in_days <= 90, 0.167)
    .when(age_in_days <= 181, 0.375)
    .when(age_in_days <= 272, 0.625)
    .when(age_in_days <= 365 - 1, 0.875)
    .when(age_in_days >= AGE_IN_DAYS_CUTOFF, 120)
    .otherwise(age_in_days / 365)
    .cast("double")
  )


def get_categorized_age_field(ref_date: Union[str, Column], dob: Union[str, Column]) -> Column:
  age_in_days = F.datediff(ref_date, dob)
  return (
    F.when(age_in_days.isNull(), F.expr("null"))
    .when(age_in_days < 1, 7001)
    .when(age_in_days <= 6, 7002)
    .when(age_in_days <= 28, 7003)
    .when(age_in_days <= 90, 7004)
    .when(age_in_days <= 181, 7005)
    .when(age_in_days <= 272, 7006)
    .when(age_in_days <= 365-1, 7007)
    .when(age_in_days >= AGE_IN_DAYS_CUTOFF, 120)
    .otherwise(F.round(age_in_days / 365, 0))
    .cast("integer")
  )
  

def get_fractional_from_categorized_age_field(categorized_age: str) -> Column:
  return (
    F.when(F.col(categorized_age).isNull(), F.expr("null"))
    .when(F.col(categorized_age) == 7001, 0.002)
    .when(F.col(categorized_age) == 7002, 0.010)
    .when(F.col(categorized_age) == 7003, 0.048)
    .when(F.col(categorized_age) == 7004, 0.167)
    .when(F.col(categorized_age) == 7005, 0.375)
    .when(F.col(categorized_age) == 7006, 0.625)
    .when(F.col(categorized_age) == 7007, 0.875)
    .otherwise(F.col(categorized_age))
    .cast("double")
  )
