# Databricks notebook source
# MAGIC %run ./regex_patterns

# COMMAND ----------

test_df = spark.createDataFrame(
  [
    ["hello@gmail.com"], 
    ["hello.world@nhs.net"], 
    ["some other string"]
  ], 
  "VALUE: string"
)
assert filter_matches(test_df, "VALUE", EMAIL_ADDRESS_PATTERN).count() == 2

# COMMAND ----------

test_df = spark.createDataFrame(
  [
    ["07123456789"], 
    ["07 123 456 789"], 
    ["+447123456789"]
  ], 
  "VALUE: string"
)
assert filter_matches(test_df, "VALUE", PHONE_NUMBER_PATTERN).count() == 2  # Need to strip spaces

# COMMAND ----------

test_df = spark.createDataFrame(
  [
    ["LS1 4AP"], 
    ["LS14AP"],
    ["Hello"]
  ], 
  "VALUE: string"
)
assert filter_matches(test_df, "VALUE", POSTCODE_PATTERN).count() == 2

# COMMAND ----------

test_df = spark.createDataFrame(
  [
    ["ABCdef"], 
    ["ghiJK"]
  ], 
  "VALUE: string"
)
assert filter_matches(test_df, "VALUE", GT_6_ALPHA_CHARS_PATTERN).count() == 1