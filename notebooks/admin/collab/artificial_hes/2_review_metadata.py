# Databricks notebook source
# MAGIC %md # Review HES Metadata
# MAGIC This notebook is used to review the HES metadata for sign-off against the disclosure controls checklist. 

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

# MAGIC %run ../meta_review/field_definitions

# COMMAND ----------

# MAGIC %run ../meta_review/assertions

# COMMAND ----------

# MAGIC %run ../meta_review/regex_patterns

# COMMAND ----------

import json
from functools import reduce
from pyspark.sql import functions as F, Column, DataFrame

from nhsdccl.util import DFCheckpoint
from dsp.udfs import is_valid_nhs_number

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config

# COMMAND ----------

database_name = "artificial_hes_meta"
table_name = "artificial_hes_meta"
table_path = f"{database_name}.{table_name}"

hes_sdc_params = {
  "threshold": 5, 
  "rounding": 5,
}

# Filters for checking string values
value_length_gt_5 = F.col("VALUE_LENGTH") > 5  # Need enough characters to actually be identifiable (i.e. narrow down to 1 in 60 million)
weight_below_100 = F.col("WEIGHT") < 100  # Low frequency values (PID will be infrequent)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Constants

# COMMAND ----------

# There should only be relational rows with a cardinality, no values
included_id_fields = [
  "PSEUDO_HESID",
  "AEKEY",
  "EPIKEY",
  "ATTENDKEY",
]

# These should not appear at all
excluded_id_fields = [
  "NHS_NUMBER",
  "NEWNHSNO",
  "TOKEN_PERSON_ID",
  "PERSION_ID",
  "HESID",
  "MPSID",
  "LOPATID",
  "PATPATHID",
  "BOOKREFNO",
  "CDSUNIQUEID",
  "SUSRECID",
  "ATTENDNO",
  "ATTENDID",
  "PREFERER",  # Pseudo'd practitioner ID
]

# COMMAND ----------

# MAGIC %md
# MAGIC # Main

# COMMAND ----------

# Ingest the data
meta_df = spark.table(table_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Frequency Checks
# MAGIC Check that frequencies are rounded and small numbers are suppressed.

# COMMAND ----------

# Run the checks
field_meta_df = meta_df.filter(~is_relationship_col)  # Exclude relationship frequency distribution which doesn't require suppression since no actual values are contained 
assert_statistical_disclosure_controls(field_meta_df, F.col("WEIGHT"), **hes_sdc_params)
print("Automated disclosure control checks passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checks on ID Fields

# COMMAND ----------

# MAGIC %md ### Check Exclusions
# MAGIC Certain IDs should not appear in the metadata. Fields won't appear in the metadata if they don't appear in the schema for the table being scraped, but this check guards against situations where the schema gets updated but we don't add new ID fields to the list of ID fields (e.g. TOKEN_PERSON_ID), so that the field ends up accidentally sneaking through as a categorical field.

# COMMAND ----------

assert meta_df.filter(F.col("FIELD_NAME").isin(excluded_id_fields)).first() is None
print("ID exclusion check passed: no excluded ID fields were found in the metadata!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check Inclusions
# MAGIC This checks ID fields that do appear in the metadata as part of cardinal relationships. No values from these fields should be scraped, so all the rows relating to these fields should correspond to entries in the relationship metadata. No other rows should exist.

# COMMAND ----------

# Check no values for ID fields other than relationship cardinality / frequency pairs
assert (
  meta_df
  .filter(~is_relationship_col)
  .filter(F.col("FIELD_NAME").isin(included_id_fields))
  .first() is None
), "Found possible values for ID fields!"

# COMMAND ----------

# Check no values for fields which should be null
assert (
  meta_df
  .filter(is_relationship_col)
  .filter(~F.isnull("VALUE_DATE") | ~F.isnull("VALUE_STRING_ARRAY") | ~F.isnull("WEIGHT_ARRAY"))  # Shouldn't find any values for these columns
  .first() is None
), "Found values in metadata for ID fields that should be null!"

# Check 'VALUE_STRING' is only the name of another ID field
assert (
  meta_df
  .filter(is_relationship_col)
  .filter(~F.col("VALUE_STRING").isin(included_id_fields))  # String values should just be field names
  .first() is None
), "Found unexpected values in metadata for ID fields"

# COMMAND ----------

# MAGIC %md
# MAGIC ## High Cardinality Checks

# COMMAND ----------

# MAGIC %md
# MAGIC Check for high-cardinality values and make sure they aren't disclosive.

# COMMAND ----------

# MAGIC %md ### Derive Bases

# COMMAND ----------

# MAGIC %md 
# MAGIC Cardinality checks are based on ratios: this section derives bases to use for calculating them.

# COMMAND ----------

def get_cardinality_bases(meta_df: DataFrame) -> DataFrame:
  """ Derive bases to calculate ratios used in cardinality checks.
  
  Parameters
  ----------
  meta_df : pyspark.sql.DataFrame
    Metadata
  cond_col : pyspark.sql.Column
    Filtering condition applied to the metadata
  weight_col : pyspark.sql.Column
    Column of `meta_df` containing the weights assigned to the values
    
  Returns
  -------
  pyspark.sql.DataFrame
    Aggregated data with bases
    
  """
  base_df = (
    meta_df
    .withColumn("IS_WEIGHT_BELOW_THRESHOLD", weight_below_100.cast("double"))
    .groupBy("TABLE_NAME", "FIELD_NAME", "VALUE_TYPE")
    .agg(
      F.count(F.lit(1)).alias("TOTAL_VALUE_COUNT"),  # Each row contains a distinct value, this count includes nulls to prevent ratios < 1
      F.sum("IS_WEIGHT_BELOW_THRESHOLD").alias("COUNT_WEIGHT_BELOW_THRESHOLD"),
      F.sum("WEIGHT").alias("TOTAL_WEIGHT"),
      F.collect_set(F.struct("VALUE_STRING", "VALUE_NUMERIC", "VALUE_DATE", "VALUE_STRING_ARRAY")).alias("VALUES")
    )
  )
  
  return base_df


# COMMAND ----------

base_df = get_cardinality_bases(field_meta_df)
base_df.persist()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ratio: Records To Values

# COMMAND ----------

# MAGIC %md
# MAGIC This ratio tells us on average how many unique records exist per distinct value.
# MAGIC - 1 indicates that there is a 1-to-1 mapping between records and values (i.e. this would be true for ID fields) (high cardinality)
# MAGIC - large numbers indicate that many records share the same value (low cardinality)
# MAGIC 
# MAGIC Note: we use a total weight after small number suppression as a proxy for number of records; in practice the former will likely be lower than the latter, due to the exclusion of low weights.

# COMMAND ----------

display(
  base_df
  .withColumn("RECORDS_TO_VALUES", F.col("TOTAL_WEIGHT") / F.col("TOTAL_VALUE_COUNT"))
  .select("TABLE_NAME", "FIELD_NAME", "VALUE_TYPE", "RECORDS_TO_VALUES")
  .orderBy(F.asc_nulls_last("RECORDS_TO_VALUES"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ratio: Values with Weight < Threshold to Values

# COMMAND ----------

# MAGIC %md
# MAGIC (Ignored)
# MAGIC This ratio tells us on average how many values with weight < threshold there are per value. E.g. 0.75 means 3 quarters of values have a weight below the threshold.

# COMMAND ----------

# # TODO: update md above
# display(
#   base_df      
#   .withColumn("FRAC_VALUES_BELOW_WEIGHT", F.col("COUNT_WEIGHT_BELOW_THRESHOLD") / F.col("TOTAL_VALUE_COUNT"))
#   .select("TABLE_NAME", "FIELD_NAME", "SUMMARY_TYPE", "FRAC_VALUES_BELOW_WEIGHT", "VALUES")
#   .orderBy(F.desc_nulls_last("FRAC_VALUES_BELOW_WEIGHT"))
#   # Add number of values
#   # What values are there?
# )

# COMMAND ----------

base_df.unpersist()

# COMMAND ----------

# MAGIC %md ## String Checks

# COMMAND ----------

# All values converted to strings
string_value_col = F.coalesce(
  F.col("VALUE_STRING"), 
  F.col("VALUE_STRING_ARRAY_ELEMENT"), 
  F.col("VALUE_NUMERIC").cast("string"), 
  F.col("VALUE_DATE").cast("string")
)
string_value_df = (
  field_meta_df
  .withColumn("VALUE_STRING_ARRAY_ELEMENT", F.explode_outer("VALUE_STRING_ARRAY"))
  .withColumn("VALUE", string_value_col)
)
  
# Add length of string values and apply filters
value_length_df = (
  string_value_df
  .withColumn("VALUE_LENGTH", F.length("VALUE"))
  .filter(value_length_gt_5)
  .filter(weight_below_100)  
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Internal Consistency Checks on Value Lengths

# COMMAND ----------

# MAGIC %md
# MAGIC Look at values that have a length which is very different from the median length for that field.

# COMMAND ----------

describe_cols = [
  F.expr("approx_percentile(VALUE_LENGTH, 0.5)").alias("MEDIAN_VALUE_LENGTH"),
  F.mean("VALUE_LENGTH").alias("MEAN_VALUE_LENGTH"),
  F.max("VALUE_LENGTH").alias("MAX_VALUE_LENGTH"),
  F.min("VALUE_LENGTH").alias("MIN_VALUE_LENGTH"),
]

value_length_stats_df = (
  value_length_df
  .transform(
    lambda df: df.join(
      df.groupBy("TABLE_NAME", "FIELD_NAME", "VALUE_TYPE").agg(*describe_cols), 
      how="inner", 
      on=["TABLE_NAME", "FIELD_NAME", "VALUE_TYPE"])
  )
)

display(
  value_length_stats_df
  .select("TABLE_NAME", "FIELD_NAME", "VALUE_TYPE", "VALUE", "WEIGHT", "VALUE_LENGTH", "MEDIAN_VALUE_LENGTH", "MIN_VALUE_LENGTH", "MAX_VALUE_LENGTH")
  .orderBy(F.abs(F.col("VALUE_LENGTH") - F.col("MEDIAN_VALUE_LENGTH")).desc(), F.col("WEIGHT"))  # Order by greatest MAD length
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern Recognition

# COMMAND ----------

# MAGIC %md
# MAGIC Scan for
# MAGIC - 'Valid' NHS number where a value has length 10 (valid doesn't mean it's a real NHS number)
# MAGIC - Personal UK phone numbers based on Regex pattern
# MAGIC - Email addresses based on Regex pattern
# MAGIC 
# MAGIC HES-specific (these may be red herrings - e.g. SNOMED descriptions, hospital names, org postcode etc)
# MAGIC - Postcodes based on Regex pattern
# MAGIC - 6 or more alpha characters in a row
# MAGIC - Whitespace

# COMMAND ----------

is_valid_nhs_number_udf = F.udf(is_valid_nhs_number, returnType="boolean")

assert (
  value_length_df
  .filter(F.col("VALUE_LENGTH") == 10)
  .filter(is_valid_nhs_number_udf("VALUE"))
  .first() is None
), "Found values that match the pattern for valid NHS numbers!"

# COMMAND ----------

org_fields = ["REFERORG", "PURCODE", "GPPRAC"]  # Excluded from postcode checks since they will fail due to the formatting (even if they do not contain PID)

value_nospace_col = F.regexp_replace("VALUE", r"[\s\t]+", "")

with DFCheckpoint(value_length_df) as df_cp:
  # Check for email addresses
  assert filter_matches(df_cp.df, value_nospace_col, EMAIL_ADDRESS_PATTERN).count() == 0, "Found values that match the pattern for email addresses!"
  
  # Check for postcodes
  assert filter_matches(df_cp.df, value_nospace_col, POSTCODE_PATTERN).filter(~F.col("FIELD_NAME").isin(org_fields)).count() == 0, "Found values that match the pattern for postcodes!"
  
  # Check for phone numbers
  value_length_gt_10_df = df_cp.df.filter(F.col("VALUE_LENGTH") > 10)
  assert filter_matches(value_length_gt_10_df, value_nospace_col, PHONE_NUMBER_PATTERN).count() == 0, "Found values that match the pattern for phone numbers!"
  

# COMMAND ----------

# MAGIC %md
# MAGIC Check strings of more than 6 alpha characters.

# COMMAND ----------

display(
  filter_matches(value_length_df, "VALUE", GT_6_ALPHA_CHARS_PATTERN)
  .select("TABLE_NAME", "FIELD_NAME", "VALUE", "WEIGHT")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Check fields containing internal whitespace.

# COMMAND ----------

value_trimmed_col = F.trim(F.regexp_replace("VALUE", r"[\t\s]+$", ""))
value_nospace_col = F.regexp_replace("VALUE", r"[\s\t]+", "")

(
  value_length_df
  .filter(value_trimmed_col != value_nospace_col)
  .select("TABLE_NAME", "FIELD_NAME", "VALUE", "WEIGHT", value_trimmed_col.alias("TRIMMED"), value_nospace_col.alias("NO_SPACES"))
  .orderBy("WEIGHT")
  .collect()
)

# COMMAND ----------

