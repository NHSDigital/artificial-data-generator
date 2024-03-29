# Databricks notebook source
# MAGIC %run ../notebooks/common/widget_utils

# COMMAND ----------

# MAGIC %run ../notebooks/common/table_helpers

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("database_name", "artificial_hes", "0.1 Project Database")

# COMMAND ----------

# Config
database_name = dbutils.widgets.get("database_name")
table_name = "user_docs"

# Check database exists
if not database_exists(spark, database_name):
  # Database not found - exit
  dbutils.notebook.exit({})

# Template variables replaced during build
user_notice_file_name = "artificial_data_user_notice"

# COMMAND ----------

user_notice_html = """<h1> Notice For Artificial Data Users </h1>

<h2> What is artificial data? </h2>

<h3> Artificial data is an anonymous representation of real data </h3>
<ul>
<li> Artificial data provides an anonymous representation of some of the properties of real datasets. </li>
<li> Artificial data preserves the formatting and structure of the original dataset, but may otherwise be unrealistic. </li>
<li> Artificial data reproduces some of the statistical properties and content complexity of fields in the real data, while excluding cross-dependencies between fields to prevent risks of reidentification. </li>
<li> Artificial data is completely isolated from any record-level data. </li>
<li> It is not possible to use artificial data to reidentify individuals, gain insights, or build statistical models that would transfer onto real data. </li>
</ul>

<h2> How is it generated? </h2>

There are three stages involved in generating the artificial data:

<ol>
<li> The Metadata Scraper: extracts anonymised, high-level aggregates from real data at a national level. At this stage key identifiers (such as patient ID) are removed and small number suppression is applied in order to prevent reidentification at a later stage. </li>
<li> The Data Generator: samples from the aggregates generated by the Metadata Scraper on a field-by-field basis and puts the sampled values together to create artificial records. </li>
<li> Postprocessing: using the output of the Data Generator, dataset-specific tweaks are applied to make the data appear more realistic (such as swapping randomly generated birth and death dates to ensure sensible ordering). This also includes adding in randomly generated identifying fields (such ‘patient’ ID) which were removed at the Metadata Scraper stage. </li>
</ol>

<h2> What is it used for? </h2>

The purpose of artificial data is twofold. 

<h3> 1. Artificial data enables faster onboarding for new data projects </h3>

Users can begin work on a project in advance of access to real data being granted. There are multiple use cases for the artificial data.
<ul>
<li> For TRE/DAE users who have submitted (or intend to submit) an access request for a given dataset: artificial data can give a feel for the format and layout of the real data prior to accessing it. It also allows users to create and test pipelines before accessing real data, or perhaps without ever accessing real data at all. </li>
<li> For TRE/DAE users who are unsure which datasets are relevant to their project: artificial data allows users to understand which data sets would be useful for them prior to applying for access. Artificial data can complement technical information found in the Data Dictionary for a given dataset, and give users a feel for how they would work with that dataset. </li>
</ul>

<h3> 2. Artificial data minimises the amount of personal data being processed </h3>

The activities mentioned above can all be completed without handling personal data, improving the ability of NHS Digital to protect patient privacy by minimising access to sensitive data.


<h2> What are its limitations? </h2>

Artificial data is not real data, and is not intended to represent something real or link to real records. Artificial records are not based on any specific records found in the original data, only on high-level, anonymised aggregates. As outlined above, it is intended to improve efficiency and protect patient data.

It is crucial to note that artificial data is not synthetic data.

Synthetic data is generated using sophisticated methods to create realistic records. Usually synthetic data aims to enable the building of statistical models or gaining of insights that transfer onto real data. This is not be possible with artificial data. The downside of synthetic data is that it is associated with non-negligible risks to patient privacy through reidentification. It is not possible to reidentify individuals using artificial data.


<h2> Additional Information </h2>

<h3> Support from senior leadership </h3>
This work has undergone due process to fully assess any potential risks to patient privacy and has been approved by senior leadership within NHS Digital, including: the Senior Information Risk Owner (SIRO); the Caldicott Guardian; the Data Protection Officer (DPO); the Executive Director of Data and Analytics Services; and the IAOs for the datasets represented by the Artificial Data assets. A full DPIA has been completed and is available upon request.

For further details, please get in touch via the mailbox linked below.

<h2> Contact </h2>
For further details, please get in touch via: <a href="mailto:nhsdigital.artificialdata@nhs.net">nhsdigital.artificialdata@nhs.net</a>
"""

# COMMAND ----------

# Create and upload the docs
docs_data = [[user_notice_file_name, user_notice_html]]
docs_schema = "file_name: string, content_html: string"
docs_df = spark.createDataFrame(docs_data, docs_schema)

create_table(spark, docs_df, database_name, table_name, format="delta", mode="overwrite")

# Check the uploads
for file_name, content_html in docs_data:
  result_content_html = (
    spark
    .table(f"{database_name}.{table_name}")
    .where(F.col("file_name") == file_name)
    .first()
    .content_html
  ) 
  assert result_content_html == content_html

# COMMAND ----------

