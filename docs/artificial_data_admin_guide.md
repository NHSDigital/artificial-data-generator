## Guide for artificial data admins
This section is aimed at administrators of artificial data, working in an environment where they can trigger the different stages of the process.

### How do I extract metadata?
To extract metadata for a dataset, run the `run_notebooks.py` notebook in the corresponding `artificial_{dataset_name}_meta` (for example `artificial_hes_meta/run_notebooks.py`). 

By default, this will read data from a database named according to the `{dataset_name}` parameter in the template string above and write to a database called `artificial_{dataset_name}_meta`. These can be changed via the notebook widgets.

When running in production on live data, this should be done by triggering the `run_notebooks` job for the respective project, as this will have the privileges to access the live data. 
Approved human users will only have access to access aggregates for review, not to the underlying record-level data.

### What happens after metadata is extracted?
Once the metadata has been extracted, it should be manually reviewed by a member of staff working in a secure environment to ensure no personally identifiable information (PII) is disclosed. This should be signed off by a senior member of staff. 

At NHS Digital we have a checklist that was approved by the Statistical Disclosure Control Panel, chaired by the Chief Statistician. 

Once we have checked the metadata and signed it off we move it into a database which inaccessible to the metadata scraper and so is completely isolated from the database containing the real data. 
This is done by executing the `run_notebooks.py` notebook in the `iuod_artificial_data_admin` project.

When running in production on live data, this should be done by triggering the `run_notebooks` job for the `iuod_artificial_data_admin` project, as this will have the privileges to read from / write to the appropriate databases.

### How do I generate artificial data?
To generate data and run postprocessing for a dataset, run the `run_notebooks.py` notebook in the `iuod_artificial_data_generator` project with the name of the dataset to generate artificial data for entered accordingly. 
For example for artificial HES data set the 'artificial_dataset' parameter to 'hes'.

By default, this process will read metadata from and write artificial data to a database called `iuod_artificial_data_generator`, but this parameter can be changed via the notebook widgets. 

When running in production on live data, this should be done by triggering the `run_notebooks` job for the `iuod_artificial_data_generator` project, as this will have the privileges to access the aggregated data.