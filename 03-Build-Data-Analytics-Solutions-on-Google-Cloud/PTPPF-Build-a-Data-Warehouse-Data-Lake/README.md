# [PTPPF: Build a Data Warehouse / Data Lake](https://partner.cloudskillsboost.google/paths/2536/course_templates/1089/labs/554902)

Lab PTPPF033

This lab is a part of the upcoming Partner Technical Presales Proficiency Framework (PTPPF) Program. This lab is intended for all technical Googlers and Partners. This lab is not intended for public consumption beyond this audience.

----------------
# Challenge lab overview
This lab challenges you to perform actions and automation across products. Instead of following step-by-step instructions, you are given a common business scenario and a set of tasks – you then figure out how to complete them on your own, using your skills to come up with a solution that demonstrates your understanding of key concepts. An automated scoring system provides feedback on whether you have completed your tasks correctly. Are you up for the challenge?

## Objectives
This lab challenges you to demonstrate your ability to perform the following tasks:

- Code-free data integration from Cloud Storage to BigQuery with Dataproc Templates.
- Analyze data quality.
----------------

# Challenge scenario
You are a data engineer with the Chicago Police Department working as part of the team that performs crime analytics tasks. The BigQuery Chicago crimes dataset that you work with includes Illinois Uniform Crime Reporting (IUCR) code descriptions.

A copy of the Chicago crimes data already exists in a BigQuery dataset available to you and updated IUCR code description data is stored in a CSV stored in a Cloud Storage bucket.

## Your challenge
Chicago Police Department has received a file containing the latest valid IUCR codes. You must ensure the accuracy of IUCR information in the crimes data that is stored in BigQuery as part of an exercise to validate data quality.

The data architect has recommended loading the IUCR reference data using Dataproc templates. Once the IUCR data has been loaded you must perform an analysis of the IUCR codes with BigQuery SQL across the ingested IUCR reference data and the existing crimes data that will be used to define data cleaning requirements for IUCR code reference data updates that will be performed in future.

## Task 1. Run a Dataproc Serverless Spark batch template workload
In this task you must load the updated IUCR reference data into BigQuery using a Dataproc template.

1) Submit a Spark batch workload using the Dataproc Cloud Storage to BigQuery template to import the IUCR reference data into a BigQuery table named `chicago_iucr_ref` under the `crimes_ds` dataset in your lab project.

2) Use the following parameters when running the Spark batch workload :

<img width="779" height="574" alt="image" src="https://github.com/user-attachments/assets/436e45cc-3496-4877-9ab9-803314b12e97" />

Note: It can take up to 2-3 minutes to register the progress after job completion. Please wait for 2-3 minutes and try again.

## Task 2. Analyze data in BigQuery

You will see the table chicago_iucr_ref is created in the database crimes_ds once batch job completes. You can now explore and analyze the data in the Google BigQuery and compare the imported IUCR codes with the chicago_crime table.

1) Use the following code block to write a query to create a crime trend report set with crimes by type and year:

```SQL
SELECT
  year,
  primary_type,
  description,
  COUNT(*) AS crime_count
FROM
  crimes_ds.chicago_crime
GROUP BY
  year,
  primary_type,
  description
ORDER BY
  year,
  primary_type,
  description
```

2) Use the following code block to ensure that there are no entries in the `chicago_crime` table in the BigQuery dataset `crimes_ds` that are missing either `primary_type` or `description` columns:

```SQL
SELECT
  *
FROM
  crimes_ds.chicago_crime
WHERE
  primary_type IS NULL
  OR description IS null
```

3) Write a query to analyze the contents of the `chicago_crime` and `chicago_iucr_ref` tables.

## Task 3. Identify if there are IUCR code/description discrepancies/mismatches across tables

To complete this task you must create a BigQuery table called `crimes_ds.invalid_crimes_iucr` that contains all of the distinct combinations of `IUCR`, `primary_type` and `description` from the table `chicago_crime` where the IUCR code is not found in the `chicago_iucr_ref` table.

## Task 4. Perform comparison based on IUCR descriptions

From last task it is clear that there are codes in the `chicago_crime` (transactions) table that are not in the `chicago_iucr_ref` (reference data) table. You must now extract data that will allow you to understand why there is a misalignment between these two tables.

Create a BigQuery table called `crimes_ds.matching_iucr_descriptions` that contains the records returned in the previous task, where the IUCR code did not match, that can be matched using the fields `primary_description` and `secondary_description` of the table `chicago_iucr_ref` joined with the type of crime and description fields from the `chicago_crime` table.

This will allow you to see how the data ingestion job could be enhanced to reduce or eliminate this issue.

## Task 5. Create a BigQuery table 'mismatched_iucr_descriptions'

In the previous task you have seen that the reference data table `chicago_iucr_ref` containes some IUCR codes that are missing the left padding with zeroes that is used in the `chicago_crime` table. However the total number of the records found in Task 4 does not match with the total number of records found in Task 3. That means there are still some crime records in the `chicago_crime` table that you haven't matched with either the IUCR or description fields in the `chicago_iucr_ref` table.

Create a BigQuery table called `crimes_ds.mismatched_iucr_descriptions` that contains the records from the `chicago_crime` data listing the IUCR codes and descriptions for records that were not returned in Task 4 in order to explore how those columns differ from the data in the IUCR code reference table.

These are the distinct combinations of IUCR, primary_type and description records that do not match with either the IUCR or description fields in the chicago_iucr_ref table.


# Congratulations!

You have successfully performed data integration with Cloud Dataproc and analyzed Chicago crimes dataset using BigQuery.
