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

After you run Batch job, BQ table will be 001/Bronze level (Databricks logic) with 'External Data Configuration' and 'Source URI'.

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

```SQL
CREATE OR REPLACE TABLE `crimes_ds.invalid_crimes_iucr` AS
SELECT DISTINCT IUCR, primary_type, description
FROM `qwiklabs-gcp-03-97da6ea35bd1.crimes_ds.chicago_crime`
WHERE IUCR NOT IN (SELECT IUCR FROM `qwiklabs-gcp-03-97da6ea35bd1.crimes_ds.chicago_iucr_ref`);
```

### This query will

- SELECT DISTINCT IUCR, primary_type, description: This selects the three columns of interest: IUCR, primary_type, and description.  DISTINCT ensures that only unique combinations of these three columns are included in the resulting table.
- WHERE IUCR NOT IN (SELECT IUCR FROM chicago_iucr_ref): This is the crucial part. It filters the results to include only rows where the IUCR code is not present in the chicago_iucr_ref table.  This uses a subquery to get all the IUCR codes from the reference table and then excludes any rows from chicago_crime that match those codes.

## Task 4. Perform comparison based on IUCR descriptions

From last task it is clear that there are codes in the `chicago_crime` (transactions) table that are not in the `chicago_iucr_ref` (reference data) table. You must now extract data that will allow you to understand why there is a misalignment between these two tables.

Create a BigQuery table called `crimes_ds.matching_iucr_descriptions` that contains the records returned in the previous task, where the IUCR code did not match, that can be matched using the fields `primary_description` and `secondary_description` of the table `chicago_iucr_ref` joined with the type of crime and description fields from the `chicago_crime` table.

This will allow you to see how the data ingestion job could be enhanced to reduce or eliminate this issue.

```SQL
CREATE OR REPLACE TABLE crimes_ds.matching_iucr_descriptions AS
SELECT
    -- Select fields from the invalid crimes table
    t1.IUCR AS original_invalid_iucr,
    t1.primary_type,
    t1.description,

    -- Select fields from the reference table that we matched on
    t2.IUCR AS potential_matched_iucr,
    t2.primary_description,
    t2.secondary_description
FROM
    -- Start with the table of invalid crimes from your previous task
    `crimes_ds.invalid_crimes_iucr` AS t1
JOIN
    -- Join to the official IUCR reference table
    `crimes_ds.chicago_iucr_ref` AS t2
ON
    -- The join condition is based on matching BOTH description fields
    t1.primary_type = t2.primary_description
    AND t1.description = t2.secondary_description;
```

### Explanation of the Code

- SELECT ...
  This clause defines the columns that will be in your new table.
  t1.IUCR AS original_invalid_iucr: We select the IUCR code from your invalid_crimes_iucr table and give it a clear alias. This is the code that we know has no direct match.
  t1.primary_type and t1.description: These are the description fields from the original crime data that we will use for matching.
  t2.IUCR AS potential_matched_iucr: This is the crucial part. We select the IUCR code from the reference table (chicago_iucr_ref). If a match is found on the descriptions, this column will show you what the IUCR code should have been.
  t2.primary_description and t2.secondary_description: We include these from the reference table to confirm that our text-based join was successful.

 - FROM \crimes_ds.invalid_crimes_iucr` AS t1`
  This specifies that the source of our data is the invalid_crimes_iucr table you created in the previous task. This is efficient because we are only working with the subset of data that we already know has a problem.
- JOIN ... ON ...
  This is the core logic of the task. We use an INNER JOIN, which means that only rows that find a successful match will be included in the final table.
  t1.primary_type = t2.primary_description AND t1.description = t2.secondary_description: This condition must be met for a row to be included. It checks if the primary crime type from the transaction table matches the primary description in the reference table AND if the secondary description also matches. This ensures a high-quality match.
  
### What This New Table Will Show You

The resulting crimes_ds.matching_iucr_descriptions table will contain a list of crimes that were previously considered "invalid" but have now been successfully identified by matching their text descriptions. By comparing the original_invalid_iucr column to the potential_matched_iucr column, you can directly diagnose the data entry errors.

## Task 5. Create a BigQuery table 'mismatched_iucr_descriptions'

In the previous task you have seen that the reference data table `chicago_iucr_ref` containes some IUCR codes that are missing the left padding with zeroes that is used in the `chicago_crime` table. However the total number of the records found in Task 4 does not match with the total number of records found in Task 3. That means there are still some crime records in the `chicago_crime` table that you haven't matched with either the IUCR or description fields in the `chicago_iucr_ref` table.

Create a BigQuery table called `crimes_ds.mismatched_iucr_descriptions` that contains the records from the `chicago_crime` data listing the IUCR codes and descriptions for records that were not returned in Task 4 in order to explore how those columns differ from the data in the IUCR code reference table.

These are the distinct combinations of IUCR, primary_type and description records that do not match with either the IUCR or description fields in the chicago_iucr_ref table.

```SQL
CREATE OR REPLACE TABLE crimes_ds.mismatched_iucr_descriptions AS
-- Use a Common Table Expression (CTE) to get the distinct combinations from the main transactions table first.
-- This is more efficient than joining on the entire multi-million row table.
WITH distinct_crime_combinations AS (
  SELECT DISTINCT
    IUCR,
    primary_type,
    description
  FROM
    `crimes_ds.chicago_crime`
)
SELECT
  dcc.IUCR,
  dcc.primary_type,
  dcc.description
FROM
  distinct_crime_combinations AS dcc
-- First, try to find a match based on the IUCR code.
LEFT JOIN
  `crimes_ds.chicago_iucr_ref` AS ref_by_code
ON
  dcc.IUCR = ref_by_code.IUCR
-- Second, try to find a match based on the description fields.
LEFT JOIN
  `crimes_ds.chicago_iucr_ref` AS ref_by_desc
ON
  dcc.primary_type = ref_by_desc.primary_description AND dcc.description = ref_by_desc.secondary_description
WHERE
  -- The core logic: keep only the rows where BOTH joins failed to find a match.
  ref_by_code.IUCR IS NULL AND ref_by_desc.IUCR IS NULL;
```

### Explanation of the Code

- WITH distinct_crime_combinations AS (...)
  This is a Common Table Expression (CTE). We use it to create a temporary, named result set (distinct_crime_combinations).
  Inside the CTE, we SELECT DISTINCT IUCR, primary_type, description from the main chicago_crime table. This is a critical optimization. Instead of joining on all 7+ million crime records, we first boil it down to just the few thousand unique combinations of codes and descriptions. This makes the subsequent joins much faster and cheaper.
- FROM distinct_crime_combinations AS dcc
Our main query now uses this pre-aggregated CTE as its source, which we alias as dcc.
- LEFT JOIN ... AS ref_by_code ON dcc.IUCR = ref_by_code.IUCR
This is our first attempt to find a match. We LEFT JOIN our distinct crime combinations to the reference table, trying to connect them by the IUCR code. If a match is found, columns from ref_by_code (like ref_by_code.IUCR) will be filled in. If no match is found, they will be NULL.
- LEFT JOIN ... AS ref_by_desc ON ...
This is our second attempt. We perform another LEFT JOIN to the same reference table, but this time the condition is the combination of primary_type and description. If a match is found on the descriptions, columns from ref_by_desc will be filled in. Otherwise, they will be NULL.
- WHERE ref_by_code.IUCR IS NULL AND ref_by_desc.IUCR IS NULL
This is the most important part of the query. The WHERE clause filters the results of the joins.
ref_by_code.IUCR IS NULL: This keeps only the rows where the first join (by code) failed.
ref_by_desc.IUCR IS NULL: This keeps only the rows where the second join (by description) failed.
By combining them with AND, we are telling BigQuery: "Only give me the records that could not be matched by the IUCR code, and also could not be matched by the descriptions." This precisely isolates the truly mismatched data as required by the task.

### What This Table Represents

The final crimes_ds.mismatched_iucr_descriptions table will contain a clean list of IUCR/description combinations that exist in the transactional chicago_crime data but have no logical equivalent in the chicago_iucr_ref master list. These are the final problem records that require manual investigation to understand the discrepancy.

# Congratulations!

You have successfully performed data integration with Cloud Dataproc and analyzed Chicago crimes dataset using BigQuery.
