# Setup Guide: Rebuilding the Pipeline

The pipeline was built through the AWS console, one section at a time. This guide lists the steps in order so it can be evaluated and rebuilt, if needed. Values that belong to an account appear as placeholders:

| Placeholder         | Meaning                                          |
| ------------------- | ------------------------------------------------ |
| `<ACCOUNT_ID>`      | Your 12-digit AWS account number                 |
| `<DRIVE_FOLDER_ID>` | The ID of the shared Google Drive folder         |
| `<SNS_TOPIC_ARN>`   | The ARN of the alerts topic created in section 6 |

Region used: us-east-1.

Repository files referenced below: `lambda/lambda_function.py`, `glue/silver/silver_build.py`, `glue/gold/gold_build.py`, `orchestration/state_machine.json`, `iam/*.json`, `sql/*.sql`.

## 1. Google Cloud

1. Create a Google Cloud project and enable the **Google Drive API**.
2. Create a **service account** (this build used `drive-reader`) and download a JSON key.
3. Put the 21 CSV files and the PDF data dictionary in one Drive folder, and **share the folder with the service account's email address**.
4. Note the folder ID from the folder's URL like so `https://drive.google.com/drive/folders/<FOLDER_ID>`
5. Test with a short script that lists the folder's files, before touching AWS.

## 2. AWS foundation

1. **Sign in** with IAM Identity Center as an admin user, not the root user. Confirm the account with `aws sts get-caller-identity` in CloudShell.
2. **S3 buckets** (names end with the account number): `gdrive-medallion-bronze-`, `-silver-`, `-quarantine-`, `-gold-` and `-work-`.
3. **DynamoDB table** `gdrive-file-state`, partition key `file_id` (string).
4. **Secrets Manager** secret `gdrive/service-account-key`, holding the Google JSON key.
5. In Athena, set the query result location to `s3://gdrive-medallion-work-<ACCOUNT_ID>/athena-results/`.

## 3. Ingestion (Lambda)

1. **IAM role** `gdrive-ingest-role`: managed policy `AWSLambdaBasicExecutionRole`, plus an inline policy that allows writing to the Bronze bucket, reading and writing the DynamoDB table, and reading the secret (see `iam/`).
2. **Lambda layer** `google-drive-libs` containing `google-api-python-client` and `google-auth`, built for Python 3.12 on x86_64.
3. **Function** `gdrive-ingest` with the code in `lambda/lambda_function.py`, 512 MB, 5-minute timeout, the layer attached, and four environment variables (see `lambda/config.md`).
4. **Test with an empty event** `{}`. First run: all 21 files ingested. Second run: all 21 skipped.

## 4. Silver (Glue)

1. **IAM role** `glue-medallion-role`: managed policy `AWSGlueServiceRole`, plus an inline policy that reads Bronze and reads and writes Silver, quarantine and Gold (see `iam/`).
2. **Glue databases:** `gdrive_silver`, `gdrive_gold`, `gdrive_quarantine`.
3. **Job** `silver-build`: Spark, Python, G.1X workers, 2 workers, script `glue/silver/silver_build.py`, with the parameters in `glue/job-settings.md`.
4. **Build in stages and verify each:** start with `--DATASET pbj_daily_nurse_staffing` (expect 1,325,324 rows), then the provider-info, claims and VBP facility datasets, then the six event tables, then the remaining reference tables. Finally run `--DATASET ALL`.
5. **Test the circuit breaker** deliberately with a file whose IDs have lost their leading zeros; the job should fail with all rows quarantined.
6. **Verify** with `sql/01_row_counts_all_tables.sql` (the Silver rows should total 3,421,978).

## 5. Gold (Glue)

1. **Job** `gold-build`: Spark, Python, G.1X, 2 workers, script `glue/gold/gold_build.py`, with the parameters in `glue/job-settings.md`. Tick the option to use the Glue Data Catalog as the Hive metastore.
2. **Build the tables in dependency order:** `gold_staffing_daily`, `gold_staffing_facility_monthly`, `gold_staffing_state_monthly`, `gold_staffing_state_ownership`, `gold_facility_quality_staffing`, `gold_relationship_stats`.
3. **Check each table** before moving on: the build runs its own blocking checks, then compare row counts with `sql/01_row_counts_all_tables.sql`.
4. **Validate against CMS** using facility 015009 (expected 4.2936).

## 6. Orchestration (Step Functions and EventBridge)

1. **IAM role** `gdrive-pipeline-sfn-role` with two inline policies: one allowing it to invoke `gdrive-ingest` and start the two Glue jobs, one allowing it to publish to the alerts topic (see `iam/`).
2. **SNS topic** `pipeline-alerts` (Standard) with your email subscribed. **Confirm the subscription** from the email AWS sends, or alerts are silently dropped.
3. **State machine** `gdrive-medallion-pipeline` (Standard) from `orchestration/state_machine.json`, with your topic ARN in the `NotifyFailure` state.
4. **Test the quiet path:** start an execution with `{}`. It should end at `NothingNew` in seconds.
5. **Test the changed path:** delete one file's record from the DynamoDB table and start an execution. Silver and Gold should run (about 15 minutes in total), and all row counts should be unchanged afterwards.
6. **Test the alert:** temporarily point the ingest step at a function name that does not exist, confirm the failure email arrives, then restore the real name.
7. **Schedule:** create an EventBridge Scheduler schedule `gdrive-pipeline-daily` (see `orchestration/schedule.md`). Test it by setting it to fire a few minutes ahead, then set it back to 6:00 AM.

## 7. Operating it

- To force a re-run of a file, delete its record from `gdrive-file-state`.
- To pause the pipeline, disable the schedule.
- After any unusual run, re-run `sql/01_row_counts_all_tables.sql`; every difference should be 0.

## 8. Known gaps

- No alert if the schedule never starts the pipeline, or if a run succeeds with wrong data.
- A CSV row with too few fields is only caught if a required column ends up empty.
