# Nursing-Home Staffing Pipeline: Google Drive to AWS Medallion

An automated AWS data pipeline that ingests 21 CMS nursing-home files from Google Drive, cleans them into 21 typed Silver tables, and builds six analysis-ready Gold tables. The Gold layer answers staffing questions: how many nursing hours residents receive, how that differs by state and ownership type, and whether it relates to occupancy or hospitalization rates.

The pipeline runs daily, loads only files that changed, protects data quality with quarantine and flags, and emails on failure.

**Stack:** AWS Lambda, S3, DynamoDB, Secrets Manager, AWS Glue (Spark and Python), Glue Data Catalog, Athena, Step Functions, EventBridge Scheduler, SNS, IAM, and Amazon QuickSight. I chose this stack as opposed to mostly Glue because I wanted to get my hands on many technologies and couple them.

Author: Eze Amadi https://www.linkedin.com/in/ezeamadi/

## Results (Q2 2024, April to June)

| Metric                                   | Result                                                                                                                                                                           |
| ---------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Staffing level                           | 3.71 nursing hours per resident per day (resident-weighted); 3.84 as a facility average, against CMS's published 3.837                                                           |
| Staffing by role                         | CNA 2.08 (56%), LPN 0.87 (24%), RN 0.60 (16%), other aide 0.16 (4%)                                                                                                              |
| Occupancy                                | About 77.7% nationally, flat across the quarter                                                                                                                                  |
| Staffing vs occupancy                    | No linear relationship (Pearson r = -0.054, Spearman +0.023). The pattern is U-shaped: 3.95 hours under 60% occupancy, about 3.81 from 60 to 100%, 4.07 above 100%               |
| Contract-labor share                     | 7.16% of hours in April, 6.89% in June                                                                                                                                           |
| Staffing vs short-stay rehospitalization | Weak negative association (r = -0.061; RN staffing -0.083). The most-staffed quartile of facilities has a rehospitalization rate of 22.41%, against 23.26% for the least-staffed |
| Staffing by ownership                    | For-profit groups cluster at 3.57 to 3.58 hours; non-profit corporations 4.17; church-related 4.26; state-run 4.81; hospital districts 3.53                                      |

**Validation:** for facility 015009, the pipeline's computed hours per resident per day is 4.2936, exactly CMS's published 4.2936.

## Architecture

```
Google Drive folder (21 CSVs + 1 PDF data dictionary)
        |  read by a service account
        v
Lambda: gdrive-ingest  <---- checksums ---->  DynamoDB: gdrive-file-state
        |  new or changed files only
        v
S3 Bronze      raw files, partitioned by ingest date
        v
Glue: silver-build  --->  S3 Silver (21 typed tables)
        |                 Quarantine (rejected rows)
        v
Glue: gold-build    --->  S3 Gold (6 analysis tables)
        v
Athena (queries Silver and Gold through the Glue Data Catalog)
        v
QuickSight (Data Visualization)

EventBridge Scheduler (daily, 6:00 AM Chicago)
   --> Step Functions: ingest --> if new files --> Silver --> Gold
   --> any failure --> SNS email (SNS email)
```

| Layer  | Contents                                   | Size                                                    |
| ------ | ------------------------------------------ | ------------------------------------------------------- |
| Bronze | Raw files exactly as received              | 21 files                                                |
| Silver | Cleaned, typed tables, one per source file | 21 tables, 3,421,978 rows, 0 quarantined, 3,957 flagged |
| Gold   | Wide, pre-joined analysis tables           | 6 tables                                                |

| Gold table                     | One row per                               | Rows      |
| ------------------------------ | ----------------------------------------- | --------- |
| gold_staffing_daily            | Facility and day                          | 1,325,324 |
| gold_staffing_facility_monthly | Facility and month                        | 43,692    |
| gold_staffing_state_monthly    | State and month                           | 156       |
| gold_staffing_state_ownership  | State, ownership type and quarter         | 486       |
| gold_facility_quality_staffing | Facility and quarter                      | 14,564    |
| gold_relationship_stats        | Statistic (6 pairs, Pearson and Spearman) | 12        |

## Data

Source: CMS nursing-home data. The main file is the Payroll-Based Journal daily nurse staffing file for Q2 2024 (one row per facility per day). The other 20 files are the October 2024 CMS releases: provider information, quality measures, health and fire-safety citations, penalties, ownership, and value-based purchasing.

Everything joins on the CMS Certification Number (CCN), a 6-character string such as `015009`. It is stored as text everywhere because a numeric type would drop leading zeros and break joins.

The data files are not in this repository because of size, and they are public CMS releases. See `.gitignore`.

## Key design decisions

| Decision                                                              | Why                                                                                                        |
| --------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| Lambda for ingestion, Glue for transformation                         | Ingestion is light file movement; transformation of 1.3 million-row files suits Spark                      |
| Checksum comparison against a DynamoDB state table                    | Loads only new or changed files, so the first run and every later run use the same code                    |
| Read every column as text, then cast                                  | Keeps leading zeros in the CCN                                                                             |
| ANSI mode off; uncastable values become NULL plus a flag              | Bad values are surfaced and counted instead of crashing the job                                            |
| Quarantine table plus a 5% reject-rate circuit breaker                | Bad rows are kept for inspection; a bad file cannot silently corrupt Silver                                |
| Delete-then-write per partition                                       | Re-running a build never doubles rows                                                                      |
| Denormalized Gold with no date dimension                              | Optimized for read speed; no query needs a join. An earlier star-schema draft was replaced for this reason |
| Hours per resident per day as a ratio of sums                         | Not an average of daily ratios, so busy days and large facilities count in proportion                      |
| Flag, do not delete, suspect values                                   | Every exclusion is visible and reversible                                                                  |
| Step Functions Standard workflow, rebuild all of Silver on any change | Glue runs exceed Express limits; no file-to-table mapping to get wrong                                     |
| Failure email sent from inside the state machine                      | Same coverage as an EventBridge rule, and it works with the console as it is                               |

Details: [docs/data-quality-rules.md](docs/data-quality-rules.md), [docs/gold-model.md](docs/gold-model.md).

## How it runs

Every day at 6:00 AM Chicago time, EventBridge starts a Step Functions state machine. It runs the ingest Lambda. If no file changed, it stops. If something changed, it runs `silver-build` (about 9 minutes) and then `gold-build` (about 5 minutes). Any failure sends an email. For the first time it ingests everything and runs end-to-end.

A forced end-to-end test (one file's DynamoDB record deleted) took about 15 minutes; all 27 Silver and Gold row counts were unchanged afterwards, and the next run ended quietly.

## Repository layout

```
README.md
glue/            silver/silver_build.py, silver/glue-silver-build.json, gold/gold_build.py, gold/glue-gold-build.json, job-settings.md
lambda/          lambda_function.py, config.md, lambda-config.json
orchestration/   state_machine.json, schedule.md, schedule.json
iam/             gdrive-pipeline-sfn-role/ IAM policies as JSON
sql/             Athena checks and analysis queries
docs/            data-quality-rules.md, gold-model.md, setup-guide.md, images/
```

## Reproduce it

See [docs/setup-guide.md](docs/setup-guide.md) for the build in order. Account-specific values appear as placeholders such as `<ACCOUNT_ID>` and `<DRIVE_FOLDER_ID>`.

## Data Visualization

The visualization was done with Amazon QuickSight =>
[docs/images/dashboard-correlation.png](docs/images/dashboard-correlation.png)
[docs/images/dashboard-ownership.png](docs/images/dashboard-ownership.png)
[docs/images/dashboard-staffing-vs-occupancy.png](docs/images/dashboard-staffing-vs-occupancy.png)
[docs/images/dashboard-state-type-1.png](docs/images/dashboard-state-type-1.png)
[docs/images/dashboard-state-type-2.png](docs/images/dashboard-state-type-2.png)
[docs/images/dashboard-weekly-trend.png](docs/images/dashboard-weekly-trend.png)

## Limitations

- Overtime and length of stay cannot be calculated from this data.
- Correlations are weak (none above 0.10) and observational; case mix, facility size and ownership are not controlled for.
- Staffing covers April to June 2024; the quality measures come from the October 2024 release and cover other periods.
- A CSV row with too few fields is only caught if a required column ends up empty. A field-count check is a known gap.
- The 1.0 hours-per-resident-per-day cut-off for implausibly low staffing is a judgment; values continue smoothly past it.
- The pipeline does not alert if its schedule never fires, or if a run succeeds with wrong data. The row-count query in `sql/` is the manual check.
