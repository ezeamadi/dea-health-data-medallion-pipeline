# Glue Job Settings

Settings for the two AWS Glue jobs. Account-specific values appear as `<ACCOUNT_ID>`. The job scripts are `silver_build.py` and `gold_build.py` in this folder.

## Common settings

| Setting                                     | silver-build                                         | gold-build                                           |
| ------------------------------------------- | ---------------------------------------------------- | ---------------------------------------------------- |
| Job type                                    | Spark                                                | Spark                                                |
| Language                                    | Python                                               | Python                                               |
| Worker type                                 | G.1X                                                 | G.1X                                                 |
| Number of workers                           | 2                                                    | 2                                                    |
| IAM role                                    | glue-medallion-role                                  | glue-medallion-role                                  |
| Spark ANSI mode                             | Off (`spark.sql.ansi.enabled=false`)                 | Off                                                  |
| Use Glue Data Catalog as the Hive metastore | Ticked                                               | Ticked                                               |
| Glue version                                | Glue 6.0                                             | Glue 6.0                                             |
| Job timeout                                 | 30 (minutes)                                         | 30 (minutes)                                         |
| Script location                             | s3://aws-glue-assets-<ACCOUNT_ID>-us-east-1/scripts/ | s3://aws-glue-assets-<ACCOUNT_ID>-us-east-1/scripts/ |

ANSI mode is not among the job's `--conf` parameters, so it is set inside each script. | spark.conf.set("spark.sql.ansi.enabled", "false")

Only one run of each job can be active at a time (the Glue default). The state machine retries if a manual run holds the job.

## silver-build parameters

| Parameter             | Value                                      | Meaning                                                      |
| --------------------- | ------------------------------------------ | ------------------------------------------------------------ |
| `--DATASET`           | `ALL`                                      | A comma-separated list of dataset names, or `ALL` for all 21 |
| `--BRONZE_BUCKET`     | `gdrive-medallion-bronze-<ACCOUNT_ID>`     | Where raw files are read from                                |
| `--SILVER_BUCKET`     | `gdrive-medallion-silver-<ACCOUNT_ID>`     | Where Silver tables are written                              |
| `--QUARANTINE_BUCKET` | `gdrive-medallion-quarantine-<ACCOUNT_ID>` | Where rejected rows are written                              |
| `--SILVER_DB`         | `gdrive_silver`                            | Glue database for Silver tables                              |
| `--QUARANTINE_DB`     | `gdrive_quarantine`                        | Glue database for the quarantine table                       |
| `--conf`              | See below                                  | Spark settings, including the data-lineage listener          |

The `--conf` value, as configured:

```
spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener
--conf spark.openlineage.transport.type=amazon_datazone_api
--conf spark.openlineage.transport.domainId=health-data-analysis
--conf spark.glue.accountId=<ACCOUNT_ID>
--conf spark.openlineage.facets.custom_environment_variables=[AWS_DEFAULT_REGION;GLUE_VERSION;GLUE_COMMAND_CRITERIA;GLUE_PYTHON_VERSION;]
--conf spark.openlineage.columnLineage.datasetLineageEnabled=True
--conf spark.eventLog.rolling.enabled=true
--conf spark.sql.catalog.glue_catalog.glue.skip-name-validation=true
```

The OpenLineage lines send data-lineage events to Amazon DataZone. If a Silver run ever fails with an error that mentions lineage or DataZone, start here.

Run a single dataset by setting `--DATASET` to its name, for example `pbj_daily_nurse_staffing`, or a list such as `nh_provider_info,nh_quality_msr_claims`.

## gold-build parameters

| Parameter       | Value                                                                                                      | Meaning                                      |
| --------------- | ---------------------------------------------------------------------------------------------------------- | -------------------------------------------- |
| `--TABLES`      | See below                                                                                                  | Comma-separated list of Gold tables to build |
| `--GOLD_BUCKET` | `gdrive-medallion-gold-<ACCOUNT_ID>`                                                                       | Where Gold tables are written                |
| `--GOLD_DB`     | `gdrive_gold`                                                                                              | Glue database for Gold tables                |
| `--conf`        | `spark.eventLog.rolling.enabled=true --conf spark.sql.catalog.glue_catalog.glue.skip-name-validation=true` | Spark settings                               |

**The job's saved default for `--TABLES` builds only two tables:**

```
gold_facility_quality_staffing,gold_relationship_stats
```

The state machine overrides it and passes all six, in dependency order:

```
gold_staffing_daily,gold_staffing_facility_monthly,gold_staffing_state_monthly,gold_staffing_state_ownership,gold_facility_quality_staffing,gold_relationship_stats
```

If you run the job by hand to rebuild everything, set `--TABLES` to the full list. Otherwise only the two default tables are refreshed and the other four stay as they were.

## Typical durations

| Job                          | Duration (full run)                   |
| ---------------------------- | ------------------------------------- |
| silver-build (`DATASET=ALL`) | About 9 minutes (8 min 56 s observed) |
| gold-build (all six tables)  | About 5 minutes (5 min 11 s observed) |
