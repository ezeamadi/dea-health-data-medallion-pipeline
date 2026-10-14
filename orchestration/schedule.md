# Orchestration: Schedule and State Machine

The state machine definition is `state_machine.json` in this folder.

## Schedule (EventBridge Scheduler)

| Setting              | Value                                                               |
| -------------------- | ------------------------------------------------------------------- |
| Schedule name        | gdrive-pipeline-daily                                               |
| Type                 | Recurring, cron-based                                               |
| Expression           | `cron(0 6 * * ? *)` (6:00 AM every day)                             |
| Time zone            | America/Chicago, so 6:00 stays 6:00 through daylight saving changes |
| Flexible time window | Off                                                                 |
| Target               | Step Functions StartExecution on `gdrive-medallion-pipeline`        |
| Input                | `{}`                                                                |
| Retry policy         | Maximum age 1 hour, 2 retries                                       |
| Dead-letter queue    | None                                                                |
| Role                 | Created with the schedule; it may only start this one state machine |

## State machine (Step Functions, Standard)

| State            | Type                                 | What it does                                                                                                                            |
| ---------------- | ------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------- |
| IngestFromDrive  | Task (Lambda invoke)                 | Runs `gdrive-ingest`; retries twice (5 s interval, backoff 2) on Lambda service errors; failure goes to NotifyFailure                   |
| CheckForNewFiles | Choice                               | If `ingested_count` is above 0, go to BuildSilver; otherwise NothingNew                                                                 |
| NothingNew       | Succeed                              | Ends the run with no Glue cost                                                                                                          |
| BuildSilver      | Task (Glue job, wait for completion) | Runs `silver-build` with `--DATASET ALL`; 60-minute timeout; retries 3 times (120 s interval, backoff 1.5) if another run holds the job |
| BuildGold        | Task (Glue job, wait for completion) | Runs `gold-build` with `--TABLES` set to all six tables; same timeout and retry                                                         |
| NotifyFailure    | Task (SNS publish)                   | Emails the execution name, error and cause to the `pipeline-alerts` topic                                                               |
| PipelineFailed   | Fail                                 | Ends the run as Failed                                                                                                                  |

`BuildGold` runs only if `BuildSilver` succeeded, so a failed Silver build never reaches the analysis tables. Silver tables processed before a failure are already updated; Gold keeps the last good data until the next good run.

## Permissions (gdrive-pipeline-sfn-role)

Two inline policies (JSON in `iam/`):

- `pipeline-run-permissions`: invoke the `gdrive-ingest` function, and start, read and stop runs of the `silver-build` and `gold-build` Glue jobs.
- `pipeline-notify`: publish to the `pipeline-alerts` SNS topic.

## Alerts

- SNS topic `pipeline-alerts` (Standard) with the owner's email subscribed and confirmed.
- The email arrives from the `NotifyFailure` state when ingest, Silver or Gold fails or times out.
- A first attempt used an EventBridge rule on failed executions. The console had changed, so the alert was built inside the state machine, with the same coverage.

## What was tested

| Test                                        | Result                                                                                            |
| ------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| Quiet day                                   | Ends at NothingNew in seconds; 21 files skipped                                                   |
| Forced change (one DynamoDB record deleted) | About 15 minutes: ingest 3 s, Silver 8 min 56 s, Gold 5 min 11 s                                  |
| After the forced run                        | All 27 Silver and Gold row counts unchanged; the next run ended quiet                             |
| Schedule                                    | Set to fire a few minutes ahead; the execution appeared with input `{}`; then set back to 6:00 AM |
| Alert                                       | Ingest step pointed at a non-existent function; failure email arrived; real name restored         |

## Operating notes

- Do not run the Glue jobs by hand around 6:00 to 6:20 AM; the pipeline retries for about 7 minutes and then fails.
- Pause the pipeline by disabling the schedule.
- Re-run a file by deleting its record from `gdrive-file-state`.

## Not covered by alerts

| Situation                              | What to do                                                           |
| -------------------------------------- | -------------------------------------------------------------------- |
| The schedule never starts the pipeline | Check the schedule's page occasionally                               |
| The run succeeds but the data is wrong | Run `sql/01_row_counts_all_tables.sql`; every difference should be 0 |

A later improvement could add an alert when no run has happened in 25 hours, and the row-count check as a final pipeline step.
