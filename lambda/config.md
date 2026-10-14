# Lambda Configuration: gdrive-ingest

The function code is `lambda_function.py` in this folder.

## Settings

| Setting        | Value                                                                          |
| -------------- | ------------------------------------------------------------------------------ |
| Function name  | gdrive-ingest                                                                  |
| Runtime        | Python 3.12                                                                    |
| Architecture   | x86_64                                                                         |
| Handler        | lambda_function.lambda_handler                                                 |
| Memory         | 512 MB                                                                         |
| Timeout        | 5 minutes                                                                      |
| Execution role | gdrive-ingest-role                                                             |
| Layer          | google-drive-libs (google-api-python-client, google-auth; Python 3.12, x86_64) |

## Environment variables

| Key           | Value                                  |
| ------------- | -------------------------------------- |
| `BUCKET`      | `gdrive-medallion-bronze-<ACCOUNT_ID>` |
| `TABLE`       | `gdrive-file-state`                    |
| `SECRET_NAME` | `gdrive/service-account-key`           |
| `FOLDER_ID`   | `<DRIVE_FOLDER_ID>`                    |

## Permissions (gdrive-ingest-role)

- Managed policy `AWSLambdaBasicExecutionRole` (logging).
- Inline policy: write to the Bronze bucket, read and write the DynamoDB table `gdrive-file-state`, read the secret `gdrive/service-account-key`. The policy JSON is in `iam/`.

## What the function does

1. Reads the Google service-account key from Secrets Manager.
2. Lists the CSV files in the Drive folder (the PDF is skipped).
3. For each file, compares its fingerprint (the Drive md5 checksum, with the modified time as a fallback) with the record in DynamoDB.
4. Downloads only new or changed files and writes them to Bronze.
5. Updates the DynamoDB record and returns counts and lists.

Changes are detected by comparing checksums instead of using Drive's change-token mechanism, because the folder holds only 21 files.

## Bronze key layout

```
s3://gdrive-medallion-bronze-<ACCOUNT_ID>/source=gdrive/file=<file name>/ingest_date=YYYY-MM-DD/<timestamp>_<file name>.csv
```

Every ingest writes a new timestamped object, so earlier versions are kept.

## DynamoDB state record (table gdrive-file-state)

| Field                     | Meaning                            |
| ------------------------- | ---------------------------------- |
| `file_id` (partition key) | The Drive file ID                  |
| `name`                    | File name                          |
| `fingerprint`             | md5 checksum (or modified time)    |
| `modified_time`           | Drive's last-modified time         |
| `s3_key`                  | Where the latest copy is in Bronze |
| `last_ingested_at`        | When it was last loaded            |

## Response shape

```json
{
  "ingested": [],
  "skipped": ["PBJ_Daily_Nurse_Staffing_Q2_2024.csv", "..."],
  "ingested_count": 0,
  "skipped_count": 21
}
```

The state machine branches on `ingested_count`: above 0 runs Silver and Gold, otherwise the pipeline stops.

## Testing

- Test event: `{}`.
- First run: `ingested_count` 21. Second run: `skipped_count` 21.
- To force a file to look new, delete its record from `gdrive-file-state` and run again.

## Building the layer

```bash
mkdir layer && cd layer
pip install google-api-python-client google-auth --target python --platform manylinux2014_x86_64 --python-version 3.12 --implementation cp --only-binary=:all:
zip -r google-layer.zip python
```

Upload the zip as a layer for Python 3.12 and x86_64.
