import io
import json
import os
import re
from datetime import datetime, timezone

import boto3
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

BUCKET = os.environ["BUCKET"]
TABLE = os.environ["TABLE"]
SECRET_NAME = os.environ["SECRET_NAME"]
FOLDER_ID = os.environ["FOLDER_ID"]

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

s3 = boto3.client("s3")
secrets = boto3.client("secretsmanager")
table = boto3.resource("dynamodb").Table(TABLE)


def get_drive_client():
    secret = secrets.get_secret_value(SecretId=SECRET_NAME)["SecretString"]
    creds = service_account.Credentials.from_service_account_info(
        json.loads(secret), scopes=SCOPES
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def list_csv_files(drive):
    files, page_token = [], None
    while True:
        resp = drive.files().list(
            q=f"'{FOLDER_ID}' in parents and trashed = false",
            fields="nextPageToken, files(id, name, md5Checksum, modifiedTime)",
            pageSize=100,
            pageToken=page_token,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        ).execute()
        files.extend(f for f in resp.get("files", []) if f["name"].lower().endswith(".csv"))
        page_token = resp.get("nextPageToken")
        if not page_token:
            return files


def download_file(drive, file_id):
    buffer = io.BytesIO()
    request = drive.files().get_media(fileId=file_id, supportsAllDrives=True)
    downloader = MediaIoBaseDownload(buffer, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return buffer.getvalue()


def lambda_handler(event, context):
    drive = get_drive_client()
    now = datetime.now(timezone.utc)
    ingest_date = now.strftime("%Y-%m-%d")
    run_ts = now.strftime("%Y%m%dT%H%M%SZ")

    ingested, skipped = [], []

    for f in list_csv_files(drive):
        fingerprint = f.get("md5Checksum") or f["modifiedTime"]
        existing = table.get_item(Key={"file_id": f["id"]}).get("Item")

        if existing and existing.get("fingerprint") == fingerprint:
            skipped.append(f["name"])
            continue

        body = download_file(drive, f["id"])
        stem = re.sub(r"[^A-Za-z0-9_-]+", "_", f["name"].rsplit(".", 1)[0]).lower()
        key = f"source=gdrive/file={stem}/ingest_date={ingest_date}/{run_ts}_{stem}.csv"

        s3.put_object(
            Bucket=BUCKET,
            Key=key,
            Body=body,
            Metadata={"drive_file_id": f["id"], "fingerprint": fingerprint},
        )
        table.put_item(
            Item={
                "file_id": f["id"],
                "name": f["name"],
                "fingerprint": fingerprint,
                "modified_time": f["modifiedTime"],
                "s3_key": key,
                "last_ingested_at": now.isoformat(),
            }
        )
        ingested.append(f["name"])

    result = {"ingested": ingested, "skipped": skipped, "ingested_count": len(ingested), "skipped_count": len(skipped)}
    print(json.dumps(result))
    return result