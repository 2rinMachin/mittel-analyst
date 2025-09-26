from datetime import datetime
import os
import json
from typing import Any
import boto3

from mysql.connector import connect

DB_HOST = os.environ["DB_HOST"]
DB_PORT = os.environ["DB_PORT"]
DB_USERNAME = os.environ["DB_USERNAME"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
DB_DATABASE = os.environ["DB_DATABASE"]

S3_BUCKET = os.environ["S3_BUCKET"]


def serializer(obj: Any):
    if isinstance(obj, datetime):
        return obj.strftime("%Y-%m-%d %H:%M:%S")

    raise TypeError(f"Type {type(obj)} is not serializable")


def fetch_all_events():
    with connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USERNAME,
        password=DB_PASSWORD,
        database=DB_DATABASE,
    ) as conn:
        with conn.cursor(dictionary=True) as cursor:
            _ = cursor.execute("select * from events")
            return cursor.fetchall()


def upload_json_rows(rows: list[Any], key: str): 
    body = "\n".join(json.dumps(r,default=serializer) for r in rows)

    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json"
    )


def main():
    print("Fetching events...")
    events = fetch_all_events()

    print("Uploading events...")
    upload_json_rows(events, "engagement/events/events.json")

    print("Data uploaded successfully.")



if __name__ == "__main__":
    main()

