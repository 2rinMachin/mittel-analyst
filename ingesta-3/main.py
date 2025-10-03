from datetime import datetime
from mypy_boto3_s3 import S3Client
import os
import json
import boto3

from mysql.connector import connect
from mysql.connector.abstracts import MySQLCursorAbstract
from mysql.connector.types import RowItemType, RowType

DB_HOST = os.environ["DB_HOST"]
DB_PORT = os.environ["DB_PORT"]
DB_USERNAME = os.environ["DB_USERNAME"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
DB_DATABASE = os.environ["DB_DATABASE"]

S3_BUCKET = os.environ["S3_BUCKET"]


def serializer(obj: object):
    if isinstance(obj, datetime):
        return obj.strftime("%Y-%m-%d %H:%M:%S")

    raise TypeError(f"Type {type(obj)} is not serializable")


def fetch_all_events(cursor: MySQLCursorAbstract):
    _ = cursor.execute("select * from events")
    return cursor.fetchall()

def fetch_all_devices(cursor: MySQLCursorAbstract):
    _ = cursor.execute("select * from devices")
    return cursor.fetchall()


def upload_json_rows(client: S3Client, rows: list[RowType | dict[str, RowItemType]], key: str): 
    body = "\n".join(json.dumps(r,default=serializer) for r in rows)
    print(body)

    _ = client.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json"
    )


def main():
    print("Fetching events...")

    with connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USERNAME,
        password=DB_PASSWORD,
        database=DB_DATABASE,
    ) as conn:
        with conn.cursor(dictionary=True) as cursor:
            events = fetch_all_events(cursor)
            devices = fetch_all_devices(cursor)

    print("Uploading events...")

    client: S3Client = boto3.client("s3")  # pyright: ignore[reportUnknownMemberType]
    upload_json_rows(client, events, "engagement/events/events.json")
    upload_json_rows(client, devices, "engagement/devices/devices.json")

    print("Data uploaded successfully.")



if __name__ == "__main__":
    main()

