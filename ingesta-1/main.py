from datetime import datetime
from mypy_boto3_s3 import S3Client
import os
import json
import boto3
import psycopg2
from psycopg2.extensions import cursor
from psycopg2.extras import RealDictCursor

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


def fetch_all_users(cursor: cursor):
    _ = cursor.execute("select * from users")
    return cursor.fetchall()

def fetch_all_sessions(cursor: cursor):
    _ = cursor.execute("select * from sessions")
    return cursor.fetchall()


def upload_json_rows(client: S3Client, rows: list[tuple[object]], key: str): 
    body = "\n".join(json.dumps(r,default=serializer) for r in rows)

    _ = client.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json"
    )


def main():
    print("Fetching data...")

    with psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USERNAME,
        password=DB_PASSWORD,
        database=DB_DATABASE,
    ) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            users = fetch_all_users(cursor)
            sessions = fetch_all_sessions(cursor)

    print("Uploading data...")

    client: S3Client = boto3.client("s3")  # pyright: ignore[reportUnknownMemberType]
    upload_json_rows(client, users, "users/users/users.json")
    upload_json_rows(client, sessions, "users/sessions/sessions.json")

    print("Data uploaded successfully.")



if __name__ == "__main__":
    main()

