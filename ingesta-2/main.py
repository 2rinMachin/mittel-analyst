from datetime import datetime
from mypy_boto3_s3 import S3Client
from bson import ObjectId
import os
import json
from pymongo import MongoClient
import boto3

MONGO_URI = os.environ["MONGO_URI"]
MONGO_DB = os.environ["MONGO_DB"]
S3_BUCKET = os.environ["S3_BUCKET"]


def serializer(obj: object):
    if isinstance(obj, datetime):
        return obj.strftime("%Y-%m-%d %H:%M:%S")

    if isinstance(obj, ObjectId):
        return str(obj)

    raise TypeError(f"Type {type(obj)} is not serializable")


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

    mongo_client = MongoClient(MONGO_URI)  
    db = mongo_client[MONGO_DB]

    articles = list(db.articles.find())
    comments = list(db.comments.find())

    print("Uploading data...")

    client: S3Client = boto3.client("s3") 
    upload_json_rows(client, articles, "articles/articles/articles.json")
    upload_json_rows(client, comments, "articles/comments/comments.json")

    print("Data uploaded successfully.")



if __name__ == "__main__":
    main()

