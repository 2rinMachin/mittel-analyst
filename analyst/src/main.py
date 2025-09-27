from fastapi import FastAPI
from schemas import *
import time
import boto3
import os

GLUE_DATABASE = os.getenv("GLUE_DATABASE") or "data-analysis-database"
S3_BUCKET = os.getenv("S3_BUCKET") or "mittel-data-analysis-bucket"
OUTPUT_LOCATION = f"s3://{S3_BUCKET}/query-results"
REGION = os.getenv("AWS_REGION") or "us-east-1"

app = FastAPI()

client = boto3.client('athena', region_name=REGION)

def athena_execute(query: str):
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": GLUE_DATABASE},
        ResultConfiguration={"OutputLocation": OUTPUT_LOCATION}
    )
    query_id = response['QueryExecutionId']

    while True:
        status = client.get_query_execution(QueryExecutionId=query_id)
        state = status["QueryExecution"]["Status"]["State"]

        if state == "SUCCEEDED":
            break
        elif state in ["FAILED", "CANCELLED"]:
            raise Exception(f"Athena query failed: {state}")

        time.sleep(2)

    result = client.get_query_results(QueryExecutionId=query_id)
    rows = result["ResultSet"]["Rows"]
    headers = [col["VarCharValue"] for col in rows[0]["Data"]]
    data = [
        {headers[i]: cell.get("VarCharValue", None) for i, cell in enumerate(row["Data"])}
        for row in rows[1:]
    ]

    return data



@app.get("/")
async def status_check():
    return { "status": 200, "title": "OK", "detail": "It works!"}

@app.get("/events/summary")
async def get_posts_history():
    rows = athena_execute("""
    SELECT
        post_id,
        SUM(CASE WHEN kind = 'view' THEN 1 ELSE 0 END) as views,
        SUM(CASE WHEN kind = 'like' THEN 1 ELSE 0 END) as likes,
        SUM(CASE WHEN kind = 'share' THEN 1 ELSE 0 END) as shares
    FROM engagement_events
    GROUP BY post_id
    """)
    return [
        {
            **row,
            "views": int(row["views"]),
            "likes": int(row["likes"]),
            "shares": int(row["shares"])
        } for row in rows]






