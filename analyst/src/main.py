from fastapi import FastAPI
from typing import List
from schemas import *
import time
import boto3
import os

GLUE_DATABASE = os.getenv("GLUE_DATABASE") or "data-analysis-database"
S3_BUCKET = os.getenv("S3_BUCKET") or "mittel-analyst-bucket"
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

@app.get("/tags/top", response_model=List[TopTagsResponse])
async def get_top_tags():
    rows = athena_execute("""
    SELECT q.tag AS tag FROM (
            SELECT
                t.tag AS tag,
                SUM(p.score) AS total_trending_score
            FROM post_scores p
                CROSS JOIN UNNEST(p.tags) AS t(tag)
            GROUP BY t.tag
        ) q
    ORDER BY q.total_trending_score DESC
    LIMIT 10;
    """)
    return [TopTagsResponse(tag=row["tag"]) for row in rows]

@app.get("/tags/topavg", response_model=List[TopTagsResponse])
async def get_top_avg_tags():
    rows = athena_execute("""
    SELECT q.tag AS tag
    FROM (
            SELECT
                t.tag AS tag,
                AVG(p.score) AS avg_trending_score
            FROM post_scores p
                CROSS JOIN UNNEST(p.tags) AS t(tag)
            GROUP BY t.tag
        ) q
    ORDER BY q.avg_trending_score DESC
    LIMIT 10;
    """)
    return [TopTagsResponse(tag=row["tag"]) for row in rows]

@app.get("/articles/top")
async def get_top_articles():
    rows = athena_execute("""
    SELECT
        p.post_id AS article_id,
        p.author_id AS author_id,
        u.username AS username,
        p.title AS title
    FROM post_scores p
        LEFT JOIN users u ON u.id = p.author_id
    ORDER BY p.score DESC
    LIMIT 10;
""")
    return [TopArticlesResponse(
        article_id=row["article_id"],
        author_id=row["author_id"],
        username=row["username"],
        title=row["title"],
        views=int(row["views"] or 0),
        likes=int(row["likes"] or 0),
        shares=int(row["shares"] or 0),
        comments=int(row["comments"] or 0)
    ) for row in rows]

@app.get("/users/listactive", response_model=List[ActiveUsersResponse])
async def get_active_users():
    rows = athena_execute("""
    SELECT u.id as user_id, u.email as email, u.username as username, s.expires_at as expiration_time
FROM users u JOIN sessions s ON u.id = s.user_id WHERE s.expires_at > now();
""")
    return [ActiveUsersResponse(
        user_id=row["user_id"],
        email=row["email"],
        username=row["username"],
        expiration_time=datetime.fromisoformat(row["expiration_time"])
    ) for row in rows]

@app.get("/users/countactive", response_model=UserCount)
async def get_active_users_count():
    output = athena_execute("""
    SELECT COUNT(DISTINCT s.user_id) AS user_count FROM sessions s
    WHERE s.expires_at > now();
    """)
    count = int(output[0]["user_count"] or 0)
    return UserCount(user_count=count)

@app.get("/users/top", response_model=List[TopUsersResponse])
async def get_top_users():
    rows = athena_execute("""
    SELECT
        u.id AS user_id,
        u.email AS email,
        u.username AS username,
        COALESCE(ec.views, 0) AS views_received,
        COALESCE(ec.likes, 0) AS likes_received,
        COALESCE(ec.shares, 0) AS shares_received,
        COALESCE(cc.user_comments_received, 0) AS comments_received
    FROM users u
        LEFT JOIN event_counts_author ec ON u.id = ec.user_id
        LEFT JOIN comment_counts_received cc ON u.id = cc.user_id
    ORDER BY views_received DESC
    LIMIT 20;
""")
    return [TopUsersResponse(
        user_id=row["user_id"],
        email=row["email"],
        username=row["username"],
        views_received=int(row["views_received"] or 0),
        likes_received=int(row["likes_received"] or 0),
        shares_received=int(row["shares_received"] or 0),
        comments_received=int(row["comments_received"] or 0)
    ) for row in rows]


