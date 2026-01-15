#!/usr/bin/env python3
"""
One-time seed job to populate MinIO with initial KATSEYE news data.
Run this when first deploying to seed the news feed.
"""
import os
import json
from datetime import datetime, timezone

import boto3
from botocore.config import Config

# MinIO configuration from environment
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "")
MINIO_ACCESS_KEY = os.getenv("ACCESS_KEY", "")
MINIO_SECRET_KEY = os.getenv("SECRET_KEY", "")
BUCKET_NAME = "katseye-news"

# Initial seed data
SEED_NEWS = {
    "last_updated": datetime.now(timezone.utc).isoformat(),
    "news_items": [
        {
            "id": "1",
            "headline": "KATSEYE Releases New Single 'Touch'",
            "summary": "The global K-pop sensation KATSEYE has dropped their highly anticipated single 'Touch', showcasing their signature blend of pop perfection and powerful vocals. The track has already climbed streaming charts worldwide.",
            "category": "music",
            "source_name": "Grep Research",
            "relevance_score": 10
        },
        {
            "id": "2",
            "headline": "EYEKON Fandom Celebrates 1 Million Strong",
            "summary": "The official KATSEYE fandom, known as EYEKON, has reached a milestone of 1 million members across social platforms. Fans are celebrating with special projects and trending hashtags.",
            "category": "fan",
            "source_name": "Grep Research",
            "relevance_score": 9
        },
        {
            "id": "3",
            "headline": "Daniela and Sophia Share Dance Practice Video",
            "summary": "Members Daniela and Sophia surprised fans with an impromptu dance practice video on social media, showcasing their incredible synchronization and stage presence.",
            "category": "social",
            "source_name": "Grep Research",
            "relevance_score": 8
        },
        {
            "id": "4",
            "headline": "KATSEYE Confirmed for Major Music Festival",
            "summary": "KATSEYE has been announced as headliners for an upcoming major music festival, marking their biggest live performance since debut. Tickets are expected to sell out quickly.",
            "category": "appearance",
            "source_name": "Grep Research",
            "relevance_score": 9
        },
        {
            "id": "5",
            "headline": "Netflix's Pop Star Academy Documentary Trending",
            "summary": "The Netflix documentary 'Pop Star Academy: KATSEYE' that chronicles the group's formation is seeing renewed interest, with new viewers discovering the journey of Daniela, Lara, Manon, Megan, Sophia, and Yoonchae.",
            "category": "industry",
            "source_name": "Grep Research",
            "relevance_score": 7
        }
    ],
    "trending_topics": ["#KATSEYE", "#EYEKON", "#Touch", "#PopStarAcademy", "#HYBE"],
    "generated_at": datetime.now(timezone.utc).isoformat()
}


def main():
    print("=" * 50)
    print("KATSEYE News Seed Job Starting")
    print("=" * 50)

    if not MINIO_ENDPOINT or not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
        print("ERROR: MinIO credentials not configured")
        print(f"  MINIO_ENDPOINT: {'set' if MINIO_ENDPOINT else 'NOT SET'}")
        print(f"  ACCESS_KEY: {'set' if MINIO_ACCESS_KEY else 'NOT SET'}")
        print(f"  SECRET_KEY: {'set' if MINIO_SECRET_KEY else 'NOT SET'}")
        return

    print(f"Connecting to MinIO at {MINIO_ENDPOINT}")

    # Create S3 client for MinIO
    s3 = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name='us-east-1',
        config=Config(signature_version='s3v4')
    )

    # Ensure bucket exists
    try:
        s3.head_bucket(Bucket=BUCKET_NAME)
        print(f"Bucket {BUCKET_NAME} exists")
    except Exception:
        print(f"Creating bucket {BUCKET_NAME}")
        s3.create_bucket(Bucket=BUCKET_NAME)

    # Prepare data
    json_data = json.dumps(SEED_NEWS, indent=2)

    # Save as latest.json
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key="latest.json",
        Body=json_data.encode('utf-8'),
        ContentType="application/json"
    )
    print("Saved latest.json")

    # Archive by date
    date_key = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=f"archive/{date_key}.json",
        Body=json_data.encode('utf-8'),
        ContentType="application/json"
    )
    print(f"Saved archive/{date_key}.json")

    print("=" * 50)
    print("Seed Data Upload Complete!")
    print(f"Uploaded {len(SEED_NEWS['news_items'])} news items")
    print("=" * 50)


if __name__ == "__main__":
    main()
