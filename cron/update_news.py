#!/usr/bin/env python3
"""
Daily cron job to fetch KATSEYE news via Grep Research API.
Runs on Northflank cron schedule: 0 6 * * * (6 AM UTC daily)

Environment variables required:
- GREP_API_URL: Grep API endpoint (e.g., https://api.grep.ing)
- GREP_API_TOKEN: Admin token for research API
- MINIO_ENDPOINT: MinIO endpoint URL
- ACCESS_KEY: MinIO access key
- SECRET_KEY: MinIO secret key
"""
import os
import json
import asyncio
import logging
from datetime import datetime, timezone

import httpx
import boto3
from botocore.config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration from environment
GREP_API_URL = os.getenv("GREP_API_URL", "")
GREP_API_TOKEN = os.getenv("GREP_API_TOKEN", "")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "")
MINIO_ACCESS_KEY = os.getenv("ACCESS_KEY", "")
MINIO_SECRET_KEY = os.getenv("SECRET_KEY", "")
BUCKET_NAME = "katseye-news"

# Research configuration
RESEARCH_QUESTION = """What are the latest news, trending topics, social media moments, and upcoming events for KATSEYE (the K-pop group formed through Netflix's Pop Star Academy)?

Focus on:
- Recent music releases, chart performance, streaming milestones
- Social media updates from members (Daniela, Lara, Manon, Megan, Sophia, Yoonchae)
- TV appearances, interviews, variety shows
- Concert announcements, tour dates
- Fan community highlights

Return structured data with headlines, summaries, categories (music/social/appearance/fan/industry), source names, and relevance scores (1-10)."""


async def run_research() -> dict:
    """Submit research job and wait for completion."""
    if not GREP_API_URL or not GREP_API_TOKEN:
        raise ValueError("GREP_API_URL and GREP_API_TOKEN must be set")

    logger.info(f"Starting KATSEYE research via {GREP_API_URL}")

    async with httpx.AsyncClient(timeout=900) as client:
        # Start research job
        response = await client.post(
            f"{GREP_API_URL}/grep/research",
            headers={"Authorization": f"Bearer {GREP_API_TOKEN}"},
            json={
                "question": RESEARCH_QUESTION,
                "depth": "deep",
                "approach": "general",
                "expert_id": "katseye-news-aggregator"
            }
        )
        response.raise_for_status()
        job_data = response.json()
        job_id = job_data["job_id"]
        logger.info(f"Research job started: {job_id}")

        # Poll for completion (max 15 minutes)
        for i in range(90):
            await asyncio.sleep(10)
            result = await client.get(
                f"{GREP_API_URL}/grep/research/{job_id}",
                headers={"Authorization": f"Bearer {GREP_API_TOKEN}"},
                params={"include_check_results": "true"}
            )
            result.raise_for_status()
            data = result.json()

            status = data.get("status", "unknown")
            logger.info(f"Poll {i+1}/90: status={status}")

            if status == "complete":
                logger.info("Research completed successfully")
                return data
            if status == "failed":
                raise Exception(f"Research failed: {data}")

        raise TimeoutError("Research did not complete in 15 minutes")


def extract_news_items(research_data: dict) -> list:
    """Extract structured news items from research results."""
    news_items = []

    # Try to extract from check_results or final_report
    check_results = research_data.get("check_results", [])
    final_report = research_data.get("final_report", "")

    # Generate news items from research
    # This is a simplified extraction - in production, use LLM to parse
    item_id = 1

    # Add items from check results
    for check in check_results:
        if check.get("passed") and check.get("answer"):
            answer = check["answer"]
            if len(answer) > 50:
                news_items.append({
                    "id": str(item_id),
                    "headline": check.get("check_name", f"KATSEYE Update #{item_id}")[:100],
                    "summary": answer[:500],
                    "category": "music",
                    "source_name": "Grep Research",
                    "relevance_score": 8
                })
                item_id += 1

    # If we have a final report but no items, create a summary item
    if not news_items and final_report:
        news_items.append({
            "id": "1",
            "headline": "Latest KATSEYE News Roundup",
            "summary": final_report[:500] if len(final_report) > 500 else final_report,
            "category": "music",
            "source_name": "Grep Research",
            "relevance_score": 10
        })

    return news_items


def save_to_minio(data: dict):
    """Save research results to MinIO bucket."""
    if not MINIO_ENDPOINT or not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
        raise ValueError("MinIO credentials must be set")

    logger.info(f"Connecting to MinIO at {MINIO_ENDPOINT}")

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
        logger.info(f"Bucket {BUCKET_NAME} exists")
    except Exception:
        logger.info(f"Creating bucket {BUCKET_NAME}")
        s3.create_bucket(Bucket=BUCKET_NAME)

    # Prepare news data
    now = datetime.now(timezone.utc)
    news_data = {
        "last_updated": now.isoformat(),
        "news_items": data.get("news_items", []),
        "trending_topics": ["#KATSEYE", "#EYEKON", "#PopStarAcademy"],
        "research_job_id": data.get("job_id"),
        "generated_at": now.isoformat()
    }

    json_data = json.dumps(news_data, indent=2)

    # Save as latest.json
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key="latest.json",
        Body=json_data.encode('utf-8'),
        ContentType="application/json"
    )
    logger.info("Saved latest.json")

    # Archive by date
    date_key = now.strftime("%Y-%m-%d")
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=f"archive/{date_key}.json",
        Body=json_data.encode('utf-8'),
        ContentType="application/json"
    )
    logger.info(f"Saved archive/{date_key}.json")


async def main():
    """Main entry point."""
    logger.info("=" * 50)
    logger.info("KATSEYE News Update Job Starting")
    logger.info("=" * 50)

    try:
        # Run research
        research_data = await run_research()

        # Extract news items
        news_items = extract_news_items(research_data)
        logger.info(f"Extracted {len(news_items)} news items")

        # Prepare output data
        output_data = {
            "job_id": research_data.get("job_id"),
            "news_items": news_items
        }

        # Save to MinIO
        save_to_minio(output_data)

        logger.info("=" * 50)
        logger.info("KATSEYE News Update Complete!")
        logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Job failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    asyncio.run(main())
