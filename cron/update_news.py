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
from typing import Dict, List, Any

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

Return results in the specified JSON schema with structured news_items, trending_topics, and upcoming_events."""

# JSON Schema for structured output extraction
NEWS_ITEM_SCHEMA = {
    "type": "object",
    "properties": {
        "news_items": {
            "type": "array",
            "description": "Array of news items found during research",
            "items": {
                "type": "object",
                "properties": {
                    "headline": {
                        "type": "string",
                        "description": "Clear, engaging headline for the news item"
                    },
                    "summary": {
                        "type": "string",
                        "description": "2-3 sentence summary of the news"
                    },
                    "category": {
                        "type": "string",
                        "enum": ["music", "social", "appearance", "fan", "industry"],
                        "description": "Category of the news item"
                    },
                    "content_type": {
                        "type": "string",
                        "enum": ["article", "tweet", "tiktok", "instagram", "youtube", "official_announcement", "fan_content"],
                        "description": "Type of content source"
                    },
                    "source_url": {
                        "type": "string",
                        "description": "URL to the original source"
                    },
                    "source_name": {
                        "type": "string",
                        "description": "Name of the source (e.g., Billboard, X/Twitter)"
                    },
                    "published_date": {
                        "type": "string",
                        "description": "ISO format date when published"
                    },
                    "relevance_score": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 10,
                        "description": "Relevance score from 1-10"
                    },
                    "member_tags": {
                        "type": "array",
                        "items": {
                            "type": "string",
                            "enum": ["Daniela", "Lara", "Manon", "Megan", "Sophia", "Yoonchae", "Group"]
                        },
                        "description": "Which KATSEYE members are mentioned"
                    },
                    "media_urls": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "URLs to images or video thumbnails"
                    }
                },
                "required": ["headline", "summary", "category", "content_type", "source_name", "relevance_score"]
            }
        },
        "trending_topics": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Trending hashtags and topics related to KATSEYE"
        },
        "upcoming_events": {
            "type": "array",
            "description": "Upcoming events for KATSEYE",
            "items": {
                "type": "object",
                "properties": {
                    "title": {"type": "string"},
                    "date": {"type": "string"},
                    "location": {"type": "string"},
                    "description": {"type": "string"}
                },
                "required": ["title", "date"]
            }
        }
    },
    "required": ["news_items"]
}


async def run_research() -> dict:
    """Submit research job with structured output and wait for completion."""
    if not GREP_API_URL or not GREP_API_TOKEN:
        raise ValueError("GREP_API_URL and GREP_API_TOKEN must be set")

    logger.info(f"Starting KATSEYE research via {GREP_API_URL}")

    async with httpx.AsyncClient(timeout=900) as client:
        # Start research job with json_schema for structured output
        response = await client.post(
            f"{GREP_API_URL}/grep/research",
            headers={"Authorization": f"Bearer {GREP_API_TOKEN}"},
            json={
                "question": RESEARCH_QUESTION,
                "depth": "ultra_deep",
                "approach": "general",
                "expert_id": "katseye-news-aggregator",
                "json_schema": NEWS_ITEM_SCHEMA  # Enable structured output
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


def extract_news_items(research_data: dict) -> Dict[str, Any]:
    """Extract structured news items from research results.

    Looks for structured_output in check results payload, which is populated
    when the research job includes a json_schema parameter.
    """
    news_items: List[Dict[str, Any]] = []
    trending_topics: List[str] = []
    upcoming_events: List[Dict[str, Any]] = []

    check_results = research_data.get("check_results", [])
    final_report = research_data.get("final_report", "")

    # First, look for structured_output in check results
    for check in check_results:
        payload = check.get("payload", {})
        structured_output = payload.get("structured_output")

        if structured_output and isinstance(structured_output, dict):
            logger.info(f"Found structured_output in check: {check.get('check_name', 'unknown')}")

            # Extract news_items from structured output
            if "news_items" in structured_output:
                for item in structured_output["news_items"]:
                    news_items.append({
                        "id": str(len(news_items) + 1),
                        "headline": item.get("headline", "KATSEYE Update"),
                        "summary": item.get("summary", ""),
                        "category": item.get("category", "music"),
                        "content_type": item.get("content_type", "article"),
                        "source_url": item.get("source_url", ""),
                        "source_name": item.get("source_name", "Grep Research"),
                        "published_date": item.get("published_date", ""),
                        "relevance_score": item.get("relevance_score", 5),
                        "member_tags": item.get("member_tags", []),
                        "media_urls": item.get("media_urls", [])
                    })

            # Extract trending_topics
            if "trending_topics" in structured_output:
                trending_topics.extend(structured_output["trending_topics"])

            # Extract upcoming_events
            if "upcoming_events" in structured_output:
                upcoming_events.extend(structured_output["upcoming_events"])

    # Fallback: Try to extract from answer field if no structured_output
    if not news_items:
        logger.warning("No structured_output found, falling back to text extraction")
        item_id = 1

        for check in check_results:
            if check.get("passed") and check.get("answer"):
                answer = check["answer"]

                # Try to parse JSON from answer
                try:
                    if isinstance(answer, str) and answer.strip().startswith("{"):
                        parsed = json.loads(answer)
                        if "news_items" in parsed:
                            for item in parsed["news_items"]:
                                news_items.append({
                                    "id": str(item_id),
                                    **item
                                })
                                item_id += 1
                            continue
                except json.JSONDecodeError:
                    pass

                # Plain text extraction (last resort)
                if len(answer) > 100:
                    news_items.append({
                        "id": str(item_id),
                        "headline": check.get("check_name", f"KATSEYE Update #{item_id}")[:100],
                        "summary": answer[:500],
                        "category": "music",
                        "content_type": "article",
                        "source_url": "",
                        "source_name": "Grep Research",
                        "published_date": datetime.now(timezone.utc).isoformat(),
                        "relevance_score": 6,
                        "member_tags": ["Group"],
                        "media_urls": []
                    })
                    item_id += 1

    # If still no items, create a summary from final_report
    if not news_items and final_report:
        logger.info("Creating summary item from final_report")
        news_items.append({
            "id": "1",
            "headline": "Latest KATSEYE News Roundup",
            "summary": final_report[:500] if len(final_report) > 500 else final_report,
            "category": "music",
            "content_type": "article",
            "source_url": "",
            "source_name": "Grep Research",
            "published_date": datetime.now(timezone.utc).isoformat(),
            "relevance_score": 10,
            "member_tags": ["Group"],
            "media_urls": []
        })

    # Sort by relevance score (highest first)
    news_items.sort(key=lambda x: x.get("relevance_score", 0), reverse=True)

    # Default trending topics if none found
    if not trending_topics:
        trending_topics = ["#KATSEYE", "#EYEKON", "#PopStarAcademy", "#Touch"]

    logger.info(f"Extracted {len(news_items)} news items, {len(trending_topics)} topics, {len(upcoming_events)} events")

    return {
        "news_items": news_items,
        "trending_topics": trending_topics,
        "upcoming_events": upcoming_events
    }


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
        "trending_topics": data.get("trending_topics", []),
        "upcoming_events": data.get("upcoming_events", []),
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
        # Run research with structured output
        research_data = await run_research()

        # Extract news items from structured output
        extracted_data = extract_news_items(research_data)
        logger.info(f"Extracted {len(extracted_data['news_items'])} news items")

        # Prepare output data
        output_data = {
            "job_id": research_data.get("job_id"),
            **extracted_data
        }

        # Save to MinIO
        save_to_minio(output_data)

        logger.info("=" * 50)
        logger.info("KATSEYE News Update Complete!")
        logger.info(f"News items: {len(extracted_data['news_items'])}")
        logger.info(f"Trending topics: {extracted_data['trending_topics']}")
        logger.info(f"Upcoming events: {len(extracted_data['upcoming_events'])}")
        logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Job failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    asyncio.run(main())
