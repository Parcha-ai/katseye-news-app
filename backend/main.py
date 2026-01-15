"""
KATSEYE News Aggregator - FastAPI Backend
Serves news data from MinIO storage, updated daily by Grep research.
"""
import os
import json
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
import httpx

app = FastAPI(title="KATSEYE News", version="1.0.0")

# MinIO configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "")
MINIO_ACCESS_KEY = os.getenv("ACCESS_KEY", "")
MINIO_SECRET_KEY = os.getenv("SECRET_KEY", "")
BUCKET_NAME = "katseye-news"


class NewsItem(BaseModel):
    id: str
    headline: str
    summary: str
    category: str
    source_url: Optional[str] = None
    source_name: Optional[str] = None
    published_date: Optional[str] = None
    relevance_score: Optional[int] = None


class NewsResponse(BaseModel):
    last_updated: str
    news_items: list[NewsItem]
    trending_topics: list[str] = []


# Demo data for when MinIO isn't configured yet
DEMO_NEWS = {
    "last_updated": datetime.utcnow().isoformat(),
    "news_items": [
        {
            "id": "1",
            "headline": "KATSEYE Continues Global Rise",
            "summary": "The groundbreaking K-pop group formed through Netflix's Pop Star Academy continues to captivate audiences worldwide with their unique blend of talent and personality.",
            "category": "music",
            "source_name": "Demo News",
            "relevance_score": 10
        },
        {
            "id": "2",
            "headline": "Members Share Behind-the-Scenes Moments",
            "summary": "Daniela, Lara, Manon, Megan, Sophia, and Yoonchae give fans a glimpse into their daily lives through social media updates.",
            "category": "social",
            "source_name": "Demo News",
            "relevance_score": 9
        },
        {
            "id": "3",
            "headline": "Debut Single Reaches New Milestone",
            "summary": "KATSEYE's debut continues to climb charts as the group gains recognition across music streaming platforms.",
            "category": "music",
            "source_name": "Demo News",
            "relevance_score": 8
        }
    ],
    "trending_topics": ["#KATSEYE", "#EYEKON", "#PopStarAcademy"]
}


async def fetch_from_minio(key: str) -> Optional[dict]:
    """Fetch JSON data from MinIO bucket."""
    if not MINIO_ENDPOINT:
        return None

    try:
        # Use S3-compatible URL pattern
        url = f"{MINIO_ENDPOINT}/{BUCKET_NAME}/{key}"
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get(url)
            if response.status_code == 200:
                return response.json()
    except Exception as e:
        print(f"MinIO fetch error: {e}")
    return None


@app.get("/api/news", response_model=NewsResponse)
async def get_latest_news():
    """Get the latest KATSEYE news."""
    # Try to fetch from MinIO first
    news_data = await fetch_from_minio("latest.json")

    if news_data:
        return NewsResponse(**news_data)

    # Return demo data if MinIO not configured or no data yet
    return NewsResponse(**DEMO_NEWS)


@app.get("/api/news/archive/{date}")
async def get_archived_news(date: str):
    """Get archived news by date (YYYY-MM-DD format)."""
    news_data = await fetch_from_minio(f"archive/{date}.json")

    if news_data:
        return news_data

    raise HTTPException(status_code=404, detail=f"No archived news for {date}")


@app.get("/health")
async def health_check():
    """Health check endpoint for Northflank."""
    return {
        "status": "healthy",
        "service": "katseye-news",
        "timestamp": datetime.utcnow().isoformat(),
        "minio_configured": bool(MINIO_ENDPOINT)
    }


@app.get("/api/status")
async def status():
    """API status with configuration info."""
    return {
        "status": "ok",
        "version": "1.0.0",
        "minio_endpoint": MINIO_ENDPOINT[:20] + "..." if MINIO_ENDPOINT else "not configured",
        "bucket": BUCKET_NAME
    }


# Serve static files (React build)
if os.path.exists("/app/static"):
    app.mount("/assets", StaticFiles(directory="/app/static/assets"), name="assets")

    @app.get("/")
    async def serve_frontend():
        return FileResponse("/app/static/index.html")

    @app.get("/{path:path}")
    async def serve_frontend_routes(path: str):
        # For SPA routing, serve index.html for non-API routes
        if not path.startswith("api/") and not path.startswith("health"):
            return FileResponse("/app/static/index.html")
        raise HTTPException(status_code=404)
