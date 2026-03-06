"""
scraper.py — Scrapes social media profiles using Apify.

Uses Apify actors to get:
- Profile info (followers, bio, location)
- 10 recent posts (for niche detection by AI)

Supported platforms: X/Twitter, TikTok, YouTube, Instagram
"""

import os
import re
import logging
import time
from typing import Optional
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# Check if Apify is available
try:
    from apify_client import ApifyClient
    APIFY_AVAILABLE = True
except ImportError:
    APIFY_AVAILABLE = False
    logger.warning("[Scraper] apify-client not installed. Run: pip install apify-client")

# Apify configuration
APIFY_TOKEN = os.environ.get("APIFY_API_KEY", "")

# Apify Actor IDs
TWITTER_ACTOR = "web.harvester/twitter-scraper"  # Gets profiles + tweets
TIKTOK_ACTOR = "clockworks/tiktok-scraper"
YOUTUBE_ACTOR = "streamers/youtube-channel-scraper"
INSTAGRAM_ACTOR = "apify/instagram-scraper"

MAX_POSTS = 10  # Scrape 10 recent posts for niche detection


def detect_platform(url: str) -> str:
    """Detect social media platform from URL."""
    host = urlparse(url).netloc.lower()
    if "twitter.com" in host or "x.com" in host:
        return "X"
    if "tiktok.com" in host:
        return "TikTok"
    if "youtube.com" in host or "youtu.be" in host:
        return "YouTube"
    if "instagram.com" in host:
        return "Instagram"
    return "Other"


def scrape_profile(url: str) -> dict:
    """
    Scrape a social media profile using Apify.
    
    Returns dict with:
      - handle: @username
      - platform: X, TikTok, YouTube, Instagram
      - followers: follower count string
      - location: profile location
      - raw_bio: profile bio
      - recent_posts: list of up to 10 recent post texts
      - link_status: OK, Limited, Error, No Link
    """
    if not url:
        return {"link_status": "No Link"}

    platform = detect_platform(url)
    result = {
        "platform": platform,
        "profile_url": url,
        "link_status": "OK",
        "recent_posts": [],
    }

    logger.info(f"[Scraper] Scraping {platform}: {url}")

    # Check if Apify is configured
    if not APIFY_AVAILABLE or not APIFY_TOKEN:
        logger.warning("[Scraper] Apify not configured, using fallback")
        result.update(_extract_handle_from_url(url, platform))
        result["link_status"] = "Limited"
        return result

    try:
        if platform == "X":
            result.update(_scrape_x_apify(url))
        elif platform == "TikTok":
            result.update(_scrape_tiktok_apify(url))
        elif platform == "YouTube":
            result.update(_scrape_youtube_apify(url))
        elif platform == "Instagram":
            result.update(_scrape_instagram_apify(url))
        else:
            result.update(_extract_handle_from_url(url, platform))
            result["link_status"] = "Limited"
            
    except Exception as e:
        logger.error(f"[Scraper] Error scraping {url}: {e}")
        result.update(_extract_handle_from_url(url, platform))
        result["link_status"] = "Error"

    logger.info(f"[Scraper] Result: handle={result.get('handle')}, "
                f"followers={result.get('followers')}, "
                f"posts={len(result.get('recent_posts', []))}")
    return result


def _scrape_x_apify(url: str) -> dict:
    """Scrape X/Twitter profile using Apify."""
    result = {"recent_posts": []}
    handle = _extract_x_handle(url)
    result["handle"] = handle
    
    if not handle:
        result["link_status"] = "Invalid URL"
        return result

    clean_handle = handle.lstrip("@")
    
    try:
        client = ApifyClient(APIFY_TOKEN)
        
        # Run the Twitter scraper
        run_input = {
            "handles": [clean_handle],
            "tweetsDesired": MAX_POSTS,
            "proxyConfig": {"useApifyProxy": True},
        }
        
        logger.info(f"[Scraper] Running Apify actor for @{clean_handle}")
        run = client.actor(TWITTER_ACTOR).call(run_input=run_input, timeout_secs=120)
        
        # Get results from dataset
        items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
        
        if items:
            profile = items[0]  # First item is usually profile data
            
            # Extract profile info
            result["followers"] = _format_number(profile.get("followersCount", 0))
            result["raw_bio"] = profile.get("description", "")
            result["location"] = profile.get("location", "")
            
            # Extract recent tweets
            tweets = profile.get("tweets", []) or items[1:MAX_POSTS+1]
            for tweet in tweets[:MAX_POSTS]:
                text = tweet.get("text") or tweet.get("full_text", "")
                if text and len(text) > 10:
                    result["recent_posts"].append(text)
            
            result["link_status"] = "OK"
            logger.info(f"[Scraper] Apify success: {result['followers']} followers, "
                       f"{len(result['recent_posts'])} posts")
        else:
            logger.warning(f"[Scraper] No data returned for @{clean_handle}")
            result["link_status"] = "Limited"
            
    except Exception as e:
        logger.error(f"[Scraper] Apify error for @{clean_handle}: {e}")
        result["link_status"] = "Limited"
    
    return result


def _scrape_tiktok_apify(url: str) -> dict:
    """Scrape TikTok profile using Apify."""
    result = {"recent_posts": []}
    handle = _extract_handle_from_path(url, "@")
    result["handle"] = f"@{handle}" if handle else ""
    
    try:
        client = ApifyClient(APIFY_TOKEN)
        
        run_input = {
            "profiles": [url],
            "resultsPerPage": MAX_POSTS,
            "proxyConfiguration": {"useApifyProxy": True},
        }
        
        run = client.actor(TIKTOK_ACTOR).call(run_input=run_input, timeout_secs=120)
        items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
        
        if items:
            for item in items:
                if item.get("authorMeta"):
                    result["followers"] = _format_number(item["authorMeta"].get("fans", 0))
                    result["raw_bio"] = item["authorMeta"].get("signature", "")
                
                text = item.get("text", "")
                if text and len(text) > 10 and len(result["recent_posts"]) < MAX_POSTS:
                    result["recent_posts"].append(text)
            
            result["link_status"] = "OK"
        else:
            result["link_status"] = "Limited"
            
    except Exception as e:
        logger.error(f"[Scraper] TikTok Apify error: {e}")
        result["link_status"] = "Limited"
    
    return result


def _scrape_youtube_apify(url: str) -> dict:
    """Scrape YouTube channel using Apify."""
    result = {"recent_posts": []}
    handle = _extract_handle_from_path(url, "@")
    result["handle"] = f"@{handle}" if handle else ""
    
    try:
        client = ApifyClient(APIFY_TOKEN)
        
        run_input = {
            "startUrls": [{"url": url}],
            "maxResults": MAX_POSTS,
        }
        
        run = client.actor(YOUTUBE_ACTOR).call(run_input=run_input, timeout_secs=120)
        items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
        
        if items:
            for item in items:
                if item.get("subscriberCount"):
                    result["followers"] = item["subscriberCount"]
                if item.get("description"):
                    result["raw_bio"] = item["description"]
                
                title = item.get("title", "")
                if title and len(title) > 5 and len(result["recent_posts"]) < MAX_POSTS:
                    result["recent_posts"].append(title)
            
            result["link_status"] = "OK"
        else:
            result["link_status"] = "Limited"
            
    except Exception as e:
        logger.error(f"[Scraper] YouTube Apify error: {e}")
        result["link_status"] = "Limited"
    
    return result


def _scrape_instagram_apify(url: str) -> dict:
    """Scrape Instagram profile using Apify."""
    result = {"recent_posts": []}
    handle = _extract_handle_from_path(url, "")
    result["handle"] = f"@{handle}" if handle else ""
    
    try:
        client = ApifyClient(APIFY_TOKEN)
        
        run_input = {
            "directUrls": [url],
            "resultsLimit": MAX_POSTS,
        }
        
        run = client.actor(INSTAGRAM_ACTOR).call(run_input=run_input, timeout_secs=120)
        items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
        
        if items:
            for item in items:
                if item.get("followersCount"):
                    result["followers"] = _format_number(item["followersCount"])
                if item.get("biography"):
                    result["raw_bio"] = item["biography"]
                
                caption = item.get("caption", "")
                if caption and len(caption) > 10 and len(result["recent_posts"]) < MAX_POSTS:
                    result["recent_posts"].append(caption)
            
            result["link_status"] = "OK"
        else:
            result["link_status"] = "Limited"
            
    except Exception as e:
        logger.error(f"[Scraper] Instagram Apify error: {e}")
        result["link_status"] = "Limited"
    
    return result


# ─── Fallback / Helper Functions ─────────────────────────────────────────────

def _extract_handle_from_url(url: str, platform: str) -> dict:
    """Extract handle from URL as fallback when Apify fails."""
    if platform == "X":
        handle = _extract_x_handle(url)
    elif platform in ("TikTok", "YouTube"):
        handle = _extract_handle_from_path(url, "@")
        handle = f"@{handle}" if handle else ""
    elif platform == "Instagram":
        handle = _extract_handle_from_path(url, "")
        handle = f"@{handle}" if handle else ""
    else:
        handle = ""
    
    return {"handle": handle}


def _extract_x_handle(url: str) -> str:
    """Extract Twitter/X handle from URL."""
    path = urlparse(url).path.strip("/").split("/")
    if path:
        handle = path[0].lstrip("@")
        if handle and handle.lower() not in ["home", "explore", "search", "settings", "i", "intent"]:
            return f"@{handle}"
    return ""


def _extract_handle_from_path(url: str, prefix: str) -> str:
    """Extract handle from URL path."""
    path = urlparse(url).path.strip("/").split("/")
    for segment in path:
        if prefix and segment.startswith(prefix):
            return segment.lstrip(prefix)
        elif segment and not segment.startswith("?"):
            return segment
    return ""


def _format_number(n) -> str:
    """Format number with K/M suffix."""
    try:
        n = int(n)
    except (ValueError, TypeError):
        return str(n) if n else ""
    
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n/1_000:.1f}K"
    return str(n)
