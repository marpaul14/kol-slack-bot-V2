"""
scraper.py — Scrapes social media profiles.

Hybrid backend:
- ScrapeCreators API for profile data on all platforms, and recent posts on
  TikTok, YouTube, Instagram.
- Apify (apidojo/tweet-scraper) for X/Twitter recent tweets, since
  ScrapeCreators' Twitter user-tweets endpoint only returns popular tweets,
  not the latest ones.

Returns up to 5 recent posts per profile for niche detection by AI.
Supported platforms: X/Twitter, TikTok, YouTube, Instagram.
"""

import os
import logging
from urllib.parse import urlparse

import requests

logger = logging.getLogger(__name__)

try:
    from apify_client import ApifyClient
    APIFY_AVAILABLE = True
except ImportError:
    APIFY_AVAILABLE = False
    logger.warning("[Scraper] apify-client not installed. Run: pip install apify-client")

SC_API_KEY = os.environ.get("SCRAPECREATORS_API_KEY", "")
SC_BASE_URL = "https://api.scrapecreators.com"
SC_TIMEOUT = 30

APIFY_TOKEN = os.environ.get("APIFY_API_KEY", "")
TWITTER_ACTOR = "apidojo/tweet-scraper"

MAX_POSTS = 5


def detect_platform(url: str) -> str:
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
    Scrape a social media profile.

    Returns dict with:
      - handle: @username
      - platform: X, TikTok, YouTube, Instagram
      - followers: follower count string
      - location: profile location
      - raw_bio: profile bio
      - recent_posts: list of up to 5 recent post texts
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

    if not SC_API_KEY:
        logger.warning("[Scraper] SCRAPECREATORS_API_KEY not set, using fallback")
        result.update(_extract_handle_from_url(url, platform))
        result["link_status"] = "Limited"
        return result

    try:
        if platform == "X":
            result.update(_scrape_x(url))
        elif platform == "TikTok":
            result.update(_scrape_tiktok(url))
        elif platform == "YouTube":
            result.update(_scrape_youtube(url))
        elif platform == "Instagram":
            result.update(_scrape_instagram(url))
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


def _sc_get(path: str, params: dict) -> dict:
    """GET against ScrapeCreators with x-api-key header."""
    resp = requests.get(
        f"{SC_BASE_URL}{path}",
        params=params,
        headers={"x-api-key": SC_API_KEY},
        timeout=SC_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()


def _scrape_x(url: str) -> dict:
    """X/Twitter: ScrapeCreators for profile + Apify for recent tweets.

    SC's /v1/twitter/user-tweets only returns popular tweets, so we keep
    Apify's apidojo/tweet-scraper for chronological recent tweets.
    """
    result = {"recent_posts": []}
    handle = _extract_x_handle(url)
    result["handle"] = handle

    if not handle:
        result["link_status"] = "Invalid URL"
        return result

    clean_handle = handle.lstrip("@")

    try:
        data = _sc_get("/v1/twitter/profile", {"handle": clean_handle})
        result["followers"] = _format_number(
            data.get("followers_count")
            or data.get("normal_followers_count")
            or 0
        )
        result["raw_bio"] = data.get("description") or ""
        result["location"] = data.get("location") or ""
        result["link_status"] = "OK"
    except Exception as e:
        logger.error(f"[Scraper] SC profile error for @{clean_handle}: {e}")
        result["link_status"] = "Limited"

    if APIFY_AVAILABLE and APIFY_TOKEN:
        try:
            client = ApifyClient(APIFY_TOKEN)
            run_input = {"author": clean_handle, "maxItems": MAX_POSTS}
            logger.info(f"[Scraper] Apify tweet-scraper for @{clean_handle}")
            run = client.actor(TWITTER_ACTOR).call(run_input=run_input, timeout_secs=120)
            items = list(client.dataset(run["defaultDatasetId"]).iterate_items())

            for item in items:
                text = (item.get("text")
                        or item.get("full_text")
                        or item.get("tweetText")
                        or item.get("content")
                        or "")
                if text and len(text) > 10 and len(result["recent_posts"]) < MAX_POSTS:
                    result["recent_posts"].append(text)
        except Exception as e:
            logger.error(f"[Scraper] Apify tweets error for @{clean_handle}: {e}")
    else:
        logger.warning("[Scraper] Apify not configured; skipping recent X tweets")

    return result


def _scrape_tiktok(url: str) -> dict:
    """TikTok: ScrapeCreators profile + profile-videos."""
    result = {"recent_posts": []}
    handle = _extract_handle_from_path(url, "@")
    result["handle"] = f"@{handle}" if handle else ""

    if not handle:
        result["link_status"] = "Invalid URL"
        return result

    try:
        profile = _sc_get("/v1/tiktok/profile", {"handle": handle})
        # SC TikTok profile responses commonly nest user data under "user"/"userInfo"/"stats"
        user = profile.get("user") or profile.get("userInfo") or profile
        stats = profile.get("stats") or profile.get("statsV2") or user.get("stats") or {}

        followers = (stats.get("followerCount")
                     or stats.get("followers")
                     or user.get("followerCount")
                     or profile.get("followerCount")
                     or 0)
        result["followers"] = _format_number(followers)
        result["raw_bio"] = (user.get("signature")
                             or profile.get("signature")
                             or user.get("bio")
                             or "")
        result["location"] = user.get("region") or profile.get("region") or ""
        result["link_status"] = "OK"
    except Exception as e:
        logger.error(f"[Scraper] SC TikTok profile error for @{handle}: {e}")
        result["link_status"] = "Limited"

    try:
        videos = _sc_get("/v2/tiktok/profile-videos", {"handle": handle})
        items = videos.get("aweme_list") or videos.get("videos") or videos.get("items") or []
        for item in items:
            text = (item.get("desc")
                    or item.get("description")
                    or item.get("text")
                    or "")
            if text and len(text) > 10 and len(result["recent_posts"]) < MAX_POSTS:
                result["recent_posts"].append(text)
    except Exception as e:
        logger.error(f"[Scraper] SC TikTok videos error for @{handle}: {e}")

    return result


def _scrape_youtube(url: str) -> dict:
    """YouTube: ScrapeCreators channel + channel-videos."""
    result = {"recent_posts": []}
    handle = _extract_handle_from_path(url, "@")
    result["handle"] = f"@{handle}" if handle else ""

    params = {"handle": handle} if handle else {"url": url}

    try:
        channel = _sc_get("/v1/youtube/channel", params)
        result["followers"] = _format_number(
            channel.get("subscriberCount")
            or channel.get("subscribers")
            or 0
        )
        result["raw_bio"] = channel.get("description") or ""
        result["location"] = channel.get("country") or ""
        result["link_status"] = "OK"
    except Exception as e:
        logger.error(f"[Scraper] SC YouTube channel error: {e}")
        result["link_status"] = "Limited"

    try:
        videos = _sc_get("/v1/youtube/channel-videos", params)
        items = videos.get("videos") or videos.get("items") or []
        for item in items:
            text = item.get("title") or ""
            desc = item.get("description") or ""
            combined = f"{text} {desc}".strip()
            if combined and len(combined) > 5 and len(result["recent_posts"]) < MAX_POSTS:
                result["recent_posts"].append(combined)
    except Exception as e:
        logger.error(f"[Scraper] SC YouTube videos error: {e}")

    return result


def _scrape_instagram(url: str) -> dict:
    """Instagram: ScrapeCreators profile + posts."""
    result = {"recent_posts": []}
    handle = _extract_handle_from_path(url, "")
    result["handle"] = f"@{handle}" if handle else ""

    if not handle:
        result["link_status"] = "Invalid URL"
        return result

    try:
        profile = _sc_get("/v1/instagram/profile", {"handle": handle})
        user = profile.get("user") or profile.get("data") or profile
        followers = (user.get("follower_count")
                     or user.get("followersCount")
                     or user.get("followers_count")
                     or profile.get("follower_count")
                     or 0)
        result["followers"] = _format_number(followers)
        result["raw_bio"] = (user.get("biography")
                             or user.get("bio")
                             or profile.get("biography")
                             or "")
        result["location"] = (user.get("city_name")
                              or user.get("location")
                              or profile.get("city_name")
                              or "")
        result["link_status"] = "OK"
    except Exception as e:
        logger.error(f"[Scraper] SC Instagram profile error for @{handle}: {e}")
        result["link_status"] = "Limited"

    try:
        posts = _sc_get("/v1/instagram/posts", {"handle": handle})
        items = posts.get("items") or posts.get("posts") or posts.get("data") or []
        for item in items:
            caption = item.get("caption")
            if isinstance(caption, dict):
                caption = caption.get("text", "")
            caption = caption or item.get("text") or ""
            if caption and len(caption) > 10 and len(result["recent_posts"]) < MAX_POSTS:
                result["recent_posts"].append(caption)
    except Exception as e:
        logger.error(f"[Scraper] SC Instagram posts error for @{handle}: {e}")

    return result


# ─── Fallback / Helper Functions ─────────────────────────────────────────────

def _extract_handle_from_url(url: str, platform: str) -> dict:
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
    path = urlparse(url).path.strip("/").split("/")
    if path:
        handle = path[0].lstrip("@")
        if handle and handle.lower() not in ["home", "explore", "search", "settings", "i", "intent"]:
            return f"@{handle}"
    return ""


def _extract_handle_from_path(url: str, prefix: str) -> str:
    path = urlparse(url).path.strip("/").split("/")
    for segment in path:
        if prefix and segment.startswith(prefix):
            return segment.lstrip(prefix)
        elif segment and not segment.startswith("?"):
            return segment
    return ""


def _format_number(n) -> str:
    try:
        n = int(n)
    except (ValueError, TypeError):
        return str(n) if n else ""

    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n/1_000:.1f}K"
    return str(n)
