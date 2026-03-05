"""
scraper.py — Scrapes social media profile data from a URL.

Supports: X/Twitter, TikTok, YouTube, Instagram.
Uses lightweight HTTP requests + BeautifulSoup where possible.
Falls back to meta-tag parsing for JS-heavy pages.
"""

import re
import logging
import requests
from bs4 import BeautifulSoup
from typing import Optional
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

TIMEOUT = 15

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


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
    Scrape a social media profile URL and return a dict with:
      handle, platform, followers, language, location, bio, link_status
    """
    if not url:
        return {"link_status": "Missing"}

    platform = detect_platform(url)
    result = {
        "platform":    platform,
        "profile_url": url,
        "link_status": "OK",
    }

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
            result.update(_scrape_generic(url))
    except requests.exceptions.Timeout:
        result["link_status"] = "Timeout"
        logger.warning(f"Timeout scraping {url}")
    except requests.exceptions.ConnectionError:
        result["link_status"] = "Unreachable"
        logger.warning(f"Connection error scraping {url}")
    except Exception as e:
        result["link_status"] = "Error"
        logger.warning(f"Error scraping {url}: {e}")

    return result


# ─── Platform scrapers ───────────────────────────────────────────────────────

def _scrape_x(url: str) -> dict:
    """
    Scrape X/Twitter via Nitter (public proxy).
    Falls back to direct meta-tag scrape if Nitter is unavailable.
    """
    handle = _extract_x_handle(url)
    result = {"handle": handle}

    # Try Nitter instances
    nitter_instances = [
        "https://nitter.privacydev.net",
        "https://nitter.poast.org",
        "https://nitter.1d4.us",
    ]

    for base in nitter_instances:
        nitter_url = f"{base}/{handle.lstrip('@')}"
        try:
            resp = requests.get(nitter_url, headers=HEADERS, timeout=TIMEOUT)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "html.parser")

                # Followers
                followers_el = soup.select_one(".followers .profile-stat-num")
                if followers_el:
                    result["followers"] = _clean_number(followers_el.get_text())

                # Bio
                bio_el = soup.select_one(".profile-bio")
                if bio_el:
                    result["raw_bio"] = bio_el.get_text(" ", strip=True)

                # Location
                loc_el = soup.select_one(".profile-location")
                if loc_el:
                    result["location"] = loc_el.get_text(strip=True)

                result["link_status"] = "OK"
                return result
        except Exception:
            continue

    # Nitter unavailable — mark but still return handle
    result["link_status"] = "Limited"
    return result


def _scrape_tiktok(url: str) -> dict:
    handle = _extract_handle_from_path(url, "@")
    result = {"handle": handle}

    resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    soup = BeautifulSoup(resp.text, "html.parser")

    # TikTok meta tags
    for tag in soup.find_all("meta"):
        prop = tag.get("property", "") or tag.get("name", "")
        content = tag.get("content", "")
        if "description" in prop:
            result["raw_bio"] = content

    # Follower count — often in structured data
    scripts = soup.find_all("script", type="application/ld+json")
    for s in scripts:
        text = s.get_text()
        m = re.search(r'"followerCount"\s*:\s*(\d+)', text)
        if m:
            result["followers"] = _format_number(int(m.group(1)))
            break

    # Try JSON data embedded in page
    page_text = resp.text
    m = re.search(r'"followerCount":(\d+)', page_text)
    if m and "followers" not in result:
        result["followers"] = _format_number(int(m.group(1)))

    return result


def _scrape_youtube(url: str) -> dict:
    handle = _extract_handle_from_path(url, "@")
    result = {"handle": handle}

    resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    soup = BeautifulSoup(resp.text, "html.parser")

    for tag in soup.find_all("meta"):
        prop = tag.get("property", "") or tag.get("name", "")
        content = tag.get("content", "")
        if prop == "og:description":
            result["raw_bio"] = content

    # Subscriber count from page source
    m = re.search(r'"subscriberCountText"[^}]*"simpleText":"([^"]+)"', resp.text)
    if m:
        result["followers"] = m.group(1).replace(" subscribers", "").strip()

    return result


def _scrape_instagram(url: str) -> dict:
    handle = _extract_handle_from_path(url, "")
    result = {"handle": f"@{handle.lstrip('@')}" if handle else ""}

    resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    soup = BeautifulSoup(resp.text, "html.parser")

    for tag in soup.find_all("meta"):
        prop = tag.get("property", "") or tag.get("name", "")
        content = tag.get("content", "")
        if prop == "og:description":
            result["raw_bio"] = content
            # Instagram og:description: "X Followers, Y Following, Z Posts – See Instagram..."
            m = re.search(r"([\d,\.KMk]+)\s*Followers", content, re.IGNORECASE)
            if m:
                result["followers"] = _clean_number(m.group(1))

    return result


def _scrape_generic(url: str) -> dict:
    resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    soup = BeautifulSoup(resp.text, "html.parser")
    bio = ""
    for tag in soup.find_all("meta"):
        prop = tag.get("property", "") or tag.get("name", "")
        content = tag.get("content", "")
        if "description" in prop:
            bio = content
            break
    return {"raw_bio": bio}


# ─── Utility helpers ─────────────────────────────────────────────────────────

def _extract_x_handle(url: str) -> str:
    path = urlparse(url).path.strip("/").split("/")
    if path:
        handle = path[0].lstrip("@")
        return f"@{handle}" if handle else ""
    return ""


def _extract_handle_from_path(url: str, prefix: str) -> str:
    path = urlparse(url).path.strip("/").split("/")
    for segment in path:
        if segment.startswith(prefix) or (segment and not segment.startswith("?")):
            return segment.lstrip(prefix)
    return ""


def _clean_number(text: str) -> str:
    text = text.strip().replace(",", "")
    try:
        n = float(text.replace("K", "e3").replace("M", "e6").replace("B", "e9"))
        return _format_number(int(n))
    except ValueError:
        return text


def _format_number(n: int) -> str:
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n/1_000:.1f}K"
    return str(n)
