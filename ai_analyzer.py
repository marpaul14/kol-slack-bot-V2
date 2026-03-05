"""
ai_analyzer.py — Uses Anthropic Claude to enrich KOL profiles.

Given raw scraped data (bio, followers, platform, location),
Claude infers: niche, language, and estimated rates.
"""

import os
import json
import logging
import re
from anthropic import Anthropic

logger = logging.getLogger(__name__)
client = Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

SYSTEM_PROMPT = """You are an expert KOL (Key Opinion Leader) analyst.
Given a social media profile's raw data, extract and infer structured information.
Always respond with valid JSON only — no markdown, no explanation."""

ANALYSIS_PROMPT = """Analyze this KOL profile and return a JSON object with these exact keys:
- niche: string — primary content niche (e.g., "Crypto", "Gaming", "Beauty", "Finance", "Tech")
- language: string — primary language of content (e.g., "English", "Filipino", "Tagalog", "Chinese")
- location: string — inferred location if not already provided (city/country)
- qt_rate: string — estimated quote-tweet rate (e.g., "$50", "$100-200", "N/A" if not X)
- tweet_rate: string — estimated tweet/post rate
- longform_rate: string — estimated long-form thread/article rate
- article_rate: string — estimated article/blog post rate
- video_rate: string — estimated video rate (for TikTok/YT/Instagram, else "N/A")
- confidence: string — "high", "medium", or "low"

Base your rate estimates on:
- Platform and follower count tiers:
  • Nano (<10K): $10-50 per post
  • Micro (10K-100K): $50-500 per post
  • Mid (100K-500K): $500-2000 per post
  • Macro (500K-1M): $2000-5000 per post
  • Mega (1M+): $5000+ per post
- Niche premium: Crypto/Finance/Tech command 2-3x vs lifestyle niches
- Region: PH/SEA rates are typically 30-60% lower than US/EU rates

Profile data:
Platform: {platform}
Followers: {followers}
Bio: {bio}
Location: {location}
Handle: {handle}

Return ONLY the JSON object."""


def analyze_profile(
    platform: str,
    followers: str,
    bio: str,
    location: str = "",
    handle: str = "",
) -> dict:
    """
    Call Claude to enrich a profile with niche, language, and rate estimates.
    Returns a dict with the enriched fields.
    """
    prompt = ANALYSIS_PROMPT.format(
        platform=platform or "Unknown",
        followers=followers or "Unknown",
        bio=bio or "No bio available",
        location=location or "Unknown",
        handle=handle or "Unknown",
    )

    try:
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",  # Cheapest, fast enough for enrichment
            max_tokens=512,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = response.content[0].text.strip()

        # Strip any accidental markdown fences
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)

        data = json.loads(raw)
        return {
            "niche":      data.get("niche", ""),
            "language":   data.get("language", ""),
            "location":   data.get("location", ""),
            "qt":         data.get("qt_rate", ""),
            "tweet":      data.get("tweet_rate", ""),
            "longform":   data.get("longform_rate", ""),
            "article":    data.get("article_rate", ""),
            "video_rate": data.get("video_rate", ""),
            "ai_confidence": data.get("confidence", "low"),
        }
    except json.JSONDecodeError as e:
        logger.warning(f"AI returned invalid JSON: {e} | raw={raw!r}")
        return {}
    except Exception as e:
        logger.error(f"AI analysis failed: {e}")
        return {}


def parse_find_query(query: str) -> dict:
    """
    Use Claude to parse a natural-language /findkol query into structured filters.
    E.g. "crypto influencer from PH with 100k+ followers" →
         {niche: "Crypto", location: "Philippines", platform: None, language: None}
    """
    try:
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=256,
            system="Parse a KOL search query into JSON filters. Return only JSON.",
            messages=[
                {
                    "role": "user",
                    "content": (
                        f"Parse this KOL search query into a JSON with keys: "
                        f"niche, platform, language, location (null if not mentioned).\n"
                        f"Query: {query}"
                    ),
                }
            ],
        )
        raw = response.content[0].text.strip()
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
        return json.loads(raw)
    except Exception as e:
        logger.warning(f"Query parsing failed: {e}")
        # Simple fallback — treat whole query as niche
        return {"niche": query, "platform": None, "language": None, "location": None}
