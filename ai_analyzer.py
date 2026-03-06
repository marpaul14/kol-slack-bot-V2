"""
ai_analyzer.py — Uses Claude Haiku (cheapest model) to analyze KOL posts.

Process:
1. Apify scrapes 10 recent posts
2. Claude Haiku analyzes posts to determine niche & language

Cost: ~$0.25 per 1M tokens (very cheap!)

This runs ONCE during /scanall and results are cached.
/findkol only queries the database - no AI calls needed.
"""

import os
import json
import logging
import re

logger = logging.getLogger(__name__)

# Initialize Anthropic client
_client = None
try:
    from anthropic import Anthropic
    if os.environ.get("ANTHROPIC_API_KEY"):
        _client = Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
except ImportError:
    logger.warning("[AI] anthropic package not installed")


SYSTEM_PROMPT = """You are an expert KOL (Key Opinion Leader) analyst.
Analyze the social media posts and profile to determine the KOL's content niche and language.
Always respond with valid JSON only — no markdown, no explanation."""

ANALYSIS_PROMPT = """Analyze this KOL's recent posts and determine their niche.

PROFILE:
- Handle: {handle}
- Followers: {followers}
- Bio: {bio}
- Location: {location}

RECENT POSTS (10 posts - analyze these to determine content niche):
{posts}

Based on the posts above, return JSON with:
- niche: string — the PRIMARY content topic. Choose ONE from:
  Crypto, DeFi, NFT, Web3, Bitcoin, Trading, Gaming, Tech, AI, 
  Beauty, Fashion, Fitness, Travel, Food, Finance, Investing,
  Music, Comedy, Education, News, Lifestyle, Entertainment, Sports, Other

- language: string — primary language of the posts (English, Filipino, Spanish, etc.)

- location: string — location if mentioned or inferred (or empty string if unknown)

Return ONLY valid JSON like: {{"niche": "Crypto", "language": "English", "location": "Philippines"}}""""""


def analyze_profile(
    platform: str,
    followers: str,
    bio: str,
    location: str = "",
    handle: str = "",
    recent_posts: list = None,
) -> dict:
    """
    Analyze KOL profile and recent posts to determine niche.
    
    Args:
        platform: X, TikTok, YouTube, Instagram
        followers: Follower count string
        bio: Profile bio text
        location: Known location
        handle: @handle
        recent_posts: List of up to 5 recent post texts
    
    Returns:
        Dict with niche, language, location
    """
    posts = recent_posts or []
    
    # If no AI client, use fallback
    if not _client:
        logger.warning("[AI] No API key, using keyword-based analysis")
        return _fallback_analysis(handle, bio, posts)
    
    # Format posts for prompt
    if posts:
        posts_text = "\n".join(f"{i+1}. {p[:300]}" for i, p in enumerate(posts[:10]))
    else:
        posts_text = "(No posts available - analyze handle and bio only)"
    
    prompt = ANALYSIS_PROMPT.format(
        handle=handle or "Unknown",
        followers=followers or "Unknown",
        bio=bio[:500] if bio else "(No bio)",
        location=location or "Unknown",
        posts=posts_text,
    )

    logger.info(f"[AI] Analyzing @{handle} with {len(posts)} posts")

    try:
        response = _client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=256,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = response.content[0].text.strip()

        # Strip markdown fences
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)

        data = json.loads(raw)
        
        result = {
            "niche": data.get("niche", ""),
            "language": data.get("language", "English"),
            "location": data.get("location", "") or location,
        }
        
        logger.info(f"[AI] Result: niche={result['niche']}, language={result['language']}")
        return result
        
    except json.JSONDecodeError as e:
        logger.warning(f"[AI] Invalid JSON response: {e}")
        return _fallback_analysis(handle, bio, posts)
    except Exception as e:
        logger.error(f"[AI] Analysis failed: {e}")
        return _fallback_analysis(handle, bio, posts)


def _fallback_analysis(handle: str, bio: str, posts: list) -> dict:
    """
    Rule-based fallback when AI is unavailable.
    Uses keyword matching on handle, bio, and posts.
    """
    handle_lower = (handle or "").lower()
    bio_lower = (bio or "").lower()
    posts_text = " ".join(posts).lower() if posts else ""
    combined = f"{handle_lower} {bio_lower} {posts_text}"
    
    # Keyword-based niche detection (ordered by priority)
    niche = "Other"
    
    # Crypto-related
    crypto_keywords = [
        "crypto", "defi", "web3", "nft", "eth", "btc", "bitcoin", "ethereum",
        "solana", "altcoin", "trading", "hodl", "airdrop", "degen", "gm", 
        "wagmi", "blockchain", "token", "chain", "wallet", "mint"
    ]
    if any(kw in combined for kw in crypto_keywords):
        # More specific crypto niches
        if any(kw in combined for kw in ["defi", "yield", "swap", "liquidity"]):
            niche = "DeFi"
        elif any(kw in combined for kw in ["nft", "mint", "collection", "pfp"]):
            niche = "NFT"
        elif any(kw in combined for kw in ["web3", "dapp"]):
            niche = "Web3"
        elif any(kw in combined for kw in ["trading", "chart", "ta ", "technical"]):
            niche = "Trading"
        else:
            niche = "Crypto"
    
    # Other niches
    elif any(kw in combined for kw in ["game", "gaming", "esport", "stream", "twitch", "gamer"]):
        niche = "Gaming"
    elif any(kw in combined for kw in ["tech", "dev", "code", "programming", "software", "startup"]):
        niche = "Tech"
    elif any(kw in combined for kw in [" ai ", "artificial", "machine learning", "llm", "chatgpt"]):
        niche = "AI"
    elif any(kw in combined for kw in ["beauty", "makeup", "skincare", "cosmetic"]):
        niche = "Beauty"
    elif any(kw in combined for kw in ["fashion", "style", "outfit", "ootd"]):
        niche = "Fashion"
    elif any(kw in combined for kw in ["fitness", "gym", "workout", "health", "muscle"]):
        niche = "Fitness"
    elif any(kw in combined for kw in ["travel", "nomad", "adventure", "trip", "destination"]):
        niche = "Travel"
    elif any(kw in combined for kw in ["food", "cook", "recipe", "restaurant", "eat", "chef"]):
        niche = "Food"
    elif any(kw in combined for kw in ["finance", "invest", "stock", "money", "wealth", "market"]):
        niche = "Finance"
    elif any(kw in combined for kw in ["music", "song", "artist", "album", "spotify"]):
        niche = "Music"
    elif any(kw in combined for kw in ["comedy", "funny", "joke", "humor", "meme"]):
        niche = "Comedy"
    
    logger.info(f"[AI] Fallback analysis: {handle} -> {niche}")
    
    return {
        "niche": niche,
        "language": "English",
        "location": "",
    }


def parse_find_query(query: str) -> dict:
    """
    Parse a /findkol query into structured filters.
    This is lightweight - minimal AI usage.
    """
    query_lower = query.lower()
    result = {"niche": None, "platform": None, "language": None, "location": None}
    
    # Detect platform
    if " x " in f" {query_lower} " or "twitter" in query_lower:
        result["platform"] = "X"
    elif "tiktok" in query_lower:
        result["platform"] = "TikTok"
    elif "youtube" in query_lower or " yt " in f" {query_lower} ":
        result["platform"] = "YouTube"
    elif "instagram" in query_lower or " ig " in f" {query_lower} ":
        result["platform"] = "Instagram"
    
    # Detect niches
    niche_keywords = {
        "crypto": "Crypto", "defi": "DeFi", "nft": "NFT", "web3": "Web3",
        "bitcoin": "Bitcoin", "trading": "Trading", "gaming": "Gaming",
        "tech": "Tech", "ai": "AI", "beauty": "Beauty", "fashion": "Fashion",
        "fitness": "Fitness", "travel": "Travel", "food": "Food",
        "finance": "Finance", "music": "Music", "comedy": "Comedy",
    }
    for keyword, niche in niche_keywords.items():
        if keyword in query_lower:
            result["niche"] = niche
            break
    
    # Detect locations
    locations = {
        "ph": "Philippines", "philippines": "Philippines", "filipino": "Philippines", "pinoy": "Philippines",
        "us": "United States", "usa": "United States", "american": "United States",
        "uk": "United Kingdom", "british": "United Kingdom",
        "singapore": "Singapore", "sg": "Singapore",
        "indonesia": "Indonesia", "indo": "Indonesia",
        "vietnam": "Vietnam", "vn": "Vietnam",
        "malaysia": "Malaysia", "my": "Malaysia",
        "thailand": "Thailand", "thai": "Thailand",
    }
    for key, loc in locations.items():
        if key in query_lower:
            result["location"] = loc
            break
    
    # Detect languages
    languages = {
        "english": "English", "tagalog": "Tagalog", "filipino": "Filipino",
        "spanish": "Spanish", "chinese": "Chinese", "japanese": "Japanese",
        "korean": "Korean", "indonesian": "Indonesian", "thai": "Thai",
    }
    for key, lang in languages.items():
        if key in query_lower:
            result["language"] = lang
            break
    
    # If no niche detected, use first word as potential niche
    if not result["niche"]:
        words = query.split()
        if words:
            result["niche"] = words[0].capitalize()
    
    logger.info(f"[AI] Parsed query '{query}' -> {result}")
    return result
