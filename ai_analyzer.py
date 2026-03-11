"""
ai_analyzer.py — Uses Claude Haiku (cheapest model) to analyze KOL posts.

Process:
1. Apify scrapes 5 recent posts + bio
2. Claude Haiku analyzes bio and posts to determine niche & language

Cost: ~$0.25 per 1M tokens (very cheap!)

This runs ONCE during /scanall and results are cached.
/findkol only queries the database - no AI calls needed.
"""

import os
import json
import logging
import re

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Niche synonym map for semantic search expansion
# ─────────────────────────────────────────────
NICHE_SYNONYMS = {
    "trading": ["trading", "trader", "technical analysis", "alpha calls", "market analysis",
                "day trading", "chart analysis", "chart", "swing trading", "scalping",
                "ta", "signals", "price action"],
    "defi": ["defi", "decentralized finance", "yield", "liquidity", "swap", "lending",
             "staking", "protocol", "farming", "amm", "dex"],
    "nft": ["nft", "non-fungible", "mint", "collection", "pfp", "digital art",
            "opensea", "nfts"],
    "crypto": ["crypto", "cryptocurrency", "bitcoin", "btc", "ethereum", "eth",
               "blockchain", "altcoin", "token", "web3", "hodl", "degen"],
    "web3": ["web3", "dapp", "decentralized", "onchain", "on-chain", "blockchain"],
    "gaming": ["gaming", "gamer", "esports", "game", "gamefi", "play to earn", "p2e",
               "streamer", "twitch"],
    "ai": ["ai", "artificial intelligence", "machine learning", "llm", "chatgpt",
           "deep learning", "gpt", "neural"],
    "finance": ["finance", "investing", "investment", "stock", "wealth", "portfolio",
                "market", "economics", "financial"],
    "beauty": ["beauty", "makeup", "skincare", "cosmetic", "cosmetics", "glam"],
    "fashion": ["fashion", "style", "outfit", "ootd", "clothing", "designer"],
    "fitness": ["fitness", "gym", "workout", "health", "muscle", "exercise", "training"],
    "food": ["food", "cook", "cooking", "recipe", "restaurant", "chef", "foodie"],
    "travel": ["travel", "nomad", "adventure", "trip", "destination", "tourism"],
    "music": ["music", "song", "artist", "album", "spotify", "musician"],
    "comedy": ["comedy", "funny", "joke", "humor", "meme", "memes"],
    "education": ["education", "teaching", "tutorial", "learn", "course", "educational"],
    "news": ["news", "breaking", "journalism", "reporter", "media", "headlines"],
    "lifestyle": ["lifestyle", "daily", "vlog", "life", "routine"],
    "sports": ["sports", "athlete", "football", "basketball", "soccer", "tennis"],
}


def expand_niche_terms(term: str) -> list:
    """Expand a niche term into related search terms using the synonym map."""
    key = term.lower().strip()
    if key in NICHE_SYNONYMS:
        return NICHE_SYNONYMS[key]
    # Check if the term is a substring of any key
    for canon, synonyms in NICHE_SYNONYMS.items():
        if key in canon or canon in key:
            return synonyms
    return [key]


# Initialize Anthropic client
_client = None
try:
    from anthropic import Anthropic
    if os.environ.get("ANTHROPIC_API_KEY"):
        _client = Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
except ImportError:
    logger.warning("[AI] anthropic package not installed")


SYSTEM_PROMPT = """You are an expert KOL (Key Opinion Leader) analyst.
Analyze the social media profile bio and recent posts to determine the KOL's content niche and language.
Always respond with valid JSON only — no markdown, no explanation."""

ANALYSIS_PROMPT = """Analyze this KOL's bio and recent posts to determine their niche in detail.

PROFILE:
- Handle: {handle}
- Followers: {followers}
- Bio: {bio}
- Location: {location}

RECENT POSTS (up to 5 posts - read these alongside the bio to determine content niche):
{posts}

Based on the bio AND the posts above, return JSON with:
- niche: string — a DETAILED niche description with multiple tags separated by " | " and commas.
  Format: "Primary Niche | Subtopic1, Subtopic2, Subtopic3 | Content Style"
  
  Examples of good niche descriptions:
  - "Trading | Alpha Calls, Market Analysis, Altcoins | Shilling"
  - "DeFi | Trading, Web3 General, Stablecoins | Education"
  - "NFT | Web3 General, GameFi, Solana | Shilling, Community"
  - "Web3 General | News, Scandals/Drama, Memecoins | Commentary"
  - "Trading & Portfolio Management | Web3 General, Risk Analysis | Education"
  - "Gaming | Esports, Streaming, Game Reviews | Entertainment"
  - "Tech | AI, Startups, Programming | Education, News"
  
  Primary niches: Crypto, DeFi, NFT, Web3 General, Trading, Gaming, Tech, AI, Beauty, Fashion, Fitness, Travel, Food, Finance, Music, Comedy, Education, News, Lifestyle, Entertainment, Sports
  
  Subtopics to consider: Alpha Calls, Market Analysis, Altcoins, Memecoins, Stablecoins, GameFi, Solana, Ethereum, Bitcoin, Layer 1, Layer 2, Airdrops, Token Promotions, Community Building, Shilling, Education, News, Commentary, Drama/Scandals, Portfolio Management, Risk Analysis, Long-term Investing, Day Trading

- language: string — primary language of the posts (English, Filipino, Spanish, etc.)

- location: string — location if mentioned or inferred (or empty string if unknown)

Return ONLY valid JSON like: {{"niche": "DeFi | Trading, Web3 General, Stablecoins | Education", "language": "English", "location": "Philippines"}}"""


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
        posts_text = "\n".join(f"{i+1}. {p[:300]}" for i, p in enumerate(posts[:5]))
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

    Supports two syntaxes:
    1. Key:value pairs: /findkol niche:Trading platform:X qt_rate:300-500
    2. Free-text (legacy): /findkol defi philippines

    Multi-word values use hyphens/underscores: niche:technical-analysis
    """
    result = {
        "niche": None, "niche_terms": None, "platform": None,
        "language": None, "location": None,
        "qt_rate": None, "tweet_rate": None,
        "longform_rate": None, "article_rate": None, "followers": None,
    }

    # Key aliases -> canonical filter name
    KEY_ALIASES = {
        "niche": "niche", "platform": "platform",
        "language": "language", "lang": "language",
        "location": "location", "loc": "location", "country": "location",
        "qt_rate": "qt_rate", "qt": "qt_rate",
        "tweet_rate": "tweet_rate", "tweet": "tweet_rate",
        "longform_rate": "longform_rate", "longform": "longform_rate",
        "thread_rate": "longform_rate", "thread": "longform_rate",
        "article_rate": "article_rate", "article": "article_rate",
        "followers": "followers", "follower_count": "followers",
    }

    # Extract key:value pairs
    kv_pairs = re.findall(r'(\w+):(\S+)', query)

    if kv_pairs:
        # Process key:value pairs
        for key, value in kv_pairs:
            canonical = KEY_ALIASES.get(key.lower())
            if canonical:
                # Replace hyphens/underscores with spaces for text filters
                if canonical not in ("qt_rate", "tweet_rate", "longform_rate",
                                     "article_rate", "followers"):
                    value = value.replace("-", " ").replace("_", " ")
                result[canonical] = value

        # Process leftover free-text (after removing key:value pairs)
        leftover = re.sub(r'\w+:\S+', '', query).strip()
        if leftover:
            _parse_freetext(leftover.lower(), result)
    else:
        # Pure free-text mode (backward compatibility)
        _parse_freetext(query.lower(), result)

    # Expand niche terms for semantic search
    if result["niche"]:
        result["niche_terms"] = expand_niche_terms(result["niche"])

    logger.info(f"[AI] Parsed query '{query}' -> {result}")
    return result


def _parse_freetext(query_lower: str, result: dict) -> None:
    """Parse free-text query using keyword matching (legacy support)."""
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
    if not result["niche"]:
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
    if not result["location"]:
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
    if not result["language"]:
        for key, lang in languages.items():
            if key in query_lower:
                result["language"] = lang
                break

    # If no niche detected, use first word as potential niche
    if not result["niche"]:
        words = query_lower.split()
        if words:
            result["niche"] = words[0].capitalize()
