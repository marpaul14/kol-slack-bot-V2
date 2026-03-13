# KOL Slack Bot

A Slack bot that automates Key Opinion Leader (KOL) roster management. It scrapes social media profiles, analyzes content with AI, and writes enriched data back to a Google Sheet — so your team can manage influencer lists without manual research.

## How It Works

1. Your team maintains a **Google Sheet** with KOL names (hyperlinked to their social profiles), platform, and rate info.
2. The bot **scrapes profiles** via [Apify](https://apify.com/) across X/Twitter, TikTok, YouTube, and Instagram.
3. **Claude AI** analyzes each KOL's bio and 5 recent posts to determine their niche, language, and location.
4. Results are **written back to the sheet** automatically — the bot never overwrites your manual data (rates, tags, contacts, notes).

## Commands

| Command | Description |
|---|---|
| `/scanall` | Scrapes every linked profile in the sheet. Extracts handle, followers, language, location, and niche via AI. Overwrites previously scanned data. |
| `/scannew` | Only scans rows missing Handle, Language, Location, or Niche. Skips already-complete rows. Cost-effective alternative to `/scanall`. |
| `/findkol <query>` | Searches the sheet with structured filters or free-text. No scraping — instant results. |
| `/kolstatus` | Shows roster stats: total rows, scanned/unscanned counts, incomplete rows, and last scan timestamp. |

### `/findkol` Query Syntax

**Structured filters (key:value):**

```
/findkol niche:DeFi location:USA
/findkol niche:Trading platform:X qt:300-500
/findkol niche:Gaming tweet:>200
/findkol niche:NFT followers:>10000
```

**Available filter keys:**

| Filter | Aliases | Example |
|---|---|---|
| `niche` | — | `niche:DeFi` |
| `platform` | — | `platform:X` |
| `language` | `lang` | `lang:English` |
| `location` | `loc` | `loc:USA` |
| `qt` | `qt_rate` | `qt:300-500` |
| `tweet` | `tweet_rate` | `tweet:>200` |
| `longform` | `thread` | `longform:<1000` |
| `article` | — | `article:500` |
| `followers` | — | `followers:>10000` |
| `cookie3` | `c3`, `cookie3_score` | `cookie3:>500` |
| `smart` | `sf`, `smart_followers` | `smart:>1000` |

**Rate/numeric formats:** `300` (exact), `300-500` (range), `>300` (minimum), `<500` (maximum)

**Free-text also works:**

```
/findkol crypto
/findkol defi philippines
```

## Google Sheet Column Layout

| Column | Field | Managed By |
|---|---|---|
| A | Name | User (hyperlinked to profile URL) |
| B | Handle | Bot (extracted from profile URL) |
| C | Platform | User (X / TikTok / YouTube / Instagram) |
| D | Followers | Bot (formatted with K/M suffix) |
| E | QT Rate | User |
| F | Tweet Rate | User |
| G | Longform Rate | User |
| H | Article Rate | User |
| I | Language | Bot (detected from posts/bio) |
| J | Location | Bot (inferred from posts/bio) |
| K | Tags | User |
| L | Contact | User |
| M | Notes | User |
| N | Niche | Bot (AI-detected) |
| O | Last Scanned | Bot (UTC timestamp) |
| P | Link Status | Bot (OK / Limited / Error / No Link / Timeout) |
| Q | Cookie3 Score | User |
| R | Smart Followers | User |

The bot **only writes** to columns B, D, I, J, N, O, and P. It **never modifies** user-managed columns (A, C, E–H, K–M, Q–R).

## Architecture

```
bot.py            Slack commands, message formatting, threading
kol_engine.py     Core orchestration: scan, search, status
scraper.py        Apify integration, profile data extraction
ai_analyzer.py    Claude API for niche/language/location analysis
sheets.py         Google Sheets API read/write layer
```

### External Services

| Service | Purpose |
|---|---|
| **Slack** (Bolt, Socket Mode) | Command handling, ephemeral progress updates |
| **Google Sheets API** | Single source of truth for the KOL roster |
| **Apify** | Web scraping across X, TikTok, YouTube, Instagram |
| **Anthropic Claude API** | AI analysis of bios and posts (uses Haiku for cost efficiency) |

### Key Design Decisions

- **Google Sheet as database** — no separate DB needed; the sheet is the single source of truth.
- **Background threads** — long-running scans run in daemon threads so Slack commands respond instantly.
- **Ephemeral progress** — scan progress is sent as private messages (only the requesting user sees them); final results are posted publicly.
- **Graceful fallbacks** — if Apify is unavailable, handles are extracted from URLs; if Claude is unavailable, niche detection falls back to keyword matching.
- **Rate limiting** — 1.5s delay between rows to respect API rate limits.

## Setup

### 1. Clone and install

```bash
git clone <repo-url>
cd kol-slack-bot-V2
pip install -r requirements.txt
```

**Dependencies:** `slack-bolt`, `slack-sdk`, `python-dotenv`, `google-api-python-client`, `google-auth`, `anthropic`, `apify-client`

### 2. Environment variables

Copy `.env.example` to `.env` and fill in your credentials:

```bash
cp .env.example .env
```

| Variable | Required | Description |
|---|---|---|
| `SLACK_BOT_TOKEN` | Yes | Bot token (`xoxb-...`) from your Slack app |
| `SLACK_APP_TOKEN` | Yes | App-level token (`xapp-...`) for Socket Mode |
| `SPREADSHEET_ID` | Yes | Google Sheet ID (from the sheet URL) |
| `ANTHROPIC_API_KEY` | Yes | Anthropic API key (`sk-ant-...`) |
| `GOOGLE_CREDENTIALS_JSON` | Yes | Service account credentials as a JSON string |
| `SHEET_NAME` | No | Sheet tab name (defaults to `Sheet1`) |
| `ALLOWED_CHANNEL_IDS` | No | Comma-separated Slack channel IDs to restrict bot access. Leave empty to allow all channels. |

### 3. Google Sheets setup

1. Create a service account in Google Cloud Console.
2. Download the credentials JSON.
3. Set the JSON as the `GOOGLE_CREDENTIALS_JSON` env var (or place as `credentials.json` in the project root).
4. Share your Google Sheet with the service account email (give it Editor access).

### 4. Slack app configuration

In your [Slack app settings](https://api.slack.com/apps):

1. **Enable Socket Mode** — generate an app-level token with `connections:write` scope.
2. **Add slash commands:** `/scanall`, `/scannew`, `/findkol`, `/kolstatus`
3. **Bot token scopes:** `chat:write`, `commands`

### 5. Run

```bash
python bot.py
```

### Deploy to Railway

The project includes a `Procfile` for [Railway](https://railway.app/) deployment:

1. Push to GitHub.
2. Connect the repo in Railway.
3. Set all environment variables in Railway's Variables tab.
4. Railway auto-detects the `Procfile` and runs `python bot.py` as a worker.

## Access Control

Set `ALLOWED_CHANNEL_IDS` to restrict which Slack channels can use bot commands. Multiple channels can be specified as a comma-separated list:

```
ALLOWED_CHANNEL_IDS=C01ABC123,C02DEF456
```

If not set, commands work in all channels. To find a channel ID: right-click a channel in Slack > "View channel details" > scroll to the bottom.
