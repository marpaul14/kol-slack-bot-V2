# kol-slack-bot

Slack bot for managing your KOL (Key Opinion Leader) roster via Google Sheets.

## Features

| Command | Description |
|---|---|
| `/scanall` | Scrapes every linked profile in the Name column and fills Handle, Platform, Followers, Language, Location, Niche, and rate columns. Overwrites cache. |
| `/findkol <query>` | Natural-language KOL search. Automatically scans any unscanned rows first. |
| `/kolstatus` | Shows cache stats and last scan time. |

## Google Sheet Column Layout

| Col | Name | Description |
|---|---|---|
| A | Name | KOL display name with embedded hyperlink to profile |
| B | Handle | @handle extracted from profile URL |
| C | Platform | X / TikTok / YouTube / Instagram |
| D | Followers | Follower/subscriber count |
| E | QT | Quote-tweet rate |
| F | Tweet | Tweet/post rate |
| G | Longform | Long-form thread rate |
| H | Article | Article/blog rate |
| I | Language | Primary content language |
| J | Location | City/Country |
| K | Tags | Custom tags |
| L | Contact | Contact info |
| M | Notes | Free-form notes |
| N | Niche | AI-detected niche |
| O | Last Scanned | Timestamp of last scan |
| P | Link Status | OK / No Link / Timeout / Error / Limited |

## Setup

### 1. Clone & install
```bash
git clone <repo>
cd kol-slack-bot
pip install -r requirements.txt
```

### 2. Environment variables
Copy `.env.example` to `.env` and fill in your credentials.

```
SLACK_BOT_TOKEN=xoxb-...
SLACK_APP_TOKEN=xapp-...
SPREADSHEET_ID=...
ANTHROPIC_API_KEY=sk-ant-...
```

### 3. Google credentials
Place your service-account `credentials.json` in the project root.  
Share the spreadsheet with the service account email.

### 4. Slack app config
In your Slack app settings:
- Enable **Socket Mode**
- Add slash commands: `/scanall`, `/findkol`, `/kolstatus`
- Bot token scopes: `chat:write`, `commands`

### 5. Run
```bash
python bot.py
```

### Deploy to Railway
Push to GitHub. Railway detects the `Procfile` and runs `python bot.py` as a worker.  
Set all env vars in Railway's Variables tab. Upload `credentials.json` as a file or inject as a base64 env var.

## How caching works

- `/scanall` always re-scrapes every row and refreshes the SQLite cache (`kol_cache.db`).
- `/findkol` only scrapes rows that haven't been cached yet — cached rows are served instantly.
- Cache persists across restarts in `kol_cache.db`.
