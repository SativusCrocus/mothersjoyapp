---
name: mothersjoy-bot-architecture
description: Mother's Joy Instagram curation bot — architecture, components, and setup requirements
type: project
---

Mother's Joy is an automated Instagram curation bot for a warm parenting community (mothersjoy.app).

**Architecture:** Queue-based, one-post-per-invocation. GitHub Actions runs `main.py --account <name>` every 15 minutes.

**Components:**
- `bot/config.py` — Account-aware config, niches, filters, brand voice settings
- `bot/scraper.py` — Playwright-based Instagram hashtag scraping with API interception
- `bot/ai_filter.py` — Gemini 1.5 Flash for content scoring (1-10) and caption rewriting
- `bot/queue.py` — Persistent JSON queue with 3-layer dedup and 24h staleness expiry
- `bot/poster.py` — Playwright headless Chromium posting with CreatePost API interception
- `main.py` — CLI entry: cleanup → refill → post one → exit
- `dashboard.py` — Flask app (port 5050) with dark UI, autopilot thread, dual locks
- `.github/workflows/bot.yml` — Cron */15 with cache save `if: always()`

**Secrets needed (GitHub / .env):**
- `GEMINI_API_KEY` — Google Gemini API key
- `INSTAGRAM_USERNAME` / `INSTAGRAM_PASSWORD` — IG credentials
- `INSTAGRAM_COOKIES_B64` — base64-encoded cookies.json

**Why:** Automate warm, village-style parenting content curation for growth — no manual posting.
**How to apply:** All changes must preserve the queue-based architecture and warm brand voice (💜, British English, gentle parenting tone).
