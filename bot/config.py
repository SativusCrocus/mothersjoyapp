"""
Centralized configuration for Mother's Joy Instagram bot.

Account-aware: each account stores its own .env, cookies, queue,
posted history, and Playwright state under accounts/<name>/.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

# ── Directories ──────────────────────────────────────────────────────────────

BASE_DIR = Path(__file__).resolve().parent.parent
ACCOUNTS_DIR = BASE_DIR / "accounts"

# ── Active account state ─────────────────────────────────────────────────────

_active_account: str | None = None
_account_dir: Path | None = None


def set_account(name: str):
    """Switch the active account context and load its .env."""
    global _active_account, _account_dir
    _active_account = name
    _account_dir = ACCOUNTS_DIR / name
    _account_dir.mkdir(parents=True, exist_ok=True)

    env_file = _account_dir / ".env"
    if env_file.exists():
        load_dotenv(env_file, override=True)


def get_account_name() -> str:
    if not _active_account:
        raise RuntimeError("No account set. Call set_account(name) first.")
    return _active_account


def get_account_dir() -> Path:
    if not _account_dir:
        raise RuntimeError("No account set. Call set_account(name) first.")
    return _account_dir


# ── Account file paths ───────────────────────────────────────────────────────

def get_cookies_path() -> Path:
    return get_account_dir() / "cookies.json"


def get_queue_path() -> Path:
    return get_account_dir() / "queue.json"


def get_posted_path() -> Path:
    return get_account_dir() / "posted_content.json"


def get_state_path() -> Path:
    return get_account_dir() / "playwright_state.json"


# ── Credentials ──────────────────────────────────────────────────────────────

def get_gemini_key() -> str:
    key = os.getenv("GEMINI_API_KEY", "")
    if not key:
        raise RuntimeError("GEMINI_API_KEY not set in environment or .env")
    return key


def get_instagram_username() -> str:
    return os.getenv("INSTAGRAM_USERNAME", "")


def get_instagram_password() -> str:
    return os.getenv("INSTAGRAM_PASSWORD", "")


# ── Niche search terms ───────────────────────────────────────────────────────

NICHES = [
    "UK parenting tips",
    "newborn support",
    "postnatal wellness",
    "gentle parenting",
    "mum life UK",
    "baby tips UK",
    "motherhood journey",
    "parenting community",
    "new mum advice",
    "toddler parenting",
]

# ── Engagement & quality filters ─────────────────────────────────────────────

MIN_ENGAGEMENT = 0.02          # likes / followers ratio
MAX_AGE_DAYS = 7               # ignore posts older than this
SEARCH_PER_NICHE = 10          # max posts to fetch per niche term
MIN_CAPTION_LENGTH = 50        # skip stubs / low-effort captions
SCRAPE_HOURS_BACK = 48         # only scrape posts this recent

# ── Queue behaviour ──────────────────────────────────────────────────────────

QUEUE_MIN_SIZE = 10            # refill threshold
QUEUE_ITEM_MAX_AGE_HOURS = 24  # items older than this are stale
POST_INTERVAL_MINUTES = 15     # minimum gap between posts

# ── Blocked lists ────────────────────────────────────────────────────────────

BLOCKED_ACCOUNTS: list[str] = []

BLOCKED_HASHTAGS = [
    "#ad", "#sponsored", "#gifted", "#paidpartnership",
    "#affiliate", "#promo", "#collab",
]

BLOCKED_PATTERNS = [
    "use code", "discount", "link in bio", "swipe up",
    "DM to order", "shop now", "limited offer", "giveaway",
    "tap the link", "paid partnership",
]

# ── Brand voice ──────────────────────────────────────────────────────────────

BRAND_LINK = "mothersjoy.app"
BRAND_HANDLE = "@mothersjoyapp"
MAX_CAPTION_LENGTH = 2200
