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


def get_groq_key() -> str:
    key = os.getenv("GROQ_API_KEY", "")
    if not key:
        raise RuntimeError("GROQ_API_KEY not set in environment or .env")
    return key


def get_gemini_fallback_key() -> str:
    return os.getenv("GEMINI_API_KEY_FALLBACK", "")


def get_instagram_username() -> str:
    return os.getenv("INSTAGRAM_USERNAME", "")


def get_instagram_password() -> str:
    return os.getenv("INSTAGRAM_PASSWORD", "")


# ── Niche search terms ───────────────────────────────────────────────────────

NICHES = [
    # General parenting (race-neutral)
    "gentle parenting",
    "motherhood journey",
    "newborn support",
    "postnatal wellness",
    "parenting community",
    "toddler parenting",
    # Black parenting creators
    "black mum life",
    "black motherhood",
    "black mom joy",
    "melanin mama",
    "black british mums",
    # South Asian / Asian parenting
    "asian mum life",
    "desi parenting",
    "brown mums",
    "south asian motherhood",
    # Mixed / multicultural families
    "mixed race family",
    "multicultural parenting",
    "interracial family",
    "diverse families",
    # General UK mum life
    "mum life UK",
    "new mum advice",
    "baby tips UK",
    "UK parenting tips",
]

# Discovery pools are explicit hashtag sources used for queue balance.
# This is more reliable than guessing demographics from images and lets us
# intentionally pull from the communities Mother's Joy wants to represent.
DISCOVERY_POOLS = {
    "general_parenting": [
        "gentleparenting",
        "newmumadvice",
        "motherhoodjourney",
        "postnatalwellness",
        "newbornsupport",
        "toddlerparenting",
    ],
    "black_parenting": [
        "blackmotherhood",
        "blackmomjoy",
        "blackmumlife",
        "melaninmama",
    ],
    "south_asian_parenting": [
        "desiparenting",
        "asianmumlife",
        "brownmums",
        "southasianmotherhood",
    ],
    "multicultural_parenting": [
        "multiculturalparenting",
        "mixedracefamily",
        "interracialfamily",
        "diversefamilies",
    ],
}

# ── Engagement & quality filters ─────────────────────────────────────────────

MIN_ENGAGEMENT = 0.02          # likes / followers ratio
MAX_AGE_DAYS = 7               # ignore posts older than this
SEARCH_PER_NICHE = 10          # max posts to fetch per niche term
MIN_CAPTION_LENGTH = 20        # lowered to not exclude video posts (content is visual)
SCRAPE_HOURS_BACK = 1440       # allow posts up to ~60 days old (explore page shows popular older content)
SEASONAL_CONTENT_MAX_AGE_HOURS = 72  # stale event posts age out faster
MAX_DISCOVERY_POSTS_PER_ACCOUNT = 1  # keep queue variety high
VIDEO_ONLY_MODE = True             # only scrape/accept video (reel) content
ALLOWED_MEDIA_TYPES = ("video",)   # accepted media_type values

# ── Queue behaviour ──────────────────────────────────────────────────────────

QUEUE_MIN_SIZE = 30            # when queue drops below this, trigger a refill
QUEUE_TARGET_SIZE = 45         # refill back up to this reserve size
QUEUE_ITEM_MAX_AGE_HOURS = 168  # 7 days (media cached locally, no expiry pressure)
POST_INTERVAL_MINUTES = 15     # minimum gap between posts
QUEUE_CLAIM_TTL_MINUTES = 30   # reclaim stuck work items after a crash
QUEUE_MIN_RESERVE_BEFORE_PURGE = 10  # never purge if queue would drop below this
VIDEO_POST_SHARE_WAIT_SECONDS = 35      # give reels longer server-side processing time
PROFILE_CONFIRMATION_ATTEMPTS = 8       # poll profile grid several times after share
PROFILE_CONFIRMATION_WAIT_SECONDS = 10  # wait between profile confirmation polls

# ── Agent orchestration ──────────────────────────────────────────────────────

AGENT_WORKERS = 1              # parallel AI workers during refill
AGENT_BATCH_SIZE = 4           # bounded batch size to avoid over-fetching AI work

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

# ── Smart posting schedule ────────────────────────────────────────────────
# UK peak engagement windows. Autopilot sleeps until the next slot
# rather than using a fixed interval. Off-round minutes look natural.
POSTING_SCHEDULE_ENABLED = False  # keep 15-min posting; engagement agents run independently
POSTING_TIMEZONE = "Europe/London"
POSTING_SCHEDULE = [
    {"hour": 7, "minute": 32},   # early morning parents
    {"hour": 12, "minute": 17},  # lunch break scroll
    {"hour": 17, "minute": 48},  # after-work wind-down
    {"hour": 21, "minute": 14},  # bedtime scroll
]
POSTING_JITTER_MINUTES = 8  # random ± jitter per slot

# ── Caption CTA rotation ─────────────────────────────────────────────────
CTA_STYLES = [
    "follow_ask",       # "Follow @mothersjoyapp for daily parenting warmth"
    "save_prompt",      # "Save this for when you need it most"
    "tag_friend",       # "Tag a mama who needs to hear this"
    "share_prompt",     # "Share this with someone who's in the thick of it"
    "comment_question", # An open-ended parenting question
]

# ── Community engagement ──────────────────────────────────────────────────
ENGAGEMENT_ENABLED = True
ENGAGEMENT_LIKES_PER_SESSION = 12
ENGAGEMENT_COMMENTS_PER_SESSION = 4
ENGAGEMENT_MIN_DELAY_SECONDS = 15
ENGAGEMENT_MAX_DELAY_SECONDS = 45
ENGAGEMENT_COOLDOWN_MINUTES = 90
ENGAGEMENT_ACTION_BLOCK_COOLDOWN_HOURS = 6
ENGAGEMENT_HASHTAGS = [
    "gentleparenting", "mumlife", "newmumadvice",
    "motherhoodjourney", "blackmotherhood", "toddlerparenting",
    "postnatalwellness", "desiparenting", "brownmums",
]

# ── Story sharing ─────────────────────────────────────────────────────────
STORY_SHARE_ENABLED = True
STORY_SHARE_DELAY_SECONDS = 30

# ── Comment auto-reply ────────────────────────────────────────────────────
COMMENT_REPLY_ENABLED = True
COMMENT_REPLY_CHECK_INTERVAL_MINUTES = 60
COMMENT_REPLY_MIN_DELAY_MINUTES = 5
COMMENT_REPLY_MAX_DELAY_MINUTES = 30
COMMENT_REPLY_DAILY_CAP = 30
COMMENT_REPLY_MAX_POST_AGE_HOURS = 72
COMMENT_REPLY_SKIP_KEYWORDS = ["spam", "scam", "dm me", "check bio", "link in bio"]

# ── Creator follow/unfollow ───────────────────────────────────────────────
FOLLOW_CREATORS_ENABLED = True
FOLLOW_UNFOLLOW_AFTER_DAYS = 7
FOLLOW_CHECK_INTERVAL_HOURS = 12


# ── Growth engine file paths ─────────────────────────────────────────────

def get_engagement_path() -> Path:
    return get_account_dir() / "engagement_history.json"


def get_replies_path() -> Path:
    return get_account_dir() / "comment_replies.json"


def get_follows_path() -> Path:
    return get_account_dir() / "follow_tracking.json"


# ── SQLite database ──────────────────────────────────────────────────────

def get_db_path() -> Path:
    return get_account_dir() / "bot.db"


def get_media_cache_dir() -> Path:
    d = get_account_dir() / "media_cache"
    d.mkdir(parents=True, exist_ok=True)
    return d


# ── Rolling rate limits (replaces hard daily caps) ───────────────────────
# Research shows safe limits: 1000 likes, 150 comments, 500 follows/day
# for established accounts. We use conservative-moderate values.
RATE_WINDOW_HOURS = 24
RATE_LIKE_PER_WINDOW = 150         # ~6/hour average
RATE_COMMENT_PER_WINDOW = 40       # ~1.7/hour
RATE_FOLLOW_PER_WINDOW = 25        # ~1/hour
RATE_REPLY_PER_WINDOW = 50

# Min seconds between same-type actions (human-like pacing)
RATE_LIKE_MIN_INTERVAL = 180       # 3 min between likes
RATE_COMMENT_MIN_INTERVAL = 600    # 10 min between comments
RATE_FOLLOW_MIN_INTERVAL = 900     # 15 min between follows

# Adaptive backpressure (adjusts when Instagram pushes back)
RATE_BACKPRESSURE_MULTIPLIER = 1.5
RATE_RECOVERY_MULTIPLIER = 0.95

# ── Proxy ────────────────────────────────────────────────────────────────
PROXY_URL = os.getenv("PROXY_URL", "")
PROXY_STICKY = True

# ── Stealth ──────────────────────────────────────────────────────────────
STEALTH_UA_ROTATE_DAYS = 7
STEALTH_FINGERPRINT_ROTATE_DAYS = 7

# ── Data management ──────────────────────────────────────────────────────
ENGAGEMENT_PRUNE_DAYS = 90
FAILED_PRUNE_DAYS = 30
