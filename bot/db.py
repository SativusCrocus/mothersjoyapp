"""
SQLite storage engine for Mother's Joy Instagram bot.

Replaces JSON files with ACID-compliant, concurrent-safe SQLite.
WAL mode enables concurrent readers during writes.
Thread-local connections prevent cross-thread sharing issues.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from bot import config

log = logging.getLogger(__name__)

_local = threading.local()


# ── Connection management ────────────────────────────────────────────────────

def get_db() -> sqlite3.Connection:
    """Return a thread-local SQLite connection (created on first call per thread)."""
    conn = getattr(_local, "conn", None)
    if conn is not None:
        return conn

    db_path = config.get_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path), timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA synchronous=NORMAL")
    _local.conn = conn
    return conn


def close_db():
    """Close the thread-local connection if open."""
    conn = getattr(_local, "conn", None)
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass
        _local.conn = None


# ── Schema ───────────────────────────────────────────────────────────────────

_SCHEMA = """
CREATE TABLE IF NOT EXISTS queue (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_url      TEXT NOT NULL UNIQUE,
    media_url       TEXT DEFAULT '',
    cached_media_path TEXT DEFAULT '',
    media_type      TEXT DEFAULT 'image',
    caption         TEXT DEFAULT '',
    ai_score        INTEGER DEFAULT 0,
    ai_reason       TEXT DEFAULT '',
    ai_deferred     INTEGER DEFAULT 0,
    status          TEXT DEFAULT 'queued' CHECK(status IN ('queued','claimed','failed')),
    claim_token     TEXT DEFAULT '',
    claimed_at      TEXT DEFAULT '',
    retry_count     INTEGER DEFAULT 0,
    last_error      TEXT DEFAULT '',
    next_retry_after TEXT DEFAULT '',
    queued_at       TEXT NOT NULL,
    discovery_group TEXT DEFAULT '',
    original_caption TEXT DEFAULT '',
    creator_username TEXT DEFAULT '',
    like_count      INTEGER DEFAULT 0,
    extra_json      TEXT DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS posted (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_url      TEXT NOT NULL,
    post_url        TEXT DEFAULT '',
    posted_at       TEXT NOT NULL,
    caption         TEXT DEFAULT '',
    ai_score        INTEGER DEFAULT 0,
    likes_24h       INTEGER DEFAULT 0,
    comments_24h    INTEGER DEFAULT 0,
    check_scheduled INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS failed (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_url      TEXT NOT NULL,
    failed_reason   TEXT DEFAULT '',
    failed_stage    TEXT DEFAULT '',
    failed_at       TEXT NOT NULL,
    resurrectable   INTEGER DEFAULT 1,
    extra_json      TEXT DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS engagement (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    action_type     TEXT NOT NULL,
    target_url      TEXT DEFAULT '',
    target_account  TEXT DEFAULT '',
    hashtag_source  TEXT DEFAULT '',
    comment_text    TEXT DEFAULT '',
    created_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS replies (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    post_url        TEXT NOT NULL,
    comment_author  TEXT NOT NULL,
    comment_text    TEXT DEFAULT '',
    reply_text      TEXT DEFAULT '',
    replied_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS follows (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    account         TEXT NOT NULL,
    followed_at     TEXT NOT NULL,
    source_post     TEXT DEFAULT '',
    followed_back   INTEGER DEFAULT 0,
    unfollowed_at   TEXT DEFAULT ''
);

CREATE TABLE IF NOT EXISTS health_state (
    subsystem       TEXT PRIMARY KEY,
    status          TEXT DEFAULT 'healthy',
    consecutive_failures INTEGER DEFAULT 0,
    cooldown_until  REAL DEFAULT 0,
    last_error      TEXT DEFAULT '',
    last_success    REAL DEFAULT 0,
    last_failure    REAL DEFAULT 0,
    updated_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS rate_limit_state (
    key             TEXT PRIMARY KEY,
    value           TEXT DEFAULT '',
    updated_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_queue_status ON queue(status);
CREATE INDEX IF NOT EXISTS idx_queue_source_url ON queue(source_url);
CREATE INDEX IF NOT EXISTS idx_posted_source_url ON posted(source_url);
CREATE INDEX IF NOT EXISTS idx_engagement_action ON engagement(action_type, created_at);
CREATE INDEX IF NOT EXISTS idx_engagement_target ON engagement(target_url, action_type);
CREATE INDEX IF NOT EXISTS idx_follows_account ON follows(account, unfollowed_at);
CREATE INDEX IF NOT EXISTS idx_replies_post ON replies(post_url, comment_author);
"""


def init_db():
    """Create tables and indexes. Safe to call multiple times."""
    conn = get_db()
    conn.executescript(_SCHEMA)
    conn.commit()
    log.info("Database initialized at %s", config.get_db_path())


# ── Helpers ──────────────────────────────────────────────────────────────────

def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _utcnow_dt() -> datetime:
    return datetime.now(timezone.utc)


# ── JSON migration ───────────────────────────────────────────────────────────

def migrate_from_json():
    """One-time import from existing JSON files into SQLite."""
    conn = get_db()

    row = conn.execute("SELECT COUNT(*) as c FROM posted").fetchone()
    if row["c"] > 0:
        log.info("Database already has data — skipping JSON migration")
        return

    log.info("Starting JSON migration from existing files...")
    imported = {"queue": 0, "posted": 0, "failed": 0, "engagement": 0, "replies": 0, "follows": 0}

    def _load_json(path: Path) -> list:
        if not path.exists():
            return []
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            return data if isinstance(data, list) else []
        except Exception:
            return []

    # Queue
    for item in _load_json(config.get_queue_path()):
        try:
            _migrate_queue_item(conn, item)
            imported["queue"] += 1
        except Exception:
            pass

    # Posted
    for item in _load_json(config.get_posted_path()):
        try:
            conn.execute(
                "INSERT OR IGNORE INTO posted (source_url, post_url, posted_at, caption, ai_score) VALUES (?,?,?,?,?)",
                (item.get("source_url", ""), item.get("post_link", item.get("post_url", "")),
                 item.get("posted_at", _utcnow()), item.get("caption", ""), item.get("ai_score", 0)),
            )
            imported["posted"] += 1
        except Exception:
            pass

    # Failed
    for item in _load_json(config.get_account_dir() / "failed_content.json"):
        try:
            reason = item.get("failed_reason", "")
            resurrectable = 1 if any(r in reason for r in
                ["rate_limit", "browser", "media_expired", "auth", "timeout", ""]) else 0
            extra = {k: v for k, v in item.items()
                     if k not in ("source_url", "failed_reason", "failed_stage", "failed_at")}
            conn.execute(
                "INSERT OR IGNORE INTO failed (source_url, failed_reason, failed_stage, failed_at, resurrectable, extra_json) "
                "VALUES (?,?,?,?,?,?)",
                (item.get("source_url", ""), reason, item.get("failed_stage", ""),
                 item.get("failed_at", _utcnow()), resurrectable, json.dumps(extra)),
            )
            imported["failed"] += 1
        except Exception:
            pass

    # Engagement
    for item in _load_json(config.get_engagement_path()):
        try:
            conn.execute(
                "INSERT INTO engagement (action_type, target_url, target_account, hashtag_source, comment_text, created_at) "
                "VALUES (?,?,?,?,?,?)",
                (item.get("action", ""), item.get("target_url", ""), item.get("target_account", ""),
                 item.get("hashtag_source", ""), item.get("comment_text", ""), item.get("timestamp", _utcnow())),
            )
            imported["engagement"] += 1
        except Exception:
            pass

    # Replies
    for item in _load_json(config.get_replies_path()):
        try:
            conn.execute(
                "INSERT INTO replies (post_url, comment_author, comment_text, reply_text, replied_at) VALUES (?,?,?,?,?)",
                (item.get("post_url", ""), item.get("comment_author", ""), item.get("comment_text", ""),
                 item.get("reply_text", ""), item.get("replied_at", _utcnow())),
            )
            imported["replies"] += 1
        except Exception:
            pass

    # Follows
    for item in _load_json(config.get_follows_path()):
        try:
            conn.execute(
                "INSERT OR IGNORE INTO follows (account, followed_at, source_post, followed_back, unfollowed_at) "
                "VALUES (?,?,?,?,?)",
                (item.get("account", ""), item.get("followed_at", _utcnow()), item.get("source_post", ""),
                 1 if item.get("followed_back") else 0, item.get("unfollowed_at") or ""),
            )
            imported["follows"] += 1
        except Exception:
            pass

    conn.commit()
    log.info("JSON migration complete: %s", imported)


def _migrate_queue_item(conn: sqlite3.Connection, item: dict):
    known_keys = {
        "source_url", "media_url", "cached_media_path", "media_type", "caption",
        "ai_score", "ai_reason", "status", "claim_token", "claimed_at",
        "retry_count", "last_error", "next_retry_after", "queued_at",
        "discovery_group", "original_caption", "creator_username", "like_count",
    }
    extra = {k: v for k, v in item.items() if k not in known_keys}
    conn.execute(
        """INSERT OR IGNORE INTO queue
           (source_url, media_url, cached_media_path, media_type, caption,
            ai_score, ai_reason, status, queued_at,
            discovery_group, original_caption, creator_username, like_count, extra_json)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (item.get("source_url", ""), item.get("media_url", ""), item.get("cached_media_path", ""),
         item.get("media_type", "image"), item.get("caption", ""),
         item.get("ai_score", 0), item.get("ai_reason", ""), "queued",
         item.get("queued_at", _utcnow()), item.get("discovery_group", ""),
         item.get("original_caption", ""), item.get("creator_username", ""),
         item.get("like_count", 0), json.dumps(extra)),
    )


# ── Queue operations ─────────────────────────────────────────────────────────

def queue_enqueue(item: dict) -> bool:
    """Add an item to the queue. Returns True if added."""
    conn = get_db()
    source_url = item.get("source_url", "")
    if not source_url:
        return False
    if already_posted(source_url):
        return False

    known_keys = {
        "source_url", "media_url", "cached_media_path", "media_type", "caption",
        "ai_score", "ai_reason", "discovery_group", "original_caption",
        "creator_username", "like_count",
    }
    extra = {k: v for k, v in item.items() if k not in known_keys}

    try:
        conn.execute(
            """INSERT OR IGNORE INTO queue
               (source_url, media_url, cached_media_path, media_type, caption,
                ai_score, ai_reason, queued_at, discovery_group,
                original_caption, creator_username, like_count, extra_json)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (source_url, item.get("media_url", ""), item.get("cached_media_path", ""),
             item.get("media_type", "image"), item.get("caption", ""),
             item.get("ai_score", 0), item.get("ai_reason", ""), _utcnow(),
             item.get("discovery_group", ""), item.get("original_caption", ""),
             item.get("creator_username", ""), item.get("like_count", 0), json.dumps(extra)),
        )
        conn.commit()
        return conn.total_changes > 0
    except sqlite3.IntegrityError:
        return False


def queue_enqueue_many(items: list[dict], limit: int | None = None) -> int:
    added = 0
    for item in items:
        if limit is not None and added >= limit:
            break
        if queue_enqueue(item):
            added += 1
    if added:
        log.info("Batch enqueued %d items", added)
    return added


def queue_claim_next() -> Optional[dict]:
    """Atomically claim the next available queue item."""
    import uuid
    conn = get_db()
    now_ts = _utcnow()

    # Reclaim expired claims
    conn.execute(
        """UPDATE queue SET status='queued', claim_token='', claimed_at=''
           WHERE status='claimed' AND claimed_at != ''
           AND julianday(?) - julianday(claimed_at) > ?""",
        (now_ts, config.QUEUE_CLAIM_TTL_MINUTES / 1440.0),
    )

    row = conn.execute(
        """SELECT * FROM queue
           WHERE status='queued'
           AND (next_retry_after = '' OR next_retry_after <= ?)
           ORDER BY id ASC LIMIT 1""",
        (now_ts,),
    ).fetchone()

    if not row:
        conn.commit()
        return None

    token = str(uuid.uuid4())
    conn.execute(
        "UPDATE queue SET status='claimed', claim_token=?, claimed_at=? WHERE id=?",
        (token, now_ts, row["id"]),
    )
    conn.commit()

    item = dict(row)
    item["claim_token"] = token
    item["claimed_at"] = now_ts
    try:
        extra = json.loads(item.pop("extra_json", "{}"))
        item.update(extra)
    except (json.JSONDecodeError, TypeError):
        pass

    log.info("Claimed: %s", item.get("source_url", ""))
    return item


def queue_complete_claim(claim_token: str) -> bool:
    if not claim_token:
        return False
    conn = get_db()
    cursor = conn.execute("DELETE FROM queue WHERE claim_token=?", (claim_token,))
    conn.commit()
    return cursor.rowcount > 0


def queue_release_claim(claim_token: str, updates: dict | None = None) -> bool:
    if not claim_token:
        return False
    conn = get_db()

    set_parts = ["status='queued'", "claim_token=''", "claimed_at=''"]
    params: list = []

    if updates:
        for key in ("retry_count", "last_error", "next_retry_after", "media_url", "cached_media_path"):
            if key in updates:
                set_parts.append(f"{key}=?")
                params.append(updates[key])

    params.append(claim_token)
    cursor = conn.execute(f"UPDATE queue SET {', '.join(set_parts)} WHERE claim_token=?", params)
    conn.commit()
    return cursor.rowcount > 0


def queue_fail_claim(claim_token: str, stage: str, reason: str) -> bool:
    if not claim_token:
        return False
    conn = get_db()

    row = conn.execute("SELECT * FROM queue WHERE claim_token=?", (claim_token,)).fetchone()
    if not row:
        return False

    resurrectable = 1 if any(
        r in reason for r in ["rate_limit", "browser", "media_expired", "auth", "timeout"]
    ) else 0

    conn.execute(
        "INSERT INTO failed (source_url, failed_reason, failed_stage, failed_at, resurrectable, extra_json) "
        "VALUES (?,?,?,?,?,?)",
        (row["source_url"], reason, stage, _utcnow(), resurrectable, row["extra_json"]),
    )
    conn.execute("DELETE FROM queue WHERE claim_token=?", (claim_token,))
    conn.commit()
    log.info("Failed claim: %s (%s)", row["source_url"], reason)
    return True


def queue_resurrect_failed(max_items: int = 5) -> int:
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM failed WHERE resurrectable=1 ORDER BY failed_at DESC LIMIT ?",
        (max_items,),
    ).fetchall()

    resurrected = 0
    for row in rows:
        source_url = row["source_url"]
        if already_posted(source_url):
            conn.execute("DELETE FROM failed WHERE id=?", (row["id"],))
            continue
        existing = conn.execute("SELECT id FROM queue WHERE source_url=?", (source_url,)).fetchone()
        if existing:
            conn.execute("DELETE FROM failed WHERE id=?", (row["id"],))
            continue

        try:
            extra = json.loads(row["extra_json"] or "{}")
        except (json.JSONDecodeError, TypeError):
            extra = {}

        conn.execute(
            """INSERT OR IGNORE INTO queue
               (source_url, media_url, media_type, caption, ai_score, queued_at,
                discovery_group, original_caption, creator_username, extra_json)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (source_url, extra.get("media_url", ""), extra.get("media_type", "video"),
             extra.get("caption", ""), extra.get("ai_score", 0), _utcnow(),
             extra.get("discovery_group", ""), extra.get("original_caption", ""),
             extra.get("creator_username", ""), row["extra_json"]),
        )
        conn.execute("DELETE FROM failed WHERE id=?", (row["id"],))
        resurrected += 1

    if resurrected:
        conn.commit()
        log.info("Resurrected %d items from failed", resurrected)
    return resurrected


def queue_size() -> int:
    conn = get_db()
    row = conn.execute("SELECT COUNT(*) as c FROM queue WHERE status IN ('queued','claimed')").fetchone()
    return row["c"]


def queue_peek(n: int = 5) -> list[dict]:
    conn = get_db()
    rows = conn.execute("SELECT * FROM queue WHERE status='queued' ORDER BY id ASC LIMIT ?", (n,)).fetchall()
    return [dict(r) for r in rows]


def queue_cleanup_stale() -> int:
    conn = get_db()
    current_size = queue_size()
    min_reserve = getattr(config, "QUEUE_MIN_RESERVE_BEFORE_PURGE", 10)
    if current_size <= min_reserve:
        return 0

    cutoff_str = (_utcnow_dt() - timedelta(hours=config.QUEUE_ITEM_MAX_AGE_HOURS)).isoformat()
    purgeable = current_size - min_reserve
    cursor = conn.execute(
        """DELETE FROM queue WHERE id IN (
               SELECT id FROM queue WHERE status='queued' AND queued_at < ?
               ORDER BY queued_at ASC LIMIT ?)""",
        (cutoff_str, purgeable),
    )
    removed = cursor.rowcount
    if removed:
        conn.commit()
        log.info("Cleaned %d stale queue items", removed)
    return removed


# ── Posted operations ────────────────────────────────────────────────────────

def already_posted(url: str) -> bool:
    conn = get_db()
    row = conn.execute("SELECT id FROM posted WHERE source_url=? LIMIT 1", (url,)).fetchone()
    return row is not None


def mark_posted(source_url: str, post_url: str = "", caption: str = "", ai_score: int = 0):
    conn = get_db()
    conn.execute(
        "INSERT INTO posted (source_url, post_url, posted_at, caption, ai_score) VALUES (?,?,?,?,?)",
        (source_url, post_url, _utcnow(), caption, ai_score),
    )
    conn.commit()


def get_posted_history(limit: int = 200) -> list[dict]:
    conn = get_db()
    rows = conn.execute("SELECT * FROM posted ORDER BY posted_at DESC LIMIT ?", (limit,)).fetchall()
    return [dict(r) for r in rows]


def update_post_metrics(post_url: str, likes: int, comments: int):
    conn = get_db()
    conn.execute(
        "UPDATE posted SET likes_24h=?, comments_24h=?, check_scheduled=1 WHERE post_url=?",
        (likes, comments, post_url),
    )
    conn.commit()


# ── Engagement operations ────────────────────────────────────────────────────

def record_engagement(action: str, target_url: str, target_account: str = "",
                      hashtag_source: str = "", comment_text: str = ""):
    conn = get_db()
    conn.execute(
        "INSERT INTO engagement (action_type, target_url, target_account, hashtag_source, comment_text, created_at) "
        "VALUES (?,?,?,?,?,?)",
        (action, target_url, target_account, hashtag_source, comment_text, _utcnow()),
    )
    conn.commit()


def rolling_action_count(action: str, window_hours: int | None = None) -> int:
    window = window_hours or getattr(config, "RATE_WINDOW_HOURS", 24)
    conn = get_db()
    cutoff = (_utcnow_dt() - timedelta(hours=window)).isoformat()
    row = conn.execute(
        "SELECT COUNT(*) as c FROM engagement WHERE action_type=? AND created_at > ?",
        (action, cutoff),
    ).fetchone()
    return row["c"]


def last_action_time(action: str | None = None) -> float:
    conn = get_db()
    if action:
        row = conn.execute(
            "SELECT created_at FROM engagement WHERE action_type=? ORDER BY created_at DESC LIMIT 1",
            (action,),
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT created_at FROM engagement WHERE action_type IN ('like','comment') ORDER BY created_at DESC LIMIT 1",
        ).fetchone()
    if not row:
        return 0
    try:
        return datetime.fromisoformat(row["created_at"]).timestamp()
    except (ValueError, TypeError):
        return 0


def already_engaged(target_url: str, action: str) -> bool:
    conn = get_db()
    row = conn.execute(
        "SELECT id FROM engagement WHERE target_url=? AND action_type=? LIMIT 1",
        (target_url, action),
    ).fetchone()
    return row is not None


def get_engagement_stats() -> dict:
    conn = get_db()
    window = getattr(config, "RATE_WINDOW_HOURS", 24)
    cutoff = (_utcnow_dt() - timedelta(hours=window)).isoformat()
    result = {}
    for action in ("like", "comment", "follow", "reply"):
        row_today = conn.execute(
            "SELECT COUNT(*) as c FROM engagement WHERE action_type=? AND created_at > ?",
            (action, cutoff),
        ).fetchone()
        row_total = conn.execute(
            "SELECT COUNT(*) as c FROM engagement WHERE action_type=?", (action,),
        ).fetchone()
        result[f"{action}s_today"] = row_today["c"]
        result[f"total_{action}s"] = row_total["c"]
    return result


def prune_old_engagement(days: int | None = None):
    days = days or getattr(config, "ENGAGEMENT_PRUNE_DAYS", 90)
    conn = get_db()
    cutoff = (_utcnow_dt() - timedelta(days=days)).isoformat()
    cursor = conn.execute("DELETE FROM engagement WHERE created_at < ?", (cutoff,))
    if cursor.rowcount:
        conn.commit()
        log.info("Pruned %d old engagement records", cursor.rowcount)


# ── Reply operations ─────────────────────────────────────────────────────────

def record_reply(post_url: str, comment_author: str, comment_text: str, reply_text: str):
    conn = get_db()
    conn.execute(
        "INSERT INTO replies (post_url, comment_author, comment_text, reply_text, replied_at) VALUES (?,?,?,?,?)",
        (post_url, comment_author, comment_text, reply_text, _utcnow()),
    )
    conn.commit()
    record_engagement("reply", post_url, target_account=comment_author)


def already_replied(post_url: str, comment_author: str, comment_text: str) -> bool:
    conn = get_db()
    row = conn.execute(
        "SELECT id FROM replies WHERE post_url=? AND comment_author=? AND comment_text=? LIMIT 1",
        (post_url, comment_author, comment_text),
    ).fetchone()
    return row is not None


# ── Follow operations ────────────────────────────────────────────────────────

def record_follow(account: str, source_post: str = ""):
    conn = get_db()
    existing = conn.execute(
        "SELECT id FROM follows WHERE account=? AND unfollowed_at='' LIMIT 1", (account,),
    ).fetchone()
    if existing:
        return
    conn.execute(
        "INSERT INTO follows (account, followed_at, source_post) VALUES (?,?,?)",
        (account, _utcnow(), source_post),
    )
    conn.commit()
    record_engagement("follow", f"https://www.instagram.com/{account}/", target_account=account)


def record_unfollow(account: str):
    conn = get_db()
    conn.execute(
        "UPDATE follows SET unfollowed_at=? WHERE account=? AND unfollowed_at=''",
        (_utcnow(), account),
    )
    conn.commit()


def get_stale_follows(days: int | None = None) -> list[dict]:
    days = days or config.FOLLOW_UNFOLLOW_AFTER_DAYS
    conn = get_db()
    cutoff = (_utcnow_dt() - timedelta(days=days)).isoformat()
    rows = conn.execute(
        "SELECT * FROM follows WHERE unfollowed_at='' AND followed_back=0 AND followed_at < ?",
        (cutoff,),
    ).fetchall()
    return [dict(r) for r in rows]


def mark_followed_back(account: str):
    conn = get_db()
    conn.execute(
        "UPDATE follows SET followed_back=1 WHERE account=? AND unfollowed_at=''", (account,),
    )
    conn.commit()


# ── Health state operations ──────────────────────────────────────────────────

def get_health(subsystem: str) -> Optional[dict]:
    conn = get_db()
    row = conn.execute("SELECT * FROM health_state WHERE subsystem=?", (subsystem,)).fetchone()
    return dict(row) if row else None


def set_health(subsystem: str, **kwargs):
    conn = get_db()
    existing = get_health(subsystem)
    now = _utcnow()
    if existing:
        set_parts = [f"{k}=?" for k in kwargs]
        set_parts.append("updated_at=?")
        params = list(kwargs.values()) + [now, subsystem]
        conn.execute(f"UPDATE health_state SET {', '.join(set_parts)} WHERE subsystem=?", params)
    else:
        kwargs["subsystem"] = subsystem
        kwargs["updated_at"] = now
        cols = ", ".join(kwargs.keys())
        placeholders = ", ".join("?" for _ in kwargs)
        conn.execute(f"INSERT INTO health_state ({cols}) VALUES ({placeholders})", list(kwargs.values()))
    conn.commit()


def get_all_health() -> dict[str, dict]:
    conn = get_db()
    rows = conn.execute("SELECT * FROM health_state").fetchall()
    return {row["subsystem"]: dict(row) for row in rows}


# ── Rate limit state ─────────────────────────────────────────────────────────

def get_rate_limit_value(key: str, default: str = "") -> str:
    conn = get_db()
    row = conn.execute("SELECT value FROM rate_limit_state WHERE key=?", (key,)).fetchone()
    return row["value"] if row else default


def set_rate_limit_value(key: str, value: str):
    conn = get_db()
    conn.execute(
        "INSERT OR REPLACE INTO rate_limit_state (key, value, updated_at) VALUES (?,?,?)",
        (key, value, _utcnow()),
    )
    conn.commit()


# ── Failed operations ────────────────────────────────────────────────────────

def get_failed_history(limit: int = 200) -> list[dict]:
    conn = get_db()
    rows = conn.execute("SELECT * FROM failed ORDER BY failed_at DESC LIMIT ?", (limit,)).fetchall()
    return [dict(r) for r in rows]


def prune_old_failed(days: int | None = None):
    days = days or getattr(config, "FAILED_PRUNE_DAYS", 30)
    conn = get_db()
    cutoff = (_utcnow_dt() - timedelta(days=days)).isoformat()
    cursor = conn.execute("DELETE FROM failed WHERE failed_at < ? AND resurrectable=0", (cutoff,))
    if cursor.rowcount:
        conn.commit()
        log.info("Pruned %d old failed records", cursor.rowcount)


# ── Maintenance ──────────────────────────────────────────────────────────────

def vacuum():
    conn = get_db()
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    try:
        conn.execute("VACUUM")
        log.info("Database vacuumed")
    except sqlite3.OperationalError:
        log.debug("VACUUM skipped (database busy)")
