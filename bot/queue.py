"""
Persistent JSON queue with multi-layered deduplication.

Every run: cleanup_stale → refill if low → dequeue one → post → mark_posted.
Items expire after QUEUE_ITEM_MAX_AGE_HOURS to keep content fresh.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from bot import config

log = logging.getLogger(__name__)


# ── Internal I/O ─────────────────────────────────────────────────────────────

def _read_json(path: Path) -> list:
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except (json.JSONDecodeError, ValueError):
        log.warning("Corrupt JSON at %s — resetting to empty list", path)
        return []


def _write_json(path: Path, data: list):
    path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")


def _load_queue() -> list:
    return _read_json(config.get_queue_path())


def _save_queue(queue: list):
    _write_json(config.get_queue_path(), queue)


def _load_posted() -> list:
    return _read_json(config.get_posted_path())


def _save_posted(posted: list):
    _write_json(config.get_posted_path(), posted)


# ── Dedup helpers ────────────────────────────────────────────────────────────

def already_posted(url: str) -> bool:
    """Check if a URL has already been posted."""
    return any(item.get("source_url") == url for item in _load_posted())


def _already_queued(url: str, queue: list) -> bool:
    return any(item.get("source_url") == url for item in queue)


def _is_stale(item: dict) -> bool:
    """Return True if the queued item is older than the max age."""
    queued_at = item.get("queued_at", "")
    if not queued_at:
        return True
    try:
        queued_time = datetime.fromisoformat(queued_at)
        age_secs = (datetime.now(timezone.utc) - queued_time).total_seconds()
        return age_secs > config.QUEUE_ITEM_MAX_AGE_HOURS * 3600
    except (ValueError, TypeError):
        return True


# ── Public API ───────────────────────────────────────────────────────────────

def enqueue(item: dict) -> bool:
    """
    Add an item to the queue.
    Returns True if added, False if duplicate or invalid.
    Dedup layer 1: checks both posted history and current queue.
    """
    url = item.get("source_url", "")
    if not url:
        return False

    if already_posted(url):
        log.debug("Skip enqueue — already posted: %s", url)
        return False

    queue = _load_queue()
    if _already_queued(url, queue):
        log.debug("Skip enqueue — already in queue: %s", url)
        return False

    item["queued_at"] = datetime.now(timezone.utc).isoformat()
    item["status"] = "queued"
    queue.append(item)
    _save_queue(queue)
    log.info("Enqueued: %s", url)
    return True


def dequeue() -> Optional[dict]:
    """
    Pop the front item from the queue.
    Dedup layer 2: re-checks posted status and skips stale items.
    """
    queue = _load_queue()

    while queue:
        item = queue.pop(0)
        url = item.get("source_url", "")

        if already_posted(url):
            log.debug("Dequeue skip — already posted: %s", url)
            continue

        if _is_stale(item):
            log.debug("Dequeue skip — stale: %s", url)
            continue

        _save_queue(queue)
        log.info("Dequeued: %s", url)
        return item

    _save_queue(queue)
    log.warning("Queue empty — nothing to dequeue")
    return None


def cleanup_stale() -> int:
    """Remove items older than QUEUE_ITEM_MAX_AGE_HOURS. Returns count removed."""
    queue = _load_queue()
    before = len(queue)
    cleaned = [item for item in queue if not _is_stale(item)]
    removed = before - len(cleaned)

    if removed:
        _save_queue(cleaned)
        log.info("Cleaned %d stale queue items", removed)

    return removed


def mark_posted(source_url: str, post_link: str = ""):
    """Record a URL as posted in the history file."""
    posted = _load_posted()
    posted.append({
        "source_url": source_url,
        "post_link": post_link,
        "posted_at": datetime.now(timezone.utc).isoformat(),
    })
    _save_posted(posted)
    log.info("Marked posted: %s → %s", source_url, post_link or "(no link)")


def peek_queue(n: int = 5) -> list:
    """Return the first n items without removing them."""
    return _load_queue()[:n]


def queue_size() -> int:
    return len(_load_queue())


def get_posted_history() -> list:
    return _load_posted()
