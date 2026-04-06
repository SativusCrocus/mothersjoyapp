"""
Resilient queue backed by SQLite.

Thin wrapper around bot.db functions — all JSON I/O, threading locks,
and file corruption risks have been eliminated.

Backward-compatible: all existing callers continue to work unchanged.
"""

from __future__ import annotations

import logging
from typing import Optional

from bot import db as _db

log = logging.getLogger(__name__)


# ── Dedup ───────────────────────────────────────────────────────────────────

def already_posted(url: str) -> bool:
    """Check if a URL has already been posted."""
    return _db.already_posted(url)


# ── Enqueue ─────────────────────────────────────────────────────────────────

def enqueue(item: dict) -> bool:
    """Add an item to the queue. Returns True if added."""
    added = _db.queue_enqueue(item)
    if added:
        log.info("Enqueued: %s", item.get("source_url", ""))
    return added


def enqueue_many(items: list[dict], limit: int | None = None) -> int:
    """Add multiple items. Returns number added."""
    return _db.queue_enqueue_many(items, limit=limit)


# ── Claim / Release / Fail ──────────────────────────────────────────────────

def claim_next() -> Optional[dict]:
    """Atomically claim the next available queue item."""
    return _db.queue_claim_next()


def complete_claim(claim_token: str) -> bool:
    """Remove a claimed item from the queue after success."""
    return _db.queue_complete_claim(claim_token)


def release_claim(claim_token: str, updates: dict | None = None) -> bool:
    """Return a claimed item to the queue for another attempt."""
    return _db.queue_release_claim(claim_token, updates)


def fail_claim(claim_token: str, stage: str, reason: str) -> bool:
    """Move a claimed item to the failed table."""
    return _db.queue_fail_claim(claim_token, stage, reason)


def dequeue() -> Optional[dict]:
    """Compatibility wrapper: claim + immediately complete."""
    item = claim_next()
    if not item:
        return None
    token = item.get("claim_token", "")
    complete_claim(token)
    return item


# ── Resurrection ────────────────────────────────────────────────────────────

def resurrect_failed(max_items: int = 5) -> int:
    """Move resurrectable items from failed back to queue."""
    return _db.queue_resurrect_failed(max_items)


# ── Maintenance ─────────────────────────────────────────────────────────────

def cleanup_stale() -> int:
    """Remove stale items and reclaim expired claims."""
    return _db.queue_cleanup_stale()


# ── Queries ─────────────────────────────────────────────────────────────────

def peek_queue(n: int = 5) -> list:
    """Return the first n items without removing them."""
    return _db.queue_peek(n)


def queue_size() -> int:
    return _db.queue_size()


def mark_posted(source_url: str, post_link: str = ""):
    """Record a URL as posted."""
    _db.mark_posted(source_url, post_link)
    log.info("Marked posted: %s -> %s", source_url, post_link or "(no link)")


def get_posted_history() -> list:
    return _db.get_posted_history()


def get_failed_history() -> list:
    return _db.get_failed_history()
