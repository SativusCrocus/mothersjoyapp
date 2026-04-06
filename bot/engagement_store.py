"""
Engagement tracking backed by SQLite.

Thin wrapper around bot.db functions for backward compatibility.
All JSON I/O and threading locks removed — SQLite handles concurrency.
"""

from __future__ import annotations

import logging

from bot import config
from bot import db as _db

log = logging.getLogger(__name__)


# ── Engagement history ───────────────────────────────────────────────────

def record_engagement(
    action: str,
    target_url: str,
    target_account: str = "",
    hashtag_source: str = "",
    comment_text: str = "",
) -> None:
    """Record a like, comment, or other engagement action."""
    _db.record_engagement(action, target_url, target_account, hashtag_source, comment_text)


def already_engaged(target_url: str, action: str) -> bool:
    """Check if we've already performed this action on this URL."""
    return _db.already_engaged(target_url, action)


def daily_action_count(action: str) -> int:
    """Count actions in the rolling window (backward-compatible name)."""
    return _db.rolling_action_count(action)


def last_engagement_time() -> float:
    """Timestamp of most recent engagement session action (likes/comments)."""
    return _db.last_action_time()


def get_engagement_stats() -> dict:
    """Summary stats for the dashboard."""
    return _db.get_engagement_stats()


# ── Comment reply tracking ───────────────────────────────────────────────

def record_reply(post_url: str, comment_author: str, comment_text: str, reply_text: str):
    """Record a comment reply."""
    _db.record_reply(post_url, comment_author, comment_text, reply_text)


def already_replied(post_url: str, comment_author: str, comment_text: str) -> bool:
    """Check if we've already replied to this specific comment."""
    return _db.already_replied(post_url, comment_author, comment_text)


# ── Follow tracking ──────────────────────────────────────────────────────

def record_follow(account: str, source_post: str = ""):
    """Record a new follow."""
    _db.record_follow(account, source_post)


def record_unfollow(account: str):
    """Mark an account as unfollowed."""
    _db.record_unfollow(account)


def get_stale_follows(days: int | None = None) -> list[dict]:
    """Get follows older than N days that haven't followed back."""
    return _db.get_stale_follows(days)


def daily_follow_count() -> int:
    """Count follows in the rolling window."""
    return daily_action_count("follow")


def mark_followed_back(account: str):
    """Mark that an account followed us back."""
    _db.mark_followed_back(account)


# ── Maintenance ──────────────────────────────────────────────────────────

def prune_old(days: int | None = None):
    """Delete engagement records older than N days."""
    _db.prune_old_engagement(days)
