"""
Adaptive rate limiter with rolling windows and persistent state.

Replaces hard daily caps with rolling 24h windows.
State persists to SQLite so action blocks survive restarts.
can_perform() returns wait time so callers can SCHEDULE, not GIVE UP.
"""

from __future__ import annotations

import logging
import time
from typing import Optional

from bot import config

log = logging.getLogger(__name__)

# ── Rolling window limits keyed by action type ───────────────────────────────

_WINDOW_LIMITS = {
    "like":    lambda: config.RATE_LIKE_PER_WINDOW,
    "comment": lambda: config.RATE_COMMENT_PER_WINDOW,
    "follow":  lambda: config.RATE_FOLLOW_PER_WINDOW,
    "unfollow": lambda: config.RATE_FOLLOW_PER_WINDOW,
    "reply":   lambda: config.RATE_REPLY_PER_WINDOW,
    "post":    lambda: len(config.POSTING_SCHEDULE) + 10,
    "story":   lambda: len(config.POSTING_SCHEDULE) + 10,
}

_MIN_INTERVALS = {
    "like":    lambda: config.RATE_LIKE_MIN_INTERVAL,
    "comment": lambda: config.RATE_COMMENT_MIN_INTERVAL,
    "follow":  lambda: config.RATE_FOLLOW_MIN_INTERVAL,
    "unfollow": lambda: config.RATE_FOLLOW_MIN_INTERVAL,
}


class AdaptiveRateLimiter:
    """Rolling-window rate limiter with adaptive pacing and persistent state."""

    def __init__(self):
        self._pacing_multiplier: float = 1.0
        self._action_blocked_until: float = 0.0
        self._consecutive_blocks: int = 0
        self._last_action_times: dict[str, float] = {}
        self._loaded = False

    def _ensure_loaded(self):
        if self._loaded:
            return
        self._loaded = True
        try:
            from bot.db import get_rate_limit_value
            self._pacing_multiplier = float(get_rate_limit_value("pacing_multiplier", "1.0"))
            self._action_blocked_until = float(get_rate_limit_value("action_blocked_until", "0"))
            self._consecutive_blocks = int(get_rate_limit_value("consecutive_blocks", "0"))

            import json
            times_json = get_rate_limit_value("last_action_times", "{}")
            self._last_action_times = json.loads(times_json)
        except Exception as exc:
            log.debug("Could not load rate limit state: %s", exc)

    def _save_state(self):
        try:
            import json
            from bot.db import set_rate_limit_value
            set_rate_limit_value("pacing_multiplier", str(self._pacing_multiplier))
            set_rate_limit_value("action_blocked_until", str(self._action_blocked_until))
            set_rate_limit_value("consecutive_blocks", str(self._consecutive_blocks))
            set_rate_limit_value("last_action_times", json.dumps(self._last_action_times))
        except Exception as exc:
            log.debug("Could not save rate limit state: %s", exc)

    def can_perform(self, action: str) -> tuple[bool, str, int]:
        """Check if action is allowed.
        Returns (allowed, reason, wait_seconds_until_allowed).
        wait_seconds lets callers SCHEDULE instead of GIVE UP."""
        self._ensure_loaded()

        # Check 1: action block cooldown (persisted)
        if self.is_action_blocked():
            remaining = self.action_block_remaining()
            return False, f"action_blocked ({remaining}s remaining)", remaining

        # Check 2: rolling window count
        limit_fn = _WINDOW_LIMITS.get(action)
        if limit_fn:
            limit = limit_fn()
            from bot.db import rolling_action_count
            count = rolling_action_count(action)
            if count >= limit:
                # Estimate when oldest action in window falls off
                wait = 3600  # conservative: wait 1 hour
                return False, f"window_limit_reached ({count}/{limit})", wait

        # Check 3: minimum interval between same-type actions
        interval_fn = _MIN_INTERVALS.get(action)
        if interval_fn:
            base_interval = interval_fn()
            effective_interval = base_interval * self._pacing_multiplier
            last_time = self._last_action_times.get(action, 0)
            elapsed = time.time() - last_time
            if elapsed < effective_interval:
                wait = int(effective_interval - elapsed) + 1
                return False, f"pacing_interval ({wait}s)", wait

        return True, "ok", 0

    def mark_action_blocked(self):
        """Instagram showed 'Action Blocked'. Persist and increase pacing."""
        self._ensure_loaded()
        self._consecutive_blocks += 1
        self._pacing_multiplier = min(
            self._pacing_multiplier * config.RATE_BACKPRESSURE_MULTIPLIER,
            5.0,
        )
        block_duration = self._compute_block_duration()
        self._action_blocked_until = time.time() + block_duration
        self._save_state()
        log.warning(
            "ACTION BLOCKED — paused for %ds (block #%d, pacing=%.2fx)",
            block_duration, self._consecutive_blocks, self._pacing_multiplier,
        )

    def _compute_block_duration(self) -> int:
        """Adaptive: starts 1h, doubles per consecutive block, caps at 12h."""
        return min(3600 * (2 ** (self._consecutive_blocks - 1)), 43200)

    def mark_action_succeeded(self, action: str):
        """Record success. Gradually reduce pacing multiplier."""
        self._ensure_loaded()
        self._pacing_multiplier = max(
            1.0,
            self._pacing_multiplier * config.RATE_RECOVERY_MULTIPLIER,
        )
        self._last_action_times[action] = time.time()
        # Reset consecutive blocks on sustained success
        if self._consecutive_blocks > 0:
            from bot.db import rolling_action_count
            recent_count = rolling_action_count(action, window_hours=1)
            if recent_count >= 3:
                self._consecutive_blocks = max(0, self._consecutive_blocks - 1)
        self._save_state()

    def is_action_blocked(self) -> bool:
        self._ensure_loaded()
        return time.time() < self._action_blocked_until

    def action_block_remaining(self) -> int:
        self._ensure_loaded()
        return max(0, int(self._action_blocked_until - time.time()))

    def test_probe(self) -> bool:
        """After block expires, test with one lightweight action before full resume.
        Returns True if probing is recommended (block recently ended)."""
        self._ensure_loaded()
        if self.is_action_blocked():
            return False
        # If block ended less than 5 minutes ago, recommend probe
        time_since_block_end = time.time() - self._action_blocked_until
        return 0 < time_since_block_end < 300

    def get_state_for_dashboard(self) -> dict:
        self._ensure_loaded()
        return {
            "pacing_multiplier": round(self._pacing_multiplier, 2),
            "action_blocked": self.is_action_blocked(),
            "action_block_remaining": self.action_block_remaining(),
            "consecutive_blocks": self._consecutive_blocks,
        }


# ── Module-level singleton ───────────────────────────────────────────────────

_limiter: Optional[AdaptiveRateLimiter] = None


def get_limiter() -> AdaptiveRateLimiter:
    global _limiter
    if _limiter is None:
        _limiter = AdaptiveRateLimiter()
    return _limiter


# ── Backward-compatible module-level functions ───────────────────────────────

def can_perform(action: str) -> tuple[bool, str]:
    """Backward-compatible: returns (allowed, reason)."""
    allowed, reason, _ = get_limiter().can_perform(action)
    return allowed, reason


def is_action_blocked() -> bool:
    return get_limiter().is_action_blocked()


def mark_action_blocked():
    get_limiter().mark_action_blocked()


def action_block_remaining() -> int:
    return get_limiter().action_block_remaining()


def cooldown_remaining() -> int:
    """Seconds until next engagement session is allowed."""
    from bot.db import last_action_time
    last = last_action_time()
    if last == 0:
        return 0
    elapsed = time.time() - last
    cooldown = config.ENGAGEMENT_COOLDOWN_MINUTES * 60
    return max(0, int(cooldown - elapsed))
