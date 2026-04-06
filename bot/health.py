"""
Subsystem health registry for Mother's Joy Instagram bot.

Tracks per-component health scores with adaptive cooldowns.
Persists to SQLite so state survives restarts.
Every module calls report_success/report_failure after operations.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Optional

log = logging.getLogger(__name__)

_SUBSYSTEMS = ("discovery", "ai_filter", "posting", "auth", "engagement", "autopilot")

# Adaptive cooldown: min(30 * 2^failures, 1800) — 30s to 30min, never permanent
_BASE_COOLDOWN = 30
_MAX_COOLDOWN = 1800


@dataclass
class SubsystemHealth:
    name: str
    status: str = "healthy"          # healthy | degraded | down
    consecutive_failures: int = 0
    cooldown_until: float = 0.0
    last_error: str = ""
    last_success: float = 0.0
    last_failure: float = 0.0

    @property
    def is_operational(self) -> bool:
        """True if healthy or degraded (not fully down)."""
        return self.status != "down"

    @property
    def is_in_cooldown(self) -> bool:
        return time.time() < self.cooldown_until

    @property
    def cooldown_remaining(self) -> int:
        return max(0, int(self.cooldown_until - time.time()))


class HealthRegistry:
    """Central health tracker for all bot subsystems."""

    def __init__(self):
        self._cache: dict[str, SubsystemHealth] = {}
        self._loaded = False

    def _ensure_loaded(self):
        if self._loaded:
            return
        self._loaded = True
        try:
            from bot.db import get_all_health
            states = get_all_health()
            for name, data in states.items():
                self._cache[name] = SubsystemHealth(
                    name=name,
                    status=data.get("status", "healthy"),
                    consecutive_failures=data.get("consecutive_failures", 0),
                    cooldown_until=data.get("cooldown_until", 0),
                    last_error=data.get("last_error", ""),
                    last_success=data.get("last_success", 0),
                    last_failure=data.get("last_failure", 0),
                )
        except Exception as exc:
            log.debug("Could not load health state from DB: %s", exc)

    def _get_or_create(self, name: str) -> SubsystemHealth:
        self._ensure_loaded()
        if name not in self._cache:
            self._cache[name] = SubsystemHealth(name=name)
        return self._cache[name]

    def _persist(self, health: SubsystemHealth):
        try:
            from bot.db import set_health
            set_health(
                health.name,
                status=health.status,
                consecutive_failures=health.consecutive_failures,
                cooldown_until=health.cooldown_until,
                last_error=health.last_error,
                last_success=health.last_success,
                last_failure=health.last_failure,
            )
        except Exception as exc:
            log.debug("Could not persist health state: %s", exc)

    def report_success(self, subsystem: str):
        """Record a successful operation. Resets failure count."""
        h = self._get_or_create(subsystem)
        h.consecutive_failures = 0
        h.status = "healthy"
        h.last_success = time.time()
        h.cooldown_until = 0
        self._persist(h)

    def report_failure(self, subsystem: str, error: str = ""):
        """Record a failed operation. Increments failure count and sets cooldown."""
        h = self._get_or_create(subsystem)
        h.consecutive_failures += 1
        h.last_failure = time.time()
        h.last_error = error[:500]  # truncate long errors

        # Status transitions
        if h.consecutive_failures >= 5:
            h.status = "down"
        elif h.consecutive_failures >= 2:
            h.status = "degraded"

        # Adaptive cooldown
        h.cooldown_until = time.time() + self.adaptive_cooldown(h.consecutive_failures)

        self._persist(h)
        log.warning(
            "Health: %s is %s (failures=%d, cooldown=%ds, error=%s)",
            subsystem, h.status, h.consecutive_failures,
            h.cooldown_remaining, error[:100],
        )

    def adaptive_cooldown(self, failures: int) -> int:
        """Compute cooldown: min(30 * 2^failures, 1800). Never permanent."""
        return min(_BASE_COOLDOWN * (2 ** min(failures, 10)), _MAX_COOLDOWN)

    def get_status(self, subsystem: str) -> SubsystemHealth:
        return self._get_or_create(subsystem)

    def is_operational(self, subsystem: str) -> bool:
        """True if the subsystem is healthy or degraded (can still attempt operations)."""
        h = self._get_or_create(subsystem)
        if h.is_in_cooldown:
            return False
        return h.is_operational

    def should_attempt(self, subsystem: str) -> tuple[bool, str]:
        """Check if we should attempt an operation on this subsystem.
        Returns (allowed, reason)."""
        h = self._get_or_create(subsystem)

        if h.is_in_cooldown:
            return False, f"cooldown ({h.cooldown_remaining}s remaining)"

        if h.status == "down":
            # Even "down" subsystems can be retried after cooldown expires
            return True, "retrying_after_cooldown"

        return True, "ok"

    def get_all(self) -> dict[str, dict]:
        """Full state snapshot for dashboard/API."""
        self._ensure_loaded()
        result = {}
        for name in _SUBSYSTEMS:
            h = self._get_or_create(name)
            result[name] = {
                "status": h.status,
                "consecutive_failures": h.consecutive_failures,
                "cooldown_remaining": h.cooldown_remaining,
                "last_error": h.last_error,
                "last_success": h.last_success,
                "last_failure": h.last_failure,
            }
        return result

    def diagnose(self) -> dict[str, str]:
        """Per-subsystem diagnosis summary."""
        self._ensure_loaded()
        result = {}
        for name in _SUBSYSTEMS:
            h = self._get_or_create(name)
            if h.status == "healthy":
                result[name] = "healthy"
            elif h.is_in_cooldown:
                result[name] = f"cooling_down ({h.cooldown_remaining}s, error: {h.last_error[:80]})"
            else:
                result[name] = f"{h.status} (failures={h.consecutive_failures}, last: {h.last_error[:80]})"
        return result


# ── Module-level singleton ───────────────────────────────────────────────────

_registry: Optional[HealthRegistry] = None


def get_registry() -> HealthRegistry:
    global _registry
    if _registry is None:
        _registry = HealthRegistry()
    return _registry
