"""
Diagnose-before-retry engine for self-healing operation.

Classifies errors into actionable diagnoses with targeted recovery steps.
Replaces blind exponential backoff with intelligent error handling.
"""

from __future__ import annotations

import logging
import re
import shutil
import time
from dataclasses import dataclass, field
from typing import Optional

from bot import config

log = logging.getLogger(__name__)


# ── Diagnosis types ─────────────────────────────────────────────────────────

@dataclass(slots=True)
class Diagnosis:
    category: str           # rate_limit, auth_expired, browser_crash, ai_down,
                            # media_expired, queue_empty, network, disk_full, unknown
    recommended_action: str # wait, refresh_auth, restart_browser, use_heuristic,
                            # rescrape, resurrect, skip, emergency_discover
    wait_seconds: int       # how long to wait before retry (0 = immediate)
    can_continue_other_ops: bool  # whether other subsystems can keep running
    message: str            # human-readable description


@dataclass(slots=True)
class RecoveryStep:
    action: str
    target: str
    priority: int  # lower = do first


# ── Error classification ────────────────────────────────────────────────────

_PATTERNS: list[tuple[str, str, str, int, bool]] = [
    # (regex, category, recommended_action, default_wait, can_continue)
    (r"429|RESOURCE_EXHAUSTED|rate.?limit|too many requests",
     "rate_limit", "wait", 300, True),

    (r"authentication|AuthenticationRequired|login|challenge|session.?expired",
     "auth_expired", "refresh_auth", 30, True),

    (r"Target closed|Protocol error|browser|context|page crash|Execution context",
     "browser_crash", "restart_browser", 5, True),

    (r"AIUnavailable|All AI providers|google.*api|gemini.*error|quota",
     "ai_down", "use_heuristic", 60, True),

    (r"expired|403.*media|410.*media|CDN|media.*not found|RESCRAPE",
     "media_expired", "rescrape", 0, True),

    (r"queue.*empty|nothing to claim|no queued work",
     "queue_empty", "resurrect", 0, True),

    (r"socket|gaierror|ConnectionError|Timeout|ECONNRESET|DNS|network",
     "network", "wait", 30, True),

    (r"[Aa]ction.?[Bb]lock|restrict|suspend|blocked",
     "action_blocked", "wait", 3600, False),
]


def diagnose_failure(subsystem: str, error: str | Exception) -> Diagnosis:
    """Classify an error and return a targeted diagnosis."""
    error_str = str(error)

    for pattern, category, action, wait, can_continue in _PATTERNS:
        if re.search(pattern, error_str, re.IGNORECASE):
            return Diagnosis(
                category=category,
                recommended_action=action,
                wait_seconds=wait,
                can_continue_other_ops=can_continue,
                message=f"[{subsystem}] {category}: {error_str[:200]}",
            )

    return Diagnosis(
        category="unknown",
        recommended_action="wait",
        wait_seconds=60,
        can_continue_other_ops=True,
        message=f"[{subsystem}] unknown error: {error_str[:200]}",
    )


# ── Health-based decisions ──────────────────────────────────────────────────

def should_attempt(subsystem: str) -> tuple[bool, str]:
    """Check health registry to decide if a subsystem should attempt work.
    Returns (allowed, reason)."""
    try:
        from bot.health import get_registry
        registry = get_registry()
        if registry.should_attempt(subsystem):
            return True, "ok"
        health = registry.get_all().get(subsystem)
        if health:
            return False, f"cooldown ({health.status}, {health.consecutive_failures} failures)"
        return True, "ok"
    except Exception:
        return True, "ok"  # if health system fails, don't block operations


def diagnose_all() -> list[Diagnosis]:
    """Run diagnostics across all subsystems and return active issues."""
    issues: list[Diagnosis] = []

    try:
        from bot.health import get_registry
        registry = get_registry()
        for name, health in registry.get_all().items():
            if health.status != "healthy" and health.last_error:
                diag = diagnose_failure(name, health.last_error)
                issues.append(diag)
    except Exception as exc:
        log.debug("diagnose_all health check error: %s", exc)

    # Check queue health
    try:
        from bot.db import queue_size
        if queue_size() == 0:
            issues.append(Diagnosis(
                category="queue_empty",
                recommended_action="resurrect",
                wait_seconds=0,
                can_continue_other_ops=True,
                message="Queue is empty — need to resurrect or discover",
            ))
    except Exception:
        pass

    # Check disk space
    try:
        usage = shutil.disk_usage(config.get_account_dir())
        free_gb = usage.free / (1024 ** 3)
        if free_gb < 0.5:
            issues.append(Diagnosis(
                category="disk_full",
                recommended_action="cleanup",
                wait_seconds=0,
                can_continue_other_ops=False,
                message=f"Disk space critical: {free_gb:.1f}GB free",
            ))
        elif free_gb < 1.0:
            issues.append(Diagnosis(
                category="disk_low",
                recommended_action="cleanup",
                wait_seconds=0,
                can_continue_other_ops=True,
                message=f"Disk space low: {free_gb:.1f}GB free",
            ))
    except Exception:
        pass

    # Check rate limiter state
    try:
        from bot.rate_limiter import get_limiter
        limiter = get_limiter()
        if limiter.is_action_blocked():
            remaining = limiter.action_block_remaining()
            issues.append(Diagnosis(
                category="action_blocked",
                recommended_action="wait",
                wait_seconds=remaining,
                can_continue_other_ops=False,
                message=f"Action blocked — {remaining}s remaining",
            ))
    except Exception:
        pass

    return issues


def recovery_plan() -> list[RecoveryStep]:
    """Build ordered recovery steps based on current health."""
    steps: list[RecoveryStep] = []
    issues = diagnose_all()

    for diag in issues:
        if diag.category == "disk_full" or diag.category == "disk_low":
            steps.append(RecoveryStep("cleanup_media_cache", "media_cache", 1))
            steps.append(RecoveryStep("prune_engagement", "db", 2))
            steps.append(RecoveryStep("prune_failed", "db", 3))
            steps.append(RecoveryStep("vacuum_db", "db", 4))
        elif diag.category == "queue_empty":
            steps.append(RecoveryStep("resurrect_failed", "queue", 5))
            steps.append(RecoveryStep("emergency_discover", "scraper", 6))
        elif diag.category == "auth_expired":
            steps.append(RecoveryStep("refresh_auth", "auth", 7))

    # Deduplicate by action
    seen = set()
    unique: list[RecoveryStep] = []
    for step in sorted(steps, key=lambda s: s.priority):
        if step.action not in seen:
            seen.add(step.action)
            unique.append(step)

    return unique


def execute_recovery(steps: list[RecoveryStep]) -> dict[str, bool]:
    """Execute recovery steps. Returns {action: success}."""
    results: dict[str, bool] = {}

    for step in steps:
        try:
            if step.action == "cleanup_media_cache":
                from bot.media_cache import evict_stale
                evict_stale()
                results[step.action] = True

            elif step.action == "prune_engagement":
                from bot.db import prune_old_engagement
                prune_old_engagement()
                results[step.action] = True

            elif step.action == "prune_failed":
                from bot.db import prune_old_failed
                prune_old_failed()
                results[step.action] = True

            elif step.action == "vacuum_db":
                from bot.db import vacuum
                vacuum()
                results[step.action] = True

            elif step.action == "resurrect_failed":
                from bot.db import queue_resurrect_failed
                count = queue_resurrect_failed(max_items=10)
                results[step.action] = count > 0
                if count:
                    log.info("Recovery: resurrected %d items from failed", count)

            elif step.action == "emergency_discover":
                # Discovery is async — flag it for the autopilot loop
                results[step.action] = True  # means "attempted"
                log.info("Recovery: flagged emergency discovery")

            elif step.action == "refresh_auth":
                # Auth refresh happens naturally on next browser context creation
                results[step.action] = True
                log.info("Recovery: auth refresh scheduled for next browser session")

            else:
                results[step.action] = False

        except Exception as exc:
            log.warning("Recovery step '%s' failed: %s", step.action, exc)
            results[step.action] = False

    return results


def recommended_wait(issues: list[Diagnosis] | None = None) -> int:
    """Smart wait time based on active issues. Not blind backoff."""
    if issues is None:
        issues = diagnose_all()

    if not issues:
        return 0

    # Use the maximum wait from all active issues
    max_wait = 0
    for diag in issues:
        if diag.wait_seconds > max_wait:
            max_wait = diag.wait_seconds

    # Cap at 30 minutes
    return min(max_wait, 1800)


def get_status_summary() -> dict:
    """Dashboard-friendly summary of all diagnostics."""
    issues = diagnose_all()
    steps = recovery_plan()

    return {
        "issues": [
            {
                "category": d.category,
                "action": d.recommended_action,
                "wait": d.wait_seconds,
                "message": d.message,
                "can_continue": d.can_continue_other_ops,
            }
            for d in issues
        ],
        "recovery_steps": [
            {"action": s.action, "target": s.target, "priority": s.priority}
            for s in steps
        ],
        "recommended_wait": recommended_wait(issues),
        "healthy": len(issues) == 0,
    }
