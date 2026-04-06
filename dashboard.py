#!/usr/bin/env python3
"""
Mother's Joy Dashboard — Flask app (port 5055).

Single-page dark UI with live stats, queue preview, history,
engagement controls, and smart-schedule autopilot.
"""

import logging
import logging.handlers
import os
import random
import signal
import shutil
import sys
import threading
import time
import traceback
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from flask import Flask, jsonify, render_template, request

from bot import config
from bot.agents import MotherJoyAgentTeam
from bot.queue import (
    get_posted_history,
    peek_queue,
    queue_size,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.handlers.RotatingFileHandler(
            os.path.join(os.path.dirname(__file__), "logs", "dashboard.log"),
            maxBytes=5 * 1024 * 1024,  # 5 MB
            backupCount=3,
        ),
    ],
)
log = logging.getLogger("dashboard")


# ── Crash-resilience: log uncaught exceptions before process dies ─────────
def _uncaught_exception_handler(exc_type, exc_value, exc_tb):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_tb)
        return
    log.critical(
        "Uncaught exception — process crashing:\n%s",
        "".join(traceback.format_exception(exc_type, exc_value, exc_tb)),
    )

sys.excepthook = _uncaught_exception_handler


def _uncaught_thread_exception(args):
    log.critical(
        "Uncaught thread exception in %s:\n%s",
        args.thread.name if args.thread else "unknown",
        "".join(traceback.format_exception(args.exc_type, args.exc_value, args.exc_traceback)),
    )

threading.excepthook = _uncaught_thread_exception

app = Flask(__name__, template_folder="templates")

# ── Locks ────────────────────────────────────────────────────────────────
_scrape_lock = threading.Lock()
_post_lock = threading.Lock()
_engagement_lock = threading.Lock()

# ── Shared state ─────────────────────────────────────────────────────────
_status = "idle"          # idle | scraping | posting | engaging | ...
_scrape_active = False
_post_active = False
_engagement_active = False
_autopilot_running = False
_autopilot_thread: threading.Thread | None = None
_autopilot_stop = threading.Event()
_last_post_time: float = 0
_next_post_time: float = 0
_next_slot_iso: str = ""          # ISO string for dashboard display

DEFAULT_ACCOUNT = "mothersjoyapp"
_start_time: float = time.time()
team = MotherJoyAgentTeam()

# ── Maintenance counters ────────────────────────────────────────────────
_last_cleanup_time: float = 0
_CLEANUP_INTERVAL = 3600  # 1 hour


def _run_maintenance():
    """Periodic maintenance: media cache eviction, DB prune, vacuum."""
    global _last_cleanup_time
    now = time.time()
    if now - _last_cleanup_time < _CLEANUP_INTERVAL:
        return
    _last_cleanup_time = now

    try:
        from bot.media_cache import evict_stale
        evict_stale()
    except Exception as exc:
        log.debug("Media cache eviction error: %s", exc)

    try:
        from bot.db import prune_old_engagement, prune_old_failed
        prune_old_engagement()
        prune_old_failed()
    except Exception as exc:
        log.debug("DB prune error: %s", exc)

    # Vacuum weekly (check if last vacuum was >7 days ago)
    try:
        from bot.db import vacuum
        vacuum()
    except Exception:
        pass

    # Clean up orphaned temp files
    try:
        import glob as _glob
        import tempfile
        tmp_dir = tempfile.gettempdir()
        for f in _glob.glob(os.path.join(tmp_dir, "mjbot_*")):
            try:
                age = now - os.path.getmtime(f)
                if age > 3600:  # older than 1 hour
                    os.unlink(f)
            except Exception:
                pass
    except Exception:
        pass


def _check_disk_space() -> dict:
    """Check available disk space."""
    try:
        usage = shutil.disk_usage(config.get_account_dir())
        free_gb = usage.free / (1024 ** 3)
        total_gb = usage.total / (1024 ** 3)
        return {"free_gb": round(free_gb, 2), "total_gb": round(total_gb, 2), "critical": free_gb < 0.5}
    except Exception:
        return {"free_gb": -1, "total_gb": -1, "critical": False}


def _refresh_status():
    global _status
    parts = []
    if _scrape_active:
        parts.append("scraping")
    if _post_active:
        parts.append("posting")
    if _engagement_active:
        parts.append("engaging")
    _status = " + ".join(parts) if parts else "idle"


def _set_scrape_active(active: bool):
    global _scrape_active
    _scrape_active = active
    _refresh_status()


def _set_post_active(active: bool):
    global _post_active
    _post_active = active
    _refresh_status()


def _set_engagement_active(active: bool):
    global _engagement_active
    _engagement_active = active
    _refresh_status()


# ── Smart schedule helpers ───────────────────────────────────────────────

def _seconds_until_next_slot() -> tuple[int, str]:
    """
    Calculate seconds until the next posting slot.
    Returns (seconds, iso_timestamp_of_slot).
    Falls back to POST_INTERVAL_MINUTES if schedule is disabled.
    """
    if not config.POSTING_SCHEDULE_ENABLED or not config.POSTING_SCHEDULE:
        interval = config.POST_INTERVAL_MINUTES * 60
        target = datetime.now().astimezone() + timedelta(seconds=interval)
        return interval, target.isoformat()

    tz = ZoneInfo(config.POSTING_TIMEZONE)
    now = datetime.now(tz)
    jitter = config.POSTING_JITTER_MINUTES

    # Build today's and tomorrow's slot times
    candidates = []
    for day_offset in (0, 1):
        base_day = now + timedelta(days=day_offset)
        for slot in config.POSTING_SCHEDULE:
            slot_time = base_day.replace(
                hour=slot["hour"],
                minute=slot["minute"],
                second=0,
                microsecond=0,
            )
            # Apply jitter
            jitter_mins = random.randint(-jitter, jitter)
            slot_time += timedelta(minutes=jitter_mins)
            if slot_time > now:
                candidates.append(slot_time)

    if not candidates:
        # Shouldn't happen but fallback
        interval = config.POST_INTERVAL_MINUTES * 60
        target = now + timedelta(seconds=interval)
        return interval, target.isoformat()

    next_slot = min(candidates)
    wait_seconds = int((next_slot - now).total_seconds())
    return max(30, wait_seconds), next_slot.isoformat()  # min 30s to avoid tight loops


def _posts_today_count() -> int:
    """Count posts made today (in posting timezone)."""
    tz = ZoneInfo(config.POSTING_TIMEZONE)
    today_start = datetime.now(tz).replace(hour=0, minute=0, second=0, microsecond=0)
    count = 0
    for item in get_posted_history():
        try:
            posted_at = datetime.fromisoformat(item.get("posted_at", ""))
            if posted_at >= today_start:
                count += 1
        except (ValueError, TypeError):
            continue
    return count


# ── Scrape/Post helpers (unchanged logic) ────────────────────────────────

def _resolve_scrape_target(target_size: int | None = None) -> int:
    if target_size is not None:
        return max(0, target_size)
    return max(queue_size() + config.AGENT_BATCH_SIZE, config.QUEUE_TARGET_SIZE)


def _perform_scrape(*, trigger: str, force: bool = True, target_size: int | None = None):
    try:
        _set_scrape_active(True)
        resolved_target = _resolve_scrape_target(target_size)
        log.info("%s: scrape triggered (target=%d)", trigger, resolved_target)
        report = team.refill_queue(force=force, target_size=resolved_target)

        if report.skipped:
            log.info("%s: scrape skipped — queue already healthy (%d)", trigger, report.final_size)
        else:
            log.info(
                "%s scrape complete: discovered=%d reviewed=%d accepted=%d added=%d queue=%d",
                trigger,
                report.discovered,
                report.reviewed,
                report.accepted,
                report.added,
                report.final_size,
            )
        return report
    finally:
        _set_scrape_active(False)


def _start_scrape_job(*, trigger: str, force: bool = True, target_size: int | None = None) -> tuple[bool, str]:
    if not _scrape_lock.acquire(blocking=False):
        return False, "A scrape is already in progress"

    def _do_scrape():
        try:
            _perform_scrape(trigger=trigger, force=force, target_size=target_size)
        except Exception as exc:
            log.error("%s scrape error: %s", trigger, exc)
        finally:
            _scrape_lock.release()

    threading.Thread(target=_do_scrape, daemon=True).start()
    return True, "Scrape started — check back shortly"


def _run_scrape_sync(*, trigger: str, force: bool = True, target_size: int | None = None):
    if not _scrape_lock.acquire(blocking=False):
        log.info("%s: scrape already in progress", trigger)
        return None

    try:
        return _perform_scrape(trigger=trigger, force=force, target_size=target_size)
    except Exception as exc:
        log.error("%s scrape error: %s", trigger, exc)
        return None
    finally:
        _scrape_lock.release()


def _perform_post(*, trigger: str):
    try:
        _set_post_active(True)
        report = team.publish_next()
        if not report.attempted:
            log.warning("%s: nothing to post — queue empty", trigger)
        elif report.posted:
            log.info("%s: posted → %s", trigger, report.post_url)
        else:
            log.warning("%s: post cycle ended with %s", trigger, report.message)
        return report
    finally:
        _set_post_active(False)


def _start_post_job(*, trigger: str) -> tuple[bool, str]:
    if not _post_lock.acquire(blocking=False):
        return False, "Another post is already in progress"

    def _do_post():
        try:
            _perform_post(trigger=trigger)
        except Exception as exc:
            log.error("%s post error: %s", trigger, exc)
        finally:
            _post_lock.release()

    threading.Thread(target=_do_post, daemon=True).start()
    return True, "Posting one item..."


def _run_post_sync(*, trigger: str):
    if not _post_lock.acquire(blocking=False):
        log.info("%s: post already in progress", trigger)
        return None

    try:
        return _perform_post(trigger=trigger)
    except Exception as exc:
        log.error("%s post error: %s", trigger, exc)
        return None
    finally:
        _post_lock.release()


# ── Engagement helpers ───────────────────────────────────────────────────

def _run_engagement_sync(*, trigger: str):
    if not _engagement_lock.acquire(blocking=False):
        log.info("%s: engagement already running", trigger)
        return None
    try:
        _set_engagement_active(True)
        log.info("%s: starting engagement session", trigger)
        report = team.run_engagement()
        log.info(
            "%s engagement: %d likes, %d comments, blocked=%s, msg=%s",
            trigger, report.likes, report.comments, report.blocked, report.message,
        )
        return report
    except Exception as exc:
        log.error("%s engagement error: %s", trigger, exc)
        return None
    finally:
        _set_engagement_active(False)
        _engagement_lock.release()


def _start_engagement_job(*, trigger: str) -> tuple[bool, str]:
    if not _engagement_lock.acquire(blocking=False):
        return False, "Engagement already running"

    def _do():
        try:
            _set_engagement_active(True)
            report = team.run_engagement()
            log.info(
                "%s engagement: likes=%d comments=%d skipped=%s blocked=%s msg=%s",
                trigger, report.likes, report.comments, report.skipped, report.blocked, report.message,
            )
        except Exception as exc:
            log.error("%s engagement error: %s", trigger, exc)
        finally:
            _set_engagement_active(False)
            _engagement_lock.release()

    threading.Thread(target=_do, daemon=True).start()
    return True, "Engagement session started"


def _run_replies_sync(*, trigger: str):
    try:
        log.info("%s: checking for new comments to reply", trigger)
        report = team.run_replies()
        log.info("%s replies: checked=%d replied=%d msg=%s", trigger, report.checked, report.replies, report.message)
        return report
    except Exception as exc:
        log.error("%s reply error: %s", trigger, exc)
        return None


def _run_unfollow_sync(*, trigger: str):
    try:
        log.info("%s: running unfollow sweep", trigger)
        report = team.run_unfollow_sweep()
        log.info("%s unfollow: unfollowed=%d msg=%s", trigger, report.unfollowed, report.message)
        return report
    except Exception as exc:
        log.error("%s unfollow error: %s", trigger, exc)
        return None


# ── Routes: Pages ────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("dashboard.html")


# ── Routes: API ──────────────────────────────────────────────────────────

@app.route("/api/state")
def api_state():
    from bot.engagement_store import get_engagement_stats
    from bot.rate_limiter import is_action_blocked, action_block_remaining

    now = time.time()
    remaining = max(0, _next_post_time - now) if _autopilot_running and _next_post_time else 0
    eng_stats = get_engagement_stats()

    return jsonify({
        "queue_size": queue_size(),
        "posted_count": len(get_posted_history()),
        "status": _status,
        "busy": _scrape_active or _post_active or _engagement_active,
        "scraping": _scrape_active,
        "posting": _post_active,
        "engaging": _engagement_active,
        "autopilot": _autopilot_running,
        "account": config.get_account_name(),
        "next_post_in": int(remaining),
        "next_slot_time": _next_slot_iso,
        "post_interval": config.POST_INTERVAL_MINUTES * 60,
        "schedule_enabled": config.POSTING_SCHEDULE_ENABLED,
        "posts_today": _posts_today_count(),
        "engagement_enabled": config.ENGAGEMENT_ENABLED,
        "action_blocked": is_action_blocked(),
        "action_block_remaining": action_block_remaining(),
        **eng_stats,
    })


@app.route("/api/queue")
def api_queue():
    return jsonify(peek_queue(50))


@app.route("/api/history")
def api_history():
    return jsonify(get_posted_history())


@app.route("/api/health")
def api_health():
    """Simple health check — returns 200 if alive, 503 if critical issues."""
    try:
        from bot.supervisor import diagnose_all
        issues = diagnose_all()
        critical = any(not d.can_continue_other_ops for d in issues)
    except Exception:
        critical = False

    status_code = 503 if critical else 200
    return jsonify({
        "status": "degraded" if critical else "ok",
        "uptime": int(time.time() - _start_time),
        "autopilot": _autopilot_running,
        "queue_size": queue_size(),
    }), status_code


@app.route("/api/health/detailed")
def api_health_detailed():
    """Detailed health: subsystem states, diagnosis, recovery plan, disk usage."""
    result = {
        "uptime": int(time.time() - _start_time),
        "autopilot": _autopilot_running,
        "queue_size": queue_size(),
        "disk": _check_disk_space(),
    }

    try:
        from bot.health import get_registry
        all_health = get_registry().get_all()
        result["subsystems"] = {
            name: {
                "status": h.status,
                "consecutive_failures": h.consecutive_failures,
                "last_error": h.last_error,
            }
            for name, h in all_health.items()
        }
    except Exception:
        result["subsystems"] = {}

    try:
        from bot.supervisor import get_status_summary
        result["supervisor"] = get_status_summary()
    except Exception:
        result["supervisor"] = {}

    try:
        from bot.rate_limiter import get_limiter
        result["rate_limiter"] = get_limiter().get_state_for_dashboard()
    except Exception:
        result["rate_limiter"] = {}

    return jsonify(result)


@app.route("/api/metrics")
def api_metrics():
    """Metrics: post success rate, engagement rates, queue depth."""
    from bot.engagement_store import get_engagement_stats

    posted = get_posted_history()
    posts_total = len(posted)
    posts_with_url = sum(1 for p in posted if p.get("post_url"))

    return jsonify({
        "posts_total": posts_total,
        "posts_successful": posts_with_url,
        "success_rate": round(posts_with_url / max(posts_total, 1), 2),
        "queue_size": queue_size(),
        "engagement": get_engagement_stats(),
        "uptime_hours": round((time.time() - _start_time) / 3600, 1),
    })


@app.route("/api/scrape", methods=["POST"])
def api_scrape():
    ok, message = _start_scrape_job(trigger="Dashboard", force=True)
    status = 200 if ok else 409
    return jsonify({"ok": ok, "message": message}), status


@app.route("/api/post", methods=["POST"])
def api_post():
    ok, message = _start_post_job(trigger="Dashboard")
    status = 200 if ok else 409
    return jsonify({"ok": ok, "message": message}), status


@app.route("/api/engage", methods=["POST"])
def api_engage():
    from bot.rate_limiter import is_action_blocked, action_block_remaining, can_perform, cooldown_remaining

    # Pre-check: give immediate feedback if engagement can't proceed
    if not config.ENGAGEMENT_ENABLED:
        return jsonify({"ok": False, "message": "Engagement is disabled in config"})

    if is_action_blocked():
        mins = max(1, action_block_remaining() // 60)
        return jsonify({"ok": False, "message": f"Action blocked — {mins}m cooldown remaining"})

    cd = cooldown_remaining()
    if cd > 0:
        mins = max(1, cd // 60)
        return jsonify({"ok": False, "message": f"Engagement cooldown — {mins}m remaining"})

    allowed, reason = can_perform("like")
    if not allowed:
        return jsonify({"ok": False, "message": f"Rate limited: {reason}"})

    ok, message = _start_engagement_job(trigger="Dashboard")
    status = 200 if ok else 409
    return jsonify({"ok": ok, "message": message}), status


@app.route("/api/autopilot", methods=["POST"])
def api_autopilot():
    global _autopilot_running, _autopilot_thread, _next_post_time, _next_slot_iso

    data = request.get_json(silent=True) or {}
    action = data.get("action", "toggle")

    if action == "stop" or (action == "toggle" and _autopilot_running):
        _autopilot_stop.set()
        _autopilot_running = False
        _next_post_time = 0
        _next_slot_iso = ""
        log.info("Autopilot stopped")
        return jsonify({"ok": True, "message": "Autopilot stopped", "autopilot": False})

    if _autopilot_running:
        return jsonify({"ok": False, "message": "Autopilot already running", "autopilot": True})

    _autopilot_stop.clear()
    _autopilot_running = True

    def _autopilot_loop():
        global _autopilot_running, _last_post_time, _next_post_time, _next_slot_iso
        consecutive_failures = 0

        mode = "schedule" if config.POSTING_SCHEDULE_ENABLED else "fixed"
        log.info("Autopilot started (mode: %s, self-healing)", mode)

        # Track engagement and reply timing
        last_engagement_run: float = 0
        last_reply_run: float = 0
        last_unfollow_run: float = 0

        while not _autopilot_stop.is_set():
            post_succeeded = False
            try:
                # ── 1. DIAGNOSE — check all subsystems ───
                try:
                    from bot.supervisor import diagnose_all, recovery_plan, execute_recovery
                    issues = diagnose_all()
                    if issues:
                        categories = [d.category for d in issues]
                        log.info("Autopilot diagnosis: %s", ", ".join(categories))

                        # ── 2. RECOVER — execute recovery steps ──
                        steps = recovery_plan()
                        if steps:
                            results = execute_recovery(steps)
                            log.info("Autopilot recovery: %s", results)
                except Exception as exc:
                    log.debug("Supervisor check error: %s", exc)

                # ── 3. QUEUE — resurrect failed, emergency discover ──
                current_queue = queue_size()
                if current_queue == 0:
                    # First try resurrection
                    try:
                        resurrected = team.resurrect_failed(max_items=10)
                        if resurrected:
                            log.info("Autopilot: resurrected %d items from failed", resurrected)
                            current_queue = queue_size()
                    except Exception as exc:
                        log.debug("Resurrection error: %s", exc)

                if current_queue == 0:
                    log.warning("Autopilot: queue still empty — emergency refill")
                    _run_scrape_sync(
                        trigger="Autopilot emergency",
                        force=True,
                        target_size=config.QUEUE_TARGET_SIZE,
                    )
                    current_queue = queue_size()
                    if current_queue == 0:
                        consecutive_failures += 1
                        log.error(
                            "Autopilot: refill produced 0 items (attempt %d) — "
                            "will retry after supervisor-recommended wait",
                            consecutive_failures,
                        )

                # ── 4. POST — gated by supervisor health ─
                if current_queue > 0:
                    report = _run_post_sync(trigger="Autopilot")
                    post_succeeded = report is not None and report.posted

                if post_succeeded:
                    consecutive_failures = 0
                    log.info("Autopilot: post succeeded (queue=%d)", queue_size())
                elif current_queue > 0:
                    consecutive_failures += 1

                # ── 5. REFILL — proactive top-up ─────────
                if queue_size() < config.QUEUE_MIN_SIZE:
                    started, msg = _start_scrape_job(
                        trigger="Autopilot top-up",
                        force=False,
                        target_size=config.QUEUE_TARGET_SIZE,
                    )
                    if started:
                        log.info("Autopilot: top-up scrape started")

                # ── 6. ENGAGE — gated by supervisor ──────
                if post_succeeded and config.ENGAGEMENT_ENABLED:
                    now = time.time()
                    cooldown = config.ENGAGEMENT_COOLDOWN_MINUTES * 60
                    if now - last_engagement_run > cooldown:
                        try:
                            _run_engagement_sync(trigger="Autopilot")
                            last_engagement_run = time.time()
                        except Exception as exc:
                            log.error("Autopilot engagement error: %s", exc)

                # ── 7. REPLIES + UNFOLLOWS — on schedule ─
                if config.COMMENT_REPLY_ENABLED:
                    now = time.time()
                    reply_interval = config.COMMENT_REPLY_CHECK_INTERVAL_MINUTES * 60
                    if now - last_reply_run > reply_interval:
                        try:
                            _run_replies_sync(trigger="Autopilot")
                            last_reply_run = time.time()
                        except Exception as exc:
                            log.error("Autopilot reply error: %s", exc)

                if config.FOLLOW_CREATORS_ENABLED:
                    now = time.time()
                    unfollow_interval = config.FOLLOW_CHECK_INTERVAL_HOURS * 3600
                    if now - last_unfollow_run > unfollow_interval:
                        try:
                            _run_unfollow_sync(trigger="Autopilot")
                            last_unfollow_run = time.time()
                        except Exception as exc:
                            log.error("Autopilot unfollow error: %s", exc)

                # ── 8. CLEANUP — hourly maintenance ──────
                try:
                    _run_maintenance()
                except Exception as exc:
                    log.debug("Maintenance error: %s", exc)

            except Exception as exc:
                consecutive_failures += 1
                log.error("Autopilot error (failures=%d): %s", consecutive_failures, exc)

            # ── 9. WAIT — supervisor-recommended (not blind backoff) ──
            if post_succeeded:
                wait_time, slot_iso = _seconds_until_next_slot()
                _next_slot_iso = slot_iso
            else:
                # Ask supervisor for intelligent wait time
                try:
                    from bot.supervisor import recommended_wait
                    supervisor_wait = recommended_wait()
                    if supervisor_wait > 0:
                        wait_time = supervisor_wait
                    else:
                        # Moderate backoff: 60s base, capped at 10min
                        wait_time = min(60 * (2 ** min(consecutive_failures - 1, 4)), 600)
                except Exception:
                    wait_time = min(60 * (2 ** min(consecutive_failures - 1, 4)), 600)

                _next_slot_iso = ""
                log.info(
                    "Autopilot: next attempt in %ds (failures=%d)",
                    wait_time, consecutive_failures,
                )

            _last_post_time = time.time()
            _next_post_time = _last_post_time + wait_time

            # Wait with 5s wake-check granularity
            for _ in range(wait_time // 5):
                if _autopilot_stop.is_set():
                    break
                _autopilot_stop.wait(5)

        _autopilot_running = False
        _next_post_time = 0
        _next_slot_iso = ""
        log.info("Autopilot thread exiting")

    _autopilot_thread = threading.Thread(target=_autopilot_loop, daemon=True)
    _autopilot_thread.start()

    mode = "Smart schedule" if config.POSTING_SCHEDULE_ENABLED else f"Fixed {config.POST_INTERVAL_MINUTES}m"
    return jsonify({
        "ok": True,
        "message": f"Autopilot started — {mode}",
        "autopilot": True,
    })


# ── Main ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Mother's Joy Dashboard")
    parser.add_argument("--account", "-a", default=DEFAULT_ACCOUNT, help="Account name")
    parser.add_argument("--port", "-p", type=int, default=5055, help="Port (default: 5055)")
    parser.add_argument("--autopilot", action="store_true", help="Auto-start autopilot on launch")
    args = parser.parse_args()

    # Ensure logs directory exists
    os.makedirs(os.path.join(os.path.dirname(__file__), "logs"), exist_ok=True)

    config.set_account(args.account)

    # ── Initialize SQLite database + migrate from JSON ─────────────────
    try:
        from bot.db import init_db, migrate_from_json
        init_db()
        migrate_from_json()
    except Exception as exc:
        log.error("Database init error: %s", exc)

    log.info("Dashboard starting for account: %s (dir: %s)", args.account, os.getcwd())

    # ── Signal handling for graceful shutdown ───────────────────────────
    def _graceful_shutdown(signum, frame):
        sig_name = signal.Signals(signum).name if hasattr(signal, "Signals") else str(signum)
        log.info("Received %s — draining...", sig_name)
        _autopilot_stop.set()

        # Wait briefly for in-flight operations to complete
        for _ in range(12):  # up to 60s
            if not (_scrape_active or _post_active or _engagement_active):
                break
            time.sleep(5)

        # Flush SQLite WAL
        try:
            from bot.db import get_db
            conn = get_db()
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except Exception:
            pass

        log.info("Graceful shutdown complete")
        sys.exit(0)

    signal.signal(signal.SIGTERM, _graceful_shutdown)
    signal.signal(signal.SIGINT, _graceful_shutdown)

    if args.autopilot:
        def _deferred_autopilot():
            """Start autopilot after Flask is ready."""
            import time as _time
            _time.sleep(3)
            import urllib.request
            req = urllib.request.Request(
                f"http://127.0.0.1:{args.port}/api/autopilot",
                data=b'{"action":"start"}',
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            try:
                urllib.request.urlopen(req, timeout=10)
                log.info("Autopilot auto-started via --autopilot flag")
            except Exception as exc:
                log.error("Failed to auto-start autopilot: %s", exc)

        threading.Thread(target=_deferred_autopilot, daemon=True).start()

    app.run(host="0.0.0.0", port=args.port, debug=False)
