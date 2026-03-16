#!/usr/bin/env python3
"""
Mother's Joy Dashboard — Flask app (port 5050).

Single-page dark UI with live stats, queue preview, history,
and autopilot controls. Two locks prevent data corruption
and concurrent operations.
"""

import logging
import threading
import time
from pathlib import Path

from flask import Flask, jsonify, render_template, request

from bot import config
from bot.queue import (
    cleanup_stale,
    dequeue,
    enqueue,
    get_posted_history,
    mark_posted,
    peek_queue,
    queue_size,
)
from bot.scraper import discover_content_sync
from bot.ai_filter import passes_filter, generate_post
from bot.poster import post_to_instagram_sync

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("dashboard")

app = Flask(__name__, template_folder="templates")

# ── Locks ────────────────────────────────────────────────────────────────────
# _file_lock:  serialises all JSON reads/writes
# _op_lock:    prevents concurrent scrape/post operations
_file_lock = threading.Lock()
_op_lock = threading.Lock()

# ── Shared state ─────────────────────────────────────────────────────────────
_status = "idle"          # idle | busy
_autopilot_running = False
_autopilot_thread: threading.Thread | None = None
_autopilot_stop = threading.Event()
_last_post_time: float = 0        # timestamp of last autopilot post
_next_post_time: float = 0        # timestamp of next scheduled post

DEFAULT_ACCOUNT = "mothersjoyapp"


def _set_status(s: str):
    global _status
    _status = s


# ── Routes: Pages ────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("dashboard.html")


# ── Routes: API ──────────────────────────────────────────────────────────────

@app.route("/api/state")
def api_state():
    with _file_lock:
        now = time.time()
        remaining = max(0, _next_post_time - now) if _autopilot_running and _next_post_time else 0
        return jsonify({
            "queue_size": queue_size(),
            "posted_count": len(get_posted_history()),
            "status": _status,
            "autopilot": _autopilot_running,
            "account": config.get_account_name(),
            "next_post_in": int(remaining),
            "post_interval": config.POST_INTERVAL_MINUTES * 60,
        })


@app.route("/api/queue")
def api_queue():
    with _file_lock:
        items = peek_queue(50)
    return jsonify(items)


@app.route("/api/history")
def api_history():
    with _file_lock:
        history = get_posted_history()
    return jsonify(history)


@app.route("/api/scrape", methods=["POST"])
def api_scrape():
    if not _op_lock.acquire(blocking=False):
        return jsonify({"ok": False, "message": "Another operation is in progress"}), 409

    def _do_scrape():
        try:
            _set_status("busy")
            log.info("Dashboard: scrape triggered")

            posts = discover_content_sync()
            added = 0
            for post in posts:
                passes, score, reason = passes_filter(post)
                if not passes:
                    continue

                result = generate_post(post)
                caption = result.get("caption", "")
                if not caption:
                    continue

                post["generated_caption"] = caption
                post["ai_score"] = score
                post["ai_reason"] = reason

                with _file_lock:
                    if enqueue(post):
                        added += 1

            log.info("Dashboard scrape complete: added %d items", added)
        except Exception as exc:
            log.error("Scrape error: %s", exc)
        finally:
            _set_status("idle")
            _op_lock.release()

    threading.Thread(target=_do_scrape, daemon=True).start()
    return jsonify({"ok": True, "message": "Scrape started — check back shortly"})


@app.route("/api/post", methods=["POST"])
def api_post():
    if not _op_lock.acquire(blocking=False):
        return jsonify({"ok": False, "message": "Another operation is in progress"}), 409

    def _do_post():
        item = None
        try:
            _set_status("busy")

            with _file_lock:
                item = dequeue()

            if not item:
                log.warning("Dashboard: nothing to post — queue empty")
                return

            source_url = item.get("source_url", "")
            log.info("Dashboard: posting %s", source_url)

            post_url = post_to_instagram_sync(item)

            if post_url:
                with _file_lock:
                    mark_posted(source_url, post_url)
                log.info("Dashboard: posted → %s", post_url)
            else:
                log.error("Dashboard: posting failed for %s — re-enqueuing", source_url)
                with _file_lock:
                    enqueue(item)
        except Exception as exc:
            log.error("Post error: %s", exc)
            if item:
                with _file_lock:
                    enqueue(item)
        finally:
            _set_status("idle")
            _op_lock.release()

    threading.Thread(target=_do_post, daemon=True).start()
    return jsonify({"ok": True, "message": "Posting one item..."})


@app.route("/api/autopilot", methods=["POST"])
def api_autopilot():
    global _autopilot_running, _autopilot_thread

    data = request.get_json(silent=True) or {}
    action = data.get("action", "toggle")

    if action == "stop" or (action == "toggle" and _autopilot_running):
        _autopilot_stop.set()
        _autopilot_running = False
        _next_post_time = 0
        log.info("Autopilot stopped")
        return jsonify({"ok": True, "message": "Autopilot stopped", "autopilot": False})

    if _autopilot_running:
        return jsonify({"ok": False, "message": "Autopilot already running", "autopilot": True})

    _autopilot_stop.clear()
    _autopilot_running = True

    def _autopilot_loop():
        global _autopilot_running, _last_post_time, _next_post_time
        interval = config.POST_INTERVAL_MINUTES * 60
        log.info("Autopilot started (interval: %dm)", config.POST_INTERVAL_MINUTES)

        while not _autopilot_stop.is_set():
            if not _op_lock.acquire(blocking=False):
                log.info("Autopilot: waiting for current operation to finish")
                _autopilot_stop.wait(30)
                continue

            try:
                _set_status("busy")

                # Cleanup stale items
                with _file_lock:
                    cleanup_stale()

                # Refill if needed
                with _file_lock:
                    current_size = queue_size()

                if current_size < config.QUEUE_MIN_SIZE:
                    log.info("Autopilot: refilling queue (%d < %d)", current_size, config.QUEUE_MIN_SIZE)
                    posts = discover_content_sync()
                    for post in posts:
                        with _file_lock:
                            if queue_size() >= config.QUEUE_MIN_SIZE:
                                break

                        passes, score, reason = passes_filter(post)
                        if not passes:
                            continue

                        result = generate_post(post)
                        caption = result.get("caption", "")
                        if not caption:
                            continue

                        post["generated_caption"] = caption
                        post["ai_score"] = score
                        post["ai_reason"] = reason

                        with _file_lock:
                            enqueue(post)

                # Post one
                with _file_lock:
                    item = dequeue()

                if item:
                    source_url = item.get("source_url", "")
                    log.info("Autopilot: posting %s", source_url)

                    post_url = post_to_instagram_sync(item)

                    if post_url:
                        with _file_lock:
                            mark_posted(source_url, post_url)
                        log.info("Autopilot: posted → %s", post_url)
                    else:
                        log.error("Autopilot: posting failed for %s — re-enqueuing", source_url)
                        with _file_lock:
                            enqueue(item)
                else:
                    log.warning("Autopilot: queue empty — skipping post")

            except Exception as exc:
                log.error("Autopilot error: %s", exc)
            finally:
                _set_status("idle")
                _op_lock.release()

            # Set countdown for next post
            _last_post_time = time.time()
            _next_post_time = _last_post_time + interval

            # Wait for next interval (check stop event every 5s)
            for _ in range(interval // 5):
                if _autopilot_stop.is_set():
                    break
                _autopilot_stop.wait(5)

        _autopilot_running = False
        _next_post_time = 0
        log.info("Autopilot thread exiting")

    _autopilot_thread = threading.Thread(target=_autopilot_loop, daemon=True)
    _autopilot_thread.start()

    return jsonify({
        "ok": True,
        "message": f"Autopilot started — posting every {config.POST_INTERVAL_MINUTES}m",
        "autopilot": True,
    })


# ── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Mother's Joy Dashboard 💜")
    parser.add_argument("--account", "-a", default=DEFAULT_ACCOUNT, help="Account name")
    parser.add_argument("--port", "-p", type=int, default=5055, help="Port (default: 5055)")
    args = parser.parse_args()

    config.set_account(args.account)
    log.info("Dashboard starting for account: %s", args.account)

    app.run(host="0.0.0.0", port=args.port, debug=False)
