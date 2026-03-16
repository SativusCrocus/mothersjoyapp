#!/usr/bin/env python3
"""
Mother's Joy Instagram Bot — CLI entry point.

Queue-based, one-post-per-invocation.
Usage: python main.py --account <name>

Each run:
  1. Set account context
  2. Clean stale queue items
  3. Refill queue if below threshold (scrape + AI filter + generate captions)
  4. Dequeue one item and post to Instagram
  5. Exit
"""

import argparse
import logging
import sys

from bot import config
from bot.queue import cleanup_stale, dequeue, enqueue, mark_posted, queue_size
from bot.scraper import discover_content_sync
from bot.ai_filter import passes_filter, generate_post
from bot.poster import post_to_instagram_sync

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("mothersjoy")


def refill_queue():
    """Scrape, filter, and enqueue content until queue is healthy."""
    current = queue_size()
    log.info("Queue size: %d (min: %d)", current, config.QUEUE_MIN_SIZE)

    if current >= config.QUEUE_MIN_SIZE:
        log.info("Queue is healthy — skipping refill")
        return

    log.info("Refilling queue...")

    # Discover content across all niches
    posts = discover_content_sync()
    log.info("Discovered %d candidate posts", len(posts))

    added = 0
    for post in posts:
        if queue_size() >= config.QUEUE_MIN_SIZE:
            log.info("Queue refilled to %d — stopping", queue_size())
            break

        # AI filter
        passes, score, reason = passes_filter(post)
        if not passes:
            log.info("  ✗ Filtered (score=%d): %s — %s", score, post["source_url"], reason)
            continue

        log.info("  ✓ Passed (score=%d): %s — %s", score, post["source_url"], reason)

        # Generate caption in Mother's Joy voice
        result = generate_post(post)
        caption = result.get("caption", "")
        if not caption:
            log.warning("  ✗ Caption generation failed for %s", post["source_url"])
            continue

        # Add generated caption to item
        post["generated_caption"] = caption
        post["ai_score"] = score
        post["ai_reason"] = reason

        if enqueue(post):
            added += 1

    log.info("Refill complete: added %d items (queue now: %d)", added, queue_size())


def post_one():
    """Dequeue one item and post to Instagram."""
    item = dequeue()
    if not item:
        log.warning("Nothing to post — queue is empty")
        return False

    source_url = item.get("source_url", "")
    log.info("Posting: %s", source_url)

    post_url = post_to_instagram_sync(item)

    if post_url:
        mark_posted(source_url, post_url)
        log.info("Posted successfully → %s", post_url)
        return True
    else:
        log.error("Posting failed for %s", source_url)
        return False


def run(account: str):
    """Full bot run: cleanup → refill → post one."""
    log.info("=" * 60)
    log.info("Mother's Joy Bot — account: %s", account)
    log.info("=" * 60)

    # 1. Set account context
    config.set_account(account)

    # 2. Clean stale queue items
    removed = cleanup_stale()
    if removed:
        log.info("Cleaned %d stale items", removed)

    # 3. Refill queue if needed
    refill_queue()

    # 4. Post one item
    success = post_one()

    log.info("=" * 60)
    log.info("Run complete (posted=%s, queue=%d)", success, queue_size())
    log.info("=" * 60)

    return success


def main():
    parser = argparse.ArgumentParser(
        description="Mother's Joy Instagram Bot 💜",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Example: python main.py --account mothersjoy",
    )
    parser.add_argument(
        "--account", "-a",
        required=True,
        help="Account name (matches accounts/<name>/ directory)",
    )
    args = parser.parse_args()

    success = run(args.account)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
