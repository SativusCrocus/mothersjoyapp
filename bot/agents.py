"""
Agent-style orchestration for Mother's Joy.

Each role owns one concern:
- DiscoveryAgent: finds candidate posts
- CuratorAgent: decides whether a post fits the feed
- CaptionAgent: rewrites approved posts in brand voice
- PublishingAgent: sends prepared items to Instagram

MotherJoyAgentTeam coordinates those roles and owns refill/post cycles.
"""

from __future__ import annotations

import logging
import random
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from itertools import islice
from typing import Iterable

from bot import config, queue as queue_store

log = logging.getLogger(__name__)


def _batched(items: Iterable[dict], size: int) -> Iterable[list[dict]]:
    """Yield fixed-size batches from an iterable."""
    iterator = iter(items)
    while True:
        batch = list(islice(iterator, size))
        if not batch:
            return
        yield batch


@dataclass(slots=True)
class CandidateDecision:
    accepted: bool
    post: dict
    score: int = 0
    reason: str = ""
    error: str = ""


@dataclass(slots=True)
class RefillReport:
    started_with: int
    discovered: int = 0
    reviewed: int = 0
    accepted: int = 0
    added: int = 0
    final_size: int = 0
    skipped: bool = False


@dataclass(slots=True)
class PublishReport:
    attempted: bool = False
    posted: bool = False
    skipped: bool = False
    source_url: str = ""
    post_url: str = ""
    retry_count: int = 0
    message: str = ""


@dataclass(slots=True)
class RunReport:
    cleaned: int
    refill: RefillReport
    publish: PublishReport


class DiscoveryAgent:
    """Collect fresh source content from Instagram."""

    def discover(self) -> list[dict]:
        from bot.scraper import discover_content_sync

        # discover_content_sync handles session expiry, API-first strategy,
        # and Playwright DOM fallback internally — no extra wrapping needed here.
        posts = discover_content_sync()
        log.info("Discovery agent collected %d candidates", len(posts))
        return posts


class CuratorAgent:
    """Approve or reject source content for the queue."""

    def review(self, post: dict) -> tuple[bool, int, str]:
        from bot.ai_filter import passes_filter

        return passes_filter(post)


class CaptionAgent:
    """Turn a source caption into Mother's Joy voice."""

    def compose(self, post: dict) -> str:
        from bot.ai_filter import generate_post

        return generate_post(post).get("caption", "")


class PublishingAgent:
    """Publish a prepared queue item to Instagram."""

    def publish(self, item: dict) -> str:
        from bot.poster import post_to_instagram_sync

        return post_to_instagram_sync(item)


# ── Growth engine agents ─────────────────────────────────────────────────

@dataclass(slots=True)
class EngagementReport:
    likes: int = 0
    comments: int = 0
    errors: int = 0
    skipped: bool = False
    blocked: bool = False
    cooldown_remaining: int = 0
    message: str = ""


@dataclass(slots=True)
class CommentReplyReport:
    checked: int = 0
    replies: int = 0
    errors: int = 0
    skipped: bool = False
    message: str = ""


@dataclass(slots=True)
class FollowReport:
    followed: int = 0
    unfollowed: int = 0
    errors: int = 0
    skipped: bool = False
    message: str = ""


class EngagementAgent:
    """Like and comment on niche posts to build community presence."""

    def engage(self) -> EngagementReport:
        from bot.engagement_store import daily_action_count
        from bot.rate_limiter import can_perform, cooldown_remaining, is_action_blocked
        from bot.poster import run_engagement_session_sync

        if not config.ENGAGEMENT_ENABLED:
            return EngagementReport(skipped=True, message="disabled")

        if is_action_blocked():
            return EngagementReport(skipped=True, message="action_blocked")

        # Check daily caps
        allowed, reason = can_perform("like")
        if not allowed:
            return EngagementReport(skipped=True, message=reason)

        # Check cooldown
        cd = cooldown_remaining()
        if cd > 0:
            return EngagementReport(skipped=True, cooldown_remaining=cd, message="cooldown")

        # Randomize counts for natural behavior
        likes = random.randint(
            max(1, config.ENGAGEMENT_LIKES_PER_SESSION - 4),
            config.ENGAGEMENT_LIKES_PER_SESSION + 3,
        )
        comments = random.randint(
            max(1, config.ENGAGEMENT_COMMENTS_PER_SESSION - 1),
            config.ENGAGEMENT_COMMENTS_PER_SESSION + 1,
        )

        # Pick random hashtags
        hashtags = random.sample(
            config.ENGAGEMENT_HASHTAGS,
            min(3, len(config.ENGAGEMENT_HASHTAGS)),
        )

        log.info("Starting engagement: %d likes, %d comments on %s", likes, comments, hashtags)
        result = run_engagement_session_sync(hashtags, likes, comments)

        return EngagementReport(
            likes=result.get("likes", 0),
            comments=result.get("comments", 0),
            errors=result.get("errors", 0),
            blocked=result.get("blocked", False),
            message="completed",
        )


class CommentReplyAgent:
    """Reply to comments on our recent posts."""

    def check_and_reply(self) -> CommentReplyReport:
        from bot.poster import run_reply_session_sync
        from bot.rate_limiter import can_perform

        if not config.COMMENT_REPLY_ENABLED:
            return CommentReplyReport(skipped=True, message="disabled")

        allowed, reason = can_perform("reply")
        if not allowed:
            return CommentReplyReport(skipped=True, message=reason)

        # Get recent posts (within reply age window)
        posted = queue_store.get_posted_history()
        cutoff = datetime.now(timezone.utc).timestamp() - (config.COMMENT_REPLY_MAX_POST_AGE_HOURS * 3600)

        recent_posts = []
        for item in reversed(posted):  # newest first
            posted_at = item.get("posted_at", "")
            try:
                ts = datetime.fromisoformat(posted_at).timestamp()
            except (ValueError, TypeError):
                continue
            if ts < cutoff:
                break
            post_link = item.get("post_link", "")
            if post_link:
                recent_posts.append({
                    "post_url": post_link,
                    "caption": item.get("caption", ""),
                })

        if not recent_posts:
            return CommentReplyReport(skipped=True, message="no_recent_posts")

        log.info("Checking %d recent posts for new comments", len(recent_posts))
        result = run_reply_session_sync(recent_posts[:10])  # cap at 10 posts per session

        return CommentReplyReport(
            checked=result.get("checked", 0),
            replies=result.get("replies", 0),
            errors=result.get("errors", 0),
            message="completed",
        )


class FollowAgent:
    """Follow source creators and manage unfollow cycle."""

    def follow_creator(self, account: str, source_post: str = "") -> bool:
        """Follow a single creator (called after posting their content)."""
        from bot.engagement_store import daily_follow_count
        from bot.poster import run_follow_session_sync
        from bot.rate_limiter import can_perform

        if not config.FOLLOW_CREATORS_ENABLED:
            return False

        allowed, reason = can_perform("follow")
        if not allowed:
            log.info("Follow skipped: %s", reason)
            return False

        result = run_follow_session_sync([{"account": account, "source_post": source_post}])
        return result.get("followed", 0) > 0

    def run_unfollow_sweep(self) -> FollowReport:
        """Unfollow accounts that haven't followed back after the grace period."""
        from bot.engagement_store import get_stale_follows
        from bot.poster import run_unfollow_session_sync

        if not config.FOLLOW_CREATORS_ENABLED:
            return FollowReport(skipped=True, message="disabled")

        stale = get_stale_follows()
        if not stale:
            return FollowReport(skipped=True, message="no_stale_follows")

        accounts = [f.get("account", "") for f in stale if f.get("account")]
        max_unfollow = getattr(config, "RATE_FOLLOW_PER_WINDOW", 25)
        log.info("Unfollowing %d stale accounts", len(accounts))
        result = run_unfollow_session_sync(accounts[:max_unfollow])

        return FollowReport(
            unfollowed=result.get("unfollowed", 0),
            errors=result.get("errors", 0),
            message="completed",
        )


class IntakeAgent:
    """Apply curation and captioning to one candidate post."""

    def __init__(
        self,
        curator: CuratorAgent | None = None,
        captioner: CaptionAgent | None = None,
    ):
        self.curator = curator or CuratorAgent()
        self.captioner = captioner or CaptionAgent()

    def prepare(self, post: dict) -> CandidateDecision:
        source_url = post.get("source_url", "")
        try:
            passes, score, reason = self.curator.review(post)
            if not passes:
                log.info("Curator agent rejected %s (score=%d): %s", source_url, score, reason)
                return CandidateDecision(False, post, score=score, reason=reason)

            from bot.ai_filter import generate_post
            caption_result = generate_post(post)
            caption = caption_result.get("caption", "")
            if not caption:
                # Last resort: use original caption so item isn't lost
                caption = (post.get("caption", "") or "").strip()
                if not caption:
                    log.warning("Caption agent failed (no fallback) for %s", source_url)
                    return CandidateDecision(
                        False,
                        post,
                        score=score,
                        reason=reason,
                        error="caption_generation_failed",
                    )
                log.info("Using original caption as fallback for %s", source_url)

            prepared = dict(post)
            prepared["generated_caption"] = caption
            prepared["ai_score"] = score
            prepared["ai_reason"] = reason

            log.info("Intake agent approved %s (score=%d)", source_url, score)
            return CandidateDecision(True, prepared, score=score, reason=reason)
        except Exception as exc:
            log.error("Intake agent error for %s: %s", source_url, exc)
            return CandidateDecision(False, post, error=str(exc))


class MotherJoyAgentTeam:
    """Shared orchestrator used by the CLI bot and dashboard."""

    def __init__(
        self,
        discovery: DiscoveryAgent | None = None,
        intake: IntakeAgent | None = None,
        publisher: PublishingAgent | None = None,
        engagement: EngagementAgent | None = None,
        reply_agent: CommentReplyAgent | None = None,
        follow_agent: FollowAgent | None = None,
        workers: int | None = None,
        batch_size: int | None = None,
    ):
        self.discovery = discovery or DiscoveryAgent()
        self.intake = intake or IntakeAgent()
        self.publisher = publisher or PublishingAgent()
        self.engagement = engagement or EngagementAgent()
        self.reply_agent = reply_agent or CommentReplyAgent()
        self.follow_agent = follow_agent or FollowAgent()
        self.workers = max(1, workers or config.AGENT_WORKERS)
        self.batch_size = max(self.workers, batch_size or config.AGENT_BATCH_SIZE)

    def _resolve_refill_target(self, current: int, target_size: int | None) -> int:
        if target_size is not None:
            return max(0, target_size)
        if current < config.QUEUE_MIN_SIZE:
            return config.QUEUE_TARGET_SIZE
        return current

    def _prepare_batch(self, batch: list[dict]) -> list[CandidateDecision]:
        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            return list(pool.map(self.intake.prepare, batch))

    def refill_queue(self, target_size: int | None = None, force: bool = False) -> RefillReport:
        current = queue_store.queue_size()
        report = RefillReport(started_with=current, final_size=current)
        refill_target = self._resolve_refill_target(current, target_size)

        if not force and current >= refill_target:
            report.skipped = True
            log.info("Agent team skipped refill: queue already healthy (%d >= %d)", current, refill_target)
            return report

        discovered_posts = self.discovery.discover()
        report.discovered = len(discovered_posts)

        prepared_posts: list[dict] = []
        remaining = max(0, refill_target - current)

        for batch in _batched(discovered_posts, self.batch_size):
            decisions = self._prepare_batch(batch)
            report.reviewed += len(decisions)

            for decision in decisions:
                if decision.accepted:
                    report.accepted += 1
                    prepared_posts.append(decision.post)

            if remaining is not None and len(prepared_posts) >= remaining:
                break

        report.added = queue_store.enqueue_many(prepared_posts, limit=remaining)
        report.final_size = queue_store.queue_size()

        log.info(
            "Agent refill complete: discovered=%d reviewed=%d accepted=%d added=%d queue=%d",
            report.discovered,
            report.reviewed,
            report.accepted,
            report.added,
            report.final_size,
        )
        return report

    def publish_next(self, dry_run: bool = False) -> PublishReport:
        # Check if posting subsystem is healthy
        try:
            from bot.supervisor import should_attempt
            allowed, reason = should_attempt("posting")
            if not allowed:
                return PublishReport(skipped=True, message=f"supervisor: {reason}")
        except Exception:
            pass

        if dry_run:
            preview = queue_store.peek_queue(1)
            if not preview:
                log.warning("Publishing agent found no queued work for dry run")
                return PublishReport(message="queue_empty")

            item = preview[0]
            log.info("Publishing agent dry run previewed %s", item.get("source_url", ""))
            return PublishReport(
                attempted=True,
                skipped=True,
                source_url=item.get("source_url", ""),
                retry_count=int(item.get("retry_count", 0)),
                message="dry_run",
            )

        item = queue_store.claim_next()
        if not item:
            log.warning("Publishing agent found no queued work")
            return PublishReport(message="queue_empty")

        token = item.get("claim_token", "")
        source_url = item.get("source_url", "")
        retries = int(item.get("retry_count", 0))

        report = PublishReport(
            attempted=True,
            source_url=source_url,
            retry_count=retries,
        )

        try:
            post_url = self.publisher.publish(item)

            if post_url and post_url not in ("", "SKIP", "RESCRAPE"):
                queue_store.complete_claim(token)
                queue_store.mark_posted(source_url, post_url)
                report.posted = True
                report.post_url = post_url
                report.message = "posted"
                log.info("Publishing agent posted %s -> %s", source_url, post_url)

                # Follow the source creator (non-blocking, best-effort)
                creator = item.get("account", item.get("creator_username", "")).lstrip("@")
                if creator and config.FOLLOW_CREATORS_ENABLED:
                    try:
                        self.follow_agent.follow_creator(creator, source_post=source_url)
                    except Exception as exc:
                        log.warning("Follow-on-publish failed for @%s: %s", creator, exc)

                return report

            if post_url == "SKIP":
                queue_store.fail_claim(token, stage="publish", reason="unrecoverable_skip")
                report.skipped = True
                report.message = "skipped"
                log.warning("Publishing agent skipped %s", source_url)
                return report

            if post_url == "RESCRAPE":
                # Media expired — release back to queue with delay so fresh items get posted first
                log.warning("Media expired for %s — releasing for rescrape (1hr delay)", source_url)
                from datetime import timedelta
                next_retry = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
                queue_store.release_claim(
                    token,
                    updates={
                        "retry_count": retries + 1,
                        "last_error": "media_expired_rescrape",
                        "media_url": "",  # clear expired URL
                        "next_retry_after": next_retry,
                    },
                )
                report.message = "rescrape_needed"
                return report

            # Empty post_url — publish failed but not fatally
            retries += 1
            report.retry_count = retries
            report.message = self._handle_publish_retry(
                token, source_url, retries, "publish_returned_empty",
            )
            return report

        except Exception as exc:
            retries += 1
            report.retry_count = retries
            report.message = self._handle_publish_retry(
                token, source_url, retries, str(exc),
            )
            log.error("Publishing agent error for %s: %s", source_url, exc)
            return report

    @staticmethod
    def _handle_publish_retry(
        token: str, source_url: str, retries: int, error: str,
    ) -> str:
        """Diagnose failure and decide retry strategy. NEVER dead-letter permanently."""
        from datetime import datetime, timezone

        try:
            from bot.supervisor import diagnose_failure
            diag = diagnose_failure("posting", error)
            wait = diag.wait_seconds
        except Exception:
            wait = min(300 * (2 ** min(retries, 5)), 7200)  # fallback: 5min to 2hr

        # For items with many retries, increase wait but NEVER abandon
        if retries >= 10:
            wait = max(wait, 3600)   # at least 1 hour
        elif retries >= 5:
            wait = max(wait, 600)    # at least 10 minutes

        next_retry = datetime.now(timezone.utc).isoformat()
        if wait > 0:
            from datetime import timedelta
            next_retry = (datetime.now(timezone.utc) + timedelta(seconds=wait)).isoformat()

        queue_store.release_claim(
            token,
            updates={
                "retry_count": retries,
                "last_error": error[:500],
                "next_retry_after": next_retry,
            },
        )
        log.info(
            "Publishing agent re-queued %s (retry %d, wait %ds): %s",
            source_url, retries, wait, error[:100],
        )
        return "requeued"

    def resurrect_failed(self, max_items: int = 10) -> int:
        """Move resurrectable items from failed back to queue."""
        return queue_store.resurrect_failed(max_items)

    def run_engagement(self) -> EngagementReport:
        """Run a community engagement session (likes + comments on niche posts)."""
        try:
            from bot.supervisor import should_attempt
            allowed, reason = should_attempt("engagement")
            if not allowed:
                return EngagementReport(skipped=True, message=f"supervisor: {reason}")
        except Exception:
            pass
        return self.engagement.engage()

    def run_replies(self) -> CommentReplyReport:
        """Check recent posts for new comments and reply."""
        return self.reply_agent.check_and_reply()

    def run_unfollow_sweep(self) -> FollowReport:
        """Unfollow stale accounts that haven't followed back."""
        return self.follow_agent.run_unfollow_sweep()

    def run_cycle(self, target_size: int | None = None, dry_run: bool = False) -> RunReport:
        cleaned = queue_store.cleanup_stale()
        current = queue_store.queue_size()

        # Keep posting momentum when there is already work queued; refill can
        # happen after the post instead of blocking it.
        if current > 0:
            publish = self.publish_next(dry_run=dry_run)
            refill = self.refill_queue(target_size=target_size)
        else:
            refill = self.refill_queue(target_size=target_size)
            publish = self.publish_next(dry_run=dry_run)

        return RunReport(cleaned=cleaned, refill=refill, publish=publish)
