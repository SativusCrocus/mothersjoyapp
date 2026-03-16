"""
Instagram content discovery via Playwright.

Searches hashtag pages, intercepts GraphQL/API responses to extract
post data, then filters by engagement, age, blocked lists, and caption quality.
"""

import asyncio
import json
import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from playwright.async_api import async_playwright, Page, BrowserContext

from bot import config

log = logging.getLogger(__name__)


# ── Helpers ──────────────────────────────────────────────────────────────────

def _niche_to_hashtag(niche: str) -> str:
    """Convert a niche phrase to a hashtag slug: 'UK parenting tips' → 'ukparentingtips'."""
    return re.sub(r"[^a-z0-9]", "", niche.lower())


def _caption_text(node: dict) -> str:
    """Extract caption string from a GraphQL media node."""
    edges = (node.get("edge_media_to_caption") or {}).get("edges", [])
    if edges:
        return edges[0].get("node", {}).get("text", "")
    return node.get("caption", {}).get("text", "") if isinstance(node.get("caption"), dict) else ""


def _is_blocked(caption: str, owner: str) -> bool:
    """Check caption + owner against blocked lists."""
    caption_lower = caption.lower()

    if owner.lower() in (a.lower() for a in config.BLOCKED_ACCOUNTS):
        return True

    for tag in config.BLOCKED_HASHTAGS:
        if tag.lower() in caption_lower:
            return True

    for pattern in config.BLOCKED_PATTERNS:
        if pattern.lower() in caption_lower:
            return True

    return False


def _passes_hard_filters(post: dict) -> bool:
    """Apply all hard filters before AI scoring."""
    caption = post.get("caption", "")
    owner = post.get("account", "")

    if _is_blocked(caption, owner):
        log.debug("Blocked: %s (%s)", post.get("source_url"), owner)
        return False

    if len(caption) < config.MIN_CAPTION_LENGTH:
        log.debug("Caption too short (%d chars): %s", len(caption), post.get("source_url"))
        return False

    # Age check
    taken_at = post.get("taken_at")
    if taken_at:
        age_hours = (time.time() - taken_at) / 3600
        if age_hours > config.SCRAPE_HOURS_BACK:
            log.debug("Too old (%.0fh): %s", age_hours, post.get("source_url"))
            return False

    return True


# ── GraphQL response parsing ────────────────────────────────────────────────

def _extract_posts_from_graphql(data: dict) -> list[dict]:
    """Parse Instagram GraphQL JSON into flat post dicts."""
    posts = []

    # Hashtag page: data.hashtag.edge_hashtag_to_media / edge_hashtag_to_top_posts
    hashtag = data.get("data", {}).get("hashtag") or data.get("graphql", {}).get("hashtag") or {}
    for edge_key in ("edge_hashtag_to_media", "edge_hashtag_to_top_posts"):
        edges = (hashtag.get(edge_key) or {}).get("edges", [])
        for edge in edges:
            node = edge.get("node", {})
            posts.append(_node_to_post(node))

    # Explore / recent media responses (API v1 style)
    sections = data.get("sections", [])
    for section in sections:
        medias = section.get("layout_content", {}).get("medias", [])
        for media_wrapper in medias:
            media = media_wrapper.get("media", {})
            posts.append(_api_media_to_post(media))

    items = data.get("items", [])
    for item in items:
        posts.append(_api_media_to_post(item))

    return [p for p in posts if p.get("source_url")]


def _node_to_post(node: dict) -> dict:
    """Convert a GraphQL media node to our post dict."""
    shortcode = node.get("shortcode", "")
    owner = node.get("owner", {})
    is_video = node.get("is_video", False)

    return {
        "account": owner.get("username", ""),
        "source_url": f"https://www.instagram.com/p/{shortcode}/" if shortcode else "",
        "caption": _caption_text(node),
        "likes": (node.get("edge_liked_by") or node.get("edge_media_preview_like") or {}).get("count", 0),
        "media_type": "video" if is_video else "image",
        "media_url": node.get("video_url") or node.get("display_url", ""),
        "taken_at": node.get("taken_at_timestamp", 0),
    }


def _api_media_to_post(media: dict) -> dict:
    """Convert an API v1 media object to our post dict."""
    user = media.get("user", {})
    shortcode = media.get("code", "") or media.get("shortcode", "")
    media_type = media.get("media_type", 1)  # 1=image, 2=video

    caption_obj = media.get("caption") or {}
    caption = caption_obj.get("text", "") if isinstance(caption_obj, dict) else ""

    # Best image URL
    candidates = media.get("image_versions2", {}).get("candidates", [])
    image_url = candidates[0].get("url", "") if candidates else ""

    video_url = ""
    video_versions = media.get("video_versions", [])
    if video_versions:
        video_url = video_versions[0].get("url", "")

    return {
        "account": user.get("username", ""),
        "source_url": f"https://www.instagram.com/p/{shortcode}/" if shortcode else "",
        "caption": caption,
        "likes": media.get("like_count", 0),
        "media_type": "video" if media_type == 2 else "image",
        "media_url": video_url or image_url,
        "taken_at": media.get("taken_at", 0),
    }


# ── Browser state ───────────────────────────────────────────────────────────

def _build_storage_state(cookies_path: Path) -> dict:
    """Convert cookies.json to Playwright storage state format."""
    if not cookies_path.exists():
        return {"cookies": [], "origins": []}

    raw = json.loads(cookies_path.read_text(encoding="utf-8"))

    # Already in storage-state format
    if isinstance(raw, dict) and "cookies" in raw:
        return raw

    # Flat cookie list — wrap it
    if isinstance(raw, list):
        cookies = []
        for c in raw:
            cookie = {
                "name": c.get("name", ""),
                "value": c.get("value", ""),
                "domain": c.get("domain", ".instagram.com"),
                "path": c.get("path", "/"),
                "secure": c.get("secure", True),
                "httpOnly": c.get("httpOnly", False),
                "sameSite": c.get("sameSite", "None"),
            }
            if c.get("expires") or c.get("expirationDate"):
                cookie["expires"] = c.get("expires") or c.get("expirationDate")
            cookies.append(cookie)
        return {"cookies": cookies, "origins": []}

    return {"cookies": [], "origins": []}


async def _create_context(playwright) -> BrowserContext:
    """Launch headless Chromium with Instagram cookies."""
    browser = await playwright.chromium.launch(headless=True)

    storage = _build_storage_state(config.get_cookies_path())

    # Write temp storage state file for Playwright
    tmp_state = config.get_account_dir() / "_tmp_storage_state.json"
    tmp_state.write_text(json.dumps(storage), encoding="utf-8")

    context = await browser.new_context(
        storage_state=str(tmp_state),
        viewport={"width": 430, "height": 932},
        user_agent=(
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) "
            "Version/17.0 Mobile/15E148 Safari/604.1"
        ),
    )

    tmp_state.unlink(missing_ok=True)
    return context


# ── Scrape one hashtag ───────────────────────────────────────────────────────

async def _scrape_hashtag(context: BrowserContext, hashtag: str, limit: int) -> list[dict]:
    """Visit a hashtag page and collect posts via API interception."""
    collected: list[dict] = []
    page = await context.new_page()

    async def on_response(response):
        url = response.url
        if not any(k in url for k in ("graphql", "api/v1/tags", "api/v1/feed", "web/explore")):
            return
        try:
            data = await response.json()
            posts = _extract_posts_from_graphql(data)
            collected.extend(posts)
        except Exception:
            pass

    page.on("response", on_response)

    try:
        await page.goto(
            f"https://www.instagram.com/explore/tags/{hashtag}/",
            wait_until="domcontentloaded",
            timeout=30000,
        )
        await page.wait_for_timeout(4000)

        # Scroll to trigger more API loads
        for _ in range(3):
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(2500)

    except Exception as exc:
        log.warning("Error scraping #%s: %s", hashtag, exc)
    finally:
        await page.close()

    # Deduplicate by source_url
    seen = set()
    unique = []
    for p in collected:
        url = p.get("source_url", "")
        if url and url not in seen:
            seen.add(url)
            unique.append(p)

    log.info("#%s → %d unique posts found", hashtag, len(unique))
    return unique[:limit]


# ── Public API ───────────────────────────────────────────────────────────────

async def discover_content(niches: list[str] | None = None) -> list[dict]:
    """
    Scrape Instagram for trending content across all configured niches.
    Returns a list of post dicts that pass hard filters, sorted by likes desc.
    """
    niches = niches or config.NICHES
    all_posts: list[dict] = []

    async with async_playwright() as pw:
        context = await _create_context(pw)

        for niche in niches:
            hashtag = _niche_to_hashtag(niche)
            if not hashtag:
                continue

            posts = await _scrape_hashtag(context, hashtag, config.SEARCH_PER_NICHE)
            all_posts.extend(posts)

            # Polite delay between hashtag searches
            await asyncio.sleep(2)

        await context.browser.close()

    # Apply hard filters
    filtered = [p for p in all_posts if _passes_hard_filters(p)]

    # Deduplicate across niches
    seen = set()
    deduped = []
    for p in filtered:
        url = p["source_url"]
        if url not in seen:
            seen.add(url)
            deduped.append(p)

    # Sort by engagement (likes descending)
    deduped.sort(key=lambda p: p.get("likes", 0), reverse=True)

    log.info("Discovery complete: %d posts after filters (from %d raw)", len(deduped), len(all_posts))
    return deduped


def discover_content_sync(niches: list[str] | None = None) -> list[dict]:
    """Synchronous wrapper for discover_content."""
    return asyncio.run(discover_content(niches))
