"""
Instagram posting via Playwright (headless Chromium, desktop mode).

Loads browser state from cookies, opens create modal via sidebar,
uploads media, types caption, clicks Share, and extracts the post ID
from API interception or profile fallback.
"""

import asyncio
import logging
import re
import tempfile
import time
from pathlib import Path

import requests
from PIL import Image
from playwright.async_api import async_playwright, BrowserContext, Page

from bot import config
from bot.instagram_auth import (
    AuthenticationRequired,
    create_authenticated_context,
    persist_storage_state,
)
from bot.queue import already_posted, get_posted_history

log = logging.getLogger(__name__)

# Desktop browser config — Instagram only shows the create button on desktop
_VIEWPORT = {"width": 1280, "height": 900}
_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)

# ── Browser Context Manager ────────────────────────────────────────────────

_MAX_CONTEXT_AGE = 1800       # 30 minutes
_MAX_CONTEXT_ACTIONS = 50     # auto-restart after this many actions


class _BrowserSession:
    """Manages a single browser context lifecycle with auto-restart."""

    def __init__(self):
        self._pw = None
        self._context: BrowserContext | None = None
        self._created_at: float = 0.0
        self._action_count: int = 0

    def _needs_restart(self) -> bool:
        if self._context is None:
            return True
        if time.time() - self._created_at > _MAX_CONTEXT_AGE:
            return True
        if self._action_count >= _MAX_CONTEXT_ACTIONS:
            return True
        return False

    async def get_context(self, pw) -> BrowserContext:
        """Return an active context, creating or restarting if needed."""
        if self._needs_restart():
            await self._close()
            self._pw = pw
            self._context = await create_authenticated_context(pw)
            self._created_at = time.time()
            self._action_count = 0
            log.info("Browser context created (fresh session)")
        return self._context

    def record_action(self):
        self._action_count += 1

    async def save_state(self):
        if self._context:
            try:
                await persist_storage_state(self._context)
            except Exception as exc:
                log.warning("Failed to save browser state: %s", exc)

    async def _close(self):
        if self._context:
            try:
                await self.save_state()
            except Exception:
                pass
            try:
                await self._context.browser.close()
            except Exception:
                pass
            self._context = None

    async def close(self):
        await self._close()
_PROFILE_POST_LINK_SELECTOR = (
    'article a[href*="/p/"], '
    'article a[href*="/reel/"], '
    'a[href*="/p/"], '
    'a[href*="/reel/"]'
)


# ── Media download ───────────────────────────────────────────────────────────

def _download_media(url: str, media_type: str, source_url: str = "") -> Path | None:
    """Download media, checking cache first. Returns path or None.
    Returns sentinel string "RESCRAPE" (via exception) when media is expired."""
    if not url:
        return None

    # Check media cache first
    if source_url:
        try:
            from bot.media_cache import get_cached_path
            cached = get_cached_path(source_url)
            if cached:
                log.info("Media cache hit: %s", cached.name)
                return cached
        except Exception:
            pass

    ext = ".mp4" if media_type == "video" else ".jpg"
    try:
        # Use proxy for media downloads if configured
        session = requests.Session()
        try:
            from bot.proxy import apply_to_requests
            apply_to_requests(session)
        except Exception:
            pass

        resp = session.get(url, timeout=30, stream=True)

        # Detect expired CDN URLs (403/410)
        if resp.status_code in (403, 410):
            log.warning("Media URL expired (HTTP %d) — needs rescrape", resp.status_code)
            return None

        resp.raise_for_status()

        tmp = Path(tempfile.mktemp(suffix=ext, prefix="mjbot_"))
        with open(tmp, "wb") as f:
            for chunk in resp.iter_content(8192):
                f.write(chunk)

        log.info("Downloaded media → %s (%.1f KB)", tmp.name, tmp.stat().st_size / 1024)

        # Cache for future use
        if source_url:
            try:
                from bot.media_cache import cache_media
                cache_media(source_url, url, media_type)
            except Exception as exc:
                log.debug("Media cache store failed: %s", exc)

        return tmp

    except requests.HTTPError as exc:
        if exc.response is not None and exc.response.status_code in (403, 410):
            log.warning("Media URL expired: %s", exc)
            return None
        log.error("Media download failed: %s", exc)
        return None
    except Exception as exc:
        log.error("Media download failed: %s", exc)
        return None


# ── Image processing ─────────────────────────────────────────────────────────

# Instagram aspect ratio limits: 4:5 (portrait) to 1.91:1 (landscape)
_IG_MAX_SIZE = 1080
_IG_MIN_RATIO = 4 / 5    # 0.8  (tallest allowed)
_IG_MAX_RATIO = 1.91     # widest allowed


def _prepare_image(path: Path) -> Path:
    """
    Resize and crop image for Instagram.
    - Scale to 1080px on longest side
    - Crop to valid IG aspect ratio if needed (4:5 to 1.91:1)
    - Save as high-quality JPEG
    Returns path to the processed image (may be same file, overwritten).
    """
    try:
        img = Image.open(path)

        # Convert RGBA/P to RGB
        if img.mode in ("RGBA", "P"):
            img = img.convert("RGB")

        w, h = img.size
        ratio = w / h

        # Crop to valid aspect ratio
        if ratio < _IG_MIN_RATIO:
            # Too tall — crop height to 4:5
            new_h = int(w / _IG_MIN_RATIO)
            top = (h - new_h) // 2
            img = img.crop((0, top, w, top + new_h))
            log.info("Cropped tall image: %dx%d → %dx%d", w, h, w, new_h)
        elif ratio > _IG_MAX_RATIO:
            # Too wide — crop width to 1.91:1
            new_w = int(h * _IG_MAX_RATIO)
            left = (w - new_w) // 2
            img = img.crop((left, 0, left + new_w, h))
            log.info("Cropped wide image: %dx%d → %dx%d", w, h, new_w, h)

        # Scale to 1080px on the largest dimension
        w, h = img.size
        if max(w, h) > _IG_MAX_SIZE:
            if w >= h:
                new_w = _IG_MAX_SIZE
                new_h = int(h * (_IG_MAX_SIZE / w))
            else:
                new_h = _IG_MAX_SIZE
                new_w = int(w * (_IG_MAX_SIZE / h))
            img = img.resize((new_w, new_h), Image.LANCZOS)
            log.info("Resized image to %dx%d", new_w, new_h)

        # Save as high-quality JPEG
        out_path = path.with_suffix(".jpg")
        img.save(out_path, "JPEG", quality=95, optimize=True)
        log.info("Image prepared: %dx%d (%.1f KB)", img.size[0], img.size[1], out_path.stat().st_size / 1024)

        # Remove original if different path
        if out_path != path and path.exists():
            path.unlink(missing_ok=True)

        return out_path

    except Exception as exc:
        log.warning("Image processing failed (using original): %s", exc)
        return path


async def _create_context(playwright) -> BrowserContext:
    """Launch headless Chromium with an authenticated Instagram session (desktop mode).
    Uses stealth fingerprint when available, falls back to defaults."""
    return await create_authenticated_context(playwright)


async def _save_state(context: BrowserContext):
    """Persist browser state for next run."""
    await persist_storage_state(context)


# ── Post creation ────────────────────────────────────────────────────────────

def _shortcode_from_href(href: str | None) -> str:
    if not href:
        return ""

    match = re.search(r"/(?:p|reel)/([^/]+)/", href)
    return match.group(1) if match else ""


def _is_new_profile_post(previous_post_id: str, latest_post_id: str) -> bool:
    return bool(latest_post_id) and latest_post_id != previous_post_id


def _absolute_instagram_url(href: str | None) -> str:
    if not href:
        return ""
    return href if href.startswith("http") else f"https://www.instagram.com{href}"


def _find_new_profile_post(
    previous_posts: set[str],
    latest_posts: list[str],
    known_post_links: set[str] | None = None,
) -> str:
    known_post_links = known_post_links or set()
    for post_url in latest_posts:
        if post_url and post_url not in previous_posts and post_url not in known_post_links:
            return post_url
    return ""


async def _recent_profile_post_urls(page: Page, limit: int = 12, attempts: int = 3) -> list[str]:
    """Read the recent profile-grid permalinks, including reels and pinned posts."""
    username = config.get_instagram_username()
    if not username:
        return []

    for attempt in range(max(1, attempts)):
        await page.goto(
            f"https://www.instagram.com/{username}/",
            wait_until="domcontentloaded",
            timeout=15000,
        )
        await page.wait_for_timeout(3000 + (attempt * 1000))

        post_links = page.locator(_PROFILE_POST_LINK_SELECTOR)
        try:
            await post_links.first.wait_for(state="visible", timeout=4000)
        except Exception:
            pass

        count = await post_links.count()
        if count == 0:
            continue

        urls: list[str] = []
        seen: set[str] = set()

        for index in range(min(count, limit)):
            href = await post_links.nth(index).get_attribute("href")
            url = _absolute_instagram_url(href)
            if url and url not in seen:
                seen.add(url)
                urls.append(url)

        if urls:
            return urls

    log.warning("Could not read profile post links from %s", page.url)
    return []


async def _post_content(context: BrowserContext, media_path: Path, caption: str,
                        media_type: str = "image") -> str:
    """
    Create an Instagram post via the desktop web UI.
    Returns the post URL if successful, empty string on failure.
    """
    is_video = media_type == "video"
    page = await context.new_page()
    post_id = ""
    post_url = ""
    previous_profile_posts: set[str] = set()

    # Intercept CreatePost API response (REST and GraphQL endpoints)
    async def on_response(response):
        nonlocal post_id
        if post_id:
            return
        url = response.url
        # Check REST configure endpoints
        rest_match = any(k in url for k in (
            "create/configure", "media/configure", "media_publish",
            "configure_to_igtv", "configure_sidecar",
            "configure_to_clips", "clip/create",
        ))
        # Check GraphQL endpoints (Instagram now routes post creation here)
        graphql_match = "graphql" in url and response.request.method == "POST"
        if not rest_match and not graphql_match:
            return
        try:
            data = await response.json()
            # REST-style: {"media": {"code": "..."}}
            media = data.get("media", {})
            code = media.get("code", "") or media.get("shortcode", "")
            if code:
                post_id = code
                log.info("Intercepted post ID: %s", code)
                return
            # GraphQL-style: dig for media code in nested response
            flat = str(data)
            if not graphql_match or "media" not in flat:
                return
            # Look for a new shortcode pattern in the response
            codes = re.findall(r'"code":\s*"([A-Za-z0-9_-]{8,})"', flat)
            if codes:
                post_id = codes[0]
                log.info("Intercepted post ID (graphql): %s", post_id)
        except Exception:
            pass

    page.on("response", on_response)

    try:
        # 1. Navigate to Instagram
        log.info("Opening Instagram (desktop)...")
        await page.goto("https://www.instagram.com/", wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(4000)

        # Dismiss popups (notifications, cookies, etc.)
        for dismiss_text in ["Not Now", "Decline", "Cancel", "Dismiss"]:
            btn = page.get_by_text(dismiss_text, exact=True)
            if await btn.count() > 0:
                await btn.first.click()
                await page.wait_for_timeout(1000)

        previous_profile_posts = set(await _recent_profile_post_urls(page, attempts=5))
        if previous_profile_posts:
            preview = ", ".join(list(previous_profile_posts)[:3])
            log.info("Recent profile posts before publish: %s", preview)
        else:
            log.warning("Could not capture a pre-publish profile baseline")

        # 2. Open create modal: click "New post" SVG in sidebar
        log.info("Opening create modal...")
        new_post_svg = page.locator('svg[aria-label="New post"]')
        if await new_post_svg.count() == 0:
            log.error("Could not find 'New post' SVG in sidebar")
            return ""

        await new_post_svg.first.click()
        await page.wait_for_timeout(1500)

        # 3. Click "Post" from the expanded submenu
        log.info("Clicking 'Post' submenu...")
        post_submenu = page.get_by_text("Post", exact=True)
        if await post_submenu.count() > 0:
            await post_submenu.first.click()
            await page.wait_for_timeout(2000)
        else:
            log.warning("No 'Post' submenu found — modal may have opened directly")

        # 4. Upload media file via the hidden file input
        log.info("Uploading media: %s", media_path.name)
        file_input = page.locator('input[type="file"]')
        try:
            await file_input.wait_for(timeout=5000)
            await file_input.set_input_files(str(media_path))
        except Exception:
            # Fallback: click "Select from computer" first
            select_btn = page.get_by_text("Select from computer")
            if await select_btn.count() > 0:
                async with page.expect_file_chooser() as fc_info:
                    await select_btn.first.click()
                file_chooser = await fc_info.value
                await file_chooser.set_files(str(media_path))
            else:
                log.error("Could not find file input or 'Select from computer' button")
                return ""

        # Wait for media to be processed (videos need much longer)
        upload_wait = 15000 if is_video else 3000
        log.info("Waiting %ds for %s processing...", upload_wait // 1000, media_type)
        await page.wait_for_timeout(upload_wait)

        # Handle "Videos will be shared as reels" or similar dialogs
        if is_video:
            for dismiss_text in ["OK", "Continue", "Got it", "Not Now"]:
                try:
                    btn = page.get_by_role("button", name=dismiss_text)
                    if await btn.count() > 0:
                        log.info("Dismissing video dialog: '%s'", dismiss_text)
                        await btn.first.click()
                        await page.wait_for_timeout(1500)
                except Exception:
                    pass

        # 5. Click Next through crop → filter → caption steps
        #    Wait for button to appear instead of silently skipping (critical for videos)
        for step in range(3):
            next_btn = page.get_by_role("button", name="Next")
            wait_timeout = 30000 if is_video else 10000
            try:
                await next_btn.first.wait_for(state="visible", timeout=wait_timeout)
                log.info("Clicking Next (step %d)...", step + 1)
                await next_btn.first.click()
                await page.wait_for_timeout(3000 if is_video else 2000)
            except Exception:
                log.warning("Next button not found at step %d — may have fewer steps", step + 1)
                # For videos, also check for "Continue" button (Reel flow variant)
                if is_video:
                    try:
                        cont_btn = page.get_by_role("button", name="Continue")
                        if await cont_btn.count() > 0:
                            log.info("Clicking Continue (video alt) at step %d", step + 1)
                            await cont_btn.first.click()
                            await page.wait_for_timeout(2000)
                    except Exception:
                        pass

        # 6. Type caption (human-like typing via stealth)
        log.info("Typing caption (%d chars)...", len(caption))
        caption_selector = (
            '[aria-label="Write a caption..."], '
            '[aria-label="Write a caption…"], '
            'div[contenteditable="true"], '
            'textarea'
        )
        caption_area = page.locator(caption_selector)

        if await caption_area.count() > 0:
            await caption_area.first.click()
            await page.wait_for_timeout(500)
            try:
                from bot.stealth import human_type
                await human_type(page, caption_selector, caption)
            except Exception:
                await page.keyboard.type(caption, delay=10)
            log.info("Caption typed successfully")
        else:
            log.warning("Could not find caption input — posting without caption")

        await page.wait_for_timeout(1000)

        # 7. Click Share
        log.info("Clicking Share...")
        share_btn = page.get_by_role("button", name="Share")
        if await share_btn.count() > 0:
            await share_btn.first.click()
        else:
            # Fallback: look for div with Share text
            share_div = page.locator('div:text-is("Share")')
            if await share_div.count() > 0:
                await share_div.first.click()
            else:
                log.error("Could not find Share button")
                return ""

        # Wait for the post to be created (videos need longer for server processing)
        share_wait = (
            config.VIDEO_POST_SHARE_WAIT_SECONDS * 1000
            if is_video
            else 12000
        )
        log.info("Waiting %ds for %s post creation...", share_wait // 1000, media_type)
        await page.wait_for_timeout(share_wait)

        # 8. Check for "shared" confirmation text (Instagram shows this after reels/posts)
        if not post_id:
            try:
                shared_indicators = page.locator(
                    'text="Your reel has been shared",'
                    'text="Your post has been shared",'
                    'text="Reel shared",'
                    'text="Post shared",'
                    'img[alt="Animated checkmark"]'
                )
                if await shared_indicators.count() > 0:
                    log.info("Share confirmation dialog detected — post went through")
            except Exception:
                pass

        # 9. Fallback: extract post ID from profile if not intercepted
        if not post_id and previous_profile_posts:
            log.info("Post ID not intercepted — checking profile grid for a new post")
            try:
                attempts = max(1, int(config.PROFILE_CONFIRMATION_ATTEMPTS))
                wait_ms = max(1000, int(config.PROFILE_CONFIRMATION_WAIT_SECONDS) * 1000)
                known_post_links = {
                    item.get("post_link", "")
                    for item in get_posted_history()
                    if item.get("post_link")
                }

                for attempt in range(attempts):
                    # Cache-bust by appending a unique query param
                    username = config.get_instagram_username()
                    await page.goto(
                        f"https://www.instagram.com/{username}/?_cb={int(asyncio.get_event_loop().time() * 1000)}",
                        wait_until="domcontentloaded",
                        timeout=15000,
                    )
                    await page.wait_for_timeout(3000 + (attempt * 2000))

                    post_links = page.locator(_PROFILE_POST_LINK_SELECTOR)
                    try:
                        await post_links.first.wait_for(state="visible", timeout=5000)
                    except Exception:
                        pass

                    count = await post_links.count()
                    latest_posts: list[str] = []
                    seen: set[str] = set()
                    for idx in range(min(count, 12)):
                        href = await post_links.nth(idx).get_attribute("href")
                        url = _absolute_instagram_url(href)
                        if url and url not in seen:
                            seen.add(url)
                            latest_posts.append(url)

                    detected_post_url = _find_new_profile_post(
                        previous_profile_posts,
                        latest_posts,
                        known_post_links=known_post_links,
                    )
                    if detected_post_url:
                        post_url = detected_post_url
                        log.info("Detected new profile post URL: %s", post_url)
                        break

                    if latest_posts:
                        log.warning(
                            "Profile fallback still shows only known posts (attempt %d/%d)",
                            attempt + 1,
                            attempts,
                        )

                    if attempt + 1 < attempts:
                        await page.wait_for_timeout(wait_ms)
            except Exception as exc:
                log.warning("Profile fallback failed: %s", exc)
        elif not post_id:
            log.warning("Skipping profile fallback because no pre-publish profile baseline was available")

    except Exception as exc:
        log.error("Posting failed: %s", exc)
    finally:
        await page.close()

    if post_url:
        return post_url
    if post_id:
        return f"https://www.instagram.com/p/{post_id}/"
    return ""


# ── Public API ───────────────────────────────────────────────────────────────

async def post_to_instagram(item: dict) -> str:
    """
    Post a queue item to Instagram.
    Downloads media, processes images, creates post, saves state.
    Returns the post URL, empty string on failure, or "RESCRAPE" when media expired.
    """
    source_url = item.get("source_url", "")
    media_url = item.get("media_url", "")
    media_type = item.get("media_type", "image")
    caption = item.get("generated_caption", "")

    # Pre-post dedup check (belt + suspenders)
    if already_posted(source_url):
        log.warning("Pre-post dedup caught duplicate: %s", source_url)
        return ""

    if not caption:
        log.error("No generated caption — cannot post")
        return "SKIP"

    # Download media (checks cache first)
    media_path = _download_media(media_url, media_type, source_url=source_url)
    if not media_path:
        log.warning("Media unavailable (URL may be expired) — returning RESCRAPE")
        return "RESCRAPE"

    # Process images for proper Instagram sizing
    if media_type in ("image", "carousel"):
        media_path = _prepare_image(media_path)

    post_url = ""
    try:
        async with async_playwright() as pw:
            context = await _create_context(pw)

            post_url = await _post_content(context, media_path, caption, media_type)

            # Share to story if enabled and post was successful
            if post_url and post_url not in ("", "SKIP", "RESCRAPE") and config.STORY_SHARE_ENABLED:
                try:
                    log.info("Waiting %ds before story share...", config.STORY_SHARE_DELAY_SECONDS)
                    await asyncio.sleep(config.STORY_SHARE_DELAY_SECONDS)
                    await share_post_to_story(context, post_url)
                except Exception as exc:
                    log.warning("Story share failed (non-fatal): %s", exc)

            # Save updated browser state
            try:
                await _save_state(context)
            except Exception as exc:
                log.warning("Failed to save browser state (non-fatal): %s", exc)

            try:
                await context.browser.close()
            except Exception as exc:
                log.warning("Browser close error (non-fatal): %s", exc)

        # Report success/failure to health
        try:
            from bot.health import get_registry
            if post_url and post_url not in ("", "SKIP", "RESCRAPE"):
                get_registry().report_success("posting")
            else:
                get_registry().report_failure("posting", f"post_result={post_url or 'empty'}")
        except Exception:
            pass

    except AuthenticationRequired:
        log.error("Posting failed: authentication required")
        try:
            from bot.health import get_registry
            get_registry().report_failure("posting", "auth_required")
        except Exception:
            pass
        return ""

    except Exception as exc:
        log.error("Posting failed: %s", exc, exc_info=True)
        try:
            from bot.health import get_registry
            get_registry().report_failure("posting", str(exc))
        except Exception:
            pass

    finally:
        # Clean up temp media file (never leave orphans)
        if media_path and media_path.exists():
            # Don't delete if it's a cached file
            try:
                from bot.media_cache import get_cached_path
                if get_cached_path(source_url) != media_path:
                    media_path.unlink(missing_ok=True)
            except Exception:
                media_path.unlink(missing_ok=True)

    return post_url


def post_to_instagram_sync(item: dict) -> str:
    """Synchronous wrapper for post_to_instagram."""
    return asyncio.run(post_to_instagram(item))


# ── Engagement actions ───────────────────────────────────────────────────

async def _check_for_action_block(page: Page) -> bool:
    """Detect Instagram 'Action Blocked' or 'Try Again Later' dialogs."""
    try:
        blocked = page.locator(
            'text="Action Blocked",'
            'text="Try Again Later",'
            'text="action was blocked",'
            'text="We restrict certain activity"'
        )
        if await blocked.count() > 0:
            log.warning("Instagram ACTION BLOCK detected!")
            # Try to dismiss
            for dismiss in ["OK", "Tell Us", "Got it"]:
                btn = page.get_by_role("button", name=dismiss)
                if await btn.count() > 0:
                    await btn.first.click()
                    await page.wait_for_timeout(1000)
            return True
    except Exception:
        pass
    return False


async def like_post(context: BrowserContext, post_url: str) -> bool:
    """Navigate to a post and like it. Returns True if liked."""
    page = await context.new_page()
    try:
        await page.goto(post_url, wait_until="domcontentloaded", timeout=20000)
        await page.wait_for_timeout(2000)

        if await _check_for_action_block(page):
            return False

        # Find the like button (heart SVG)
        like_btn = page.locator('svg[aria-label="Like"]')
        if await like_btn.count() == 0:
            # Already liked or button not found
            unlike = page.locator('svg[aria-label="Unlike"]')
            if await unlike.count() > 0:
                log.info("Already liked: %s", post_url)
                return True
            log.warning("Like button not found: %s", post_url)
            return False

        await like_btn.first.click()
        await page.wait_for_timeout(1500)

        if await _check_for_action_block(page):
            return False

        log.info("Liked: %s", post_url)
        return True
    except Exception as exc:
        log.error("Like failed for %s: %s", post_url, exc)
        return False
    finally:
        await page.close()


async def comment_on_post(context: BrowserContext, post_url: str, comment_text: str) -> bool:
    """Navigate to a post and leave a comment. Returns True if commented."""
    page = await context.new_page()
    try:
        await page.goto(post_url, wait_until="domcontentloaded", timeout=20000)
        await page.wait_for_timeout(2500)

        if await _check_for_action_block(page):
            return False

        # Click the comment icon to focus the input
        comment_icon = page.locator('svg[aria-label="Comment"]')
        if await comment_icon.count() > 0:
            await comment_icon.first.click()
            await page.wait_for_timeout(1000)

        # Find the comment textarea
        comment_input = page.locator(
            'textarea[aria-label="Add a comment…"],'
            'textarea[aria-label="Add a comment..."],'
            'textarea[placeholder="Add a comment…"],'
            'textarea[placeholder="Add a comment..."]'
        )
        if await comment_input.count() == 0:
            log.warning("Comment input not found: %s", post_url)
            return False

        await comment_input.first.click()
        await page.wait_for_timeout(500)
        try:
            from bot.stealth import human_type
            await human_type(page, 'textarea[aria-label*="comment"], textarea[placeholder*="comment"]', comment_text)
        except Exception:
            await page.keyboard.type(comment_text, delay=35)
        await page.wait_for_timeout(800)

        # Click Post button for the comment
        post_btn = page.get_by_role("button", name="Post")
        if await post_btn.count() > 0:
            await post_btn.first.click()
        else:
            # Fallback: submit via Enter
            await page.keyboard.press("Enter")

        await page.wait_for_timeout(2000)

        if await _check_for_action_block(page):
            return False

        log.info("Commented on %s: %s", post_url, comment_text[:50])
        return True
    except Exception as exc:
        log.error("Comment failed for %s: %s", post_url, exc)
        return False
    finally:
        await page.close()


async def share_post_to_story(context: BrowserContext, post_url: str) -> bool:
    """Share a feed post to Instagram Stories. Returns True if shared."""
    page = await context.new_page()
    try:
        await page.goto(post_url, wait_until="domcontentloaded", timeout=20000)
        await page.wait_for_timeout(3000)

        # Click the share/paper-plane icon
        share_icon = page.locator(
            'svg[aria-label="Share Post"],'
            'svg[aria-label="Share"],'
            'svg[aria-label="Send"]'
        )
        if await share_icon.count() == 0:
            log.warning("Share icon not found on %s", post_url)
            return False

        await share_icon.first.click()
        await page.wait_for_timeout(2000)

        # Click "Add post to your story" in the share menu
        story_option = page.get_by_text("Add post to your story")
        if await story_option.count() == 0:
            story_option = page.get_by_text("Add to your story")
        if await story_option.count() == 0:
            log.warning("'Add to story' option not found on %s", post_url)
            return False

        await story_option.first.click()
        await page.wait_for_timeout(4000)  # story editor loading

        # Click "Share to Your Story" or similar
        share_story_btn = page.locator(
            'button:has-text("Share to Your Story"),'
            'button:has-text("Share"),'
            'div[role="button"]:has-text("Share to Your Story")'
        )
        if await share_story_btn.count() == 0:
            # Try generic "Share" role button in story editor
            share_story_btn = page.get_by_role("button", name="Share")

        if await share_story_btn.count() > 0:
            await share_story_btn.first.click()
            await page.wait_for_timeout(5000)
            log.info("Shared post to story: %s", post_url)
            return True
        else:
            log.warning("Story share button not found on %s", post_url)
            return False

    except Exception as exc:
        log.error("Story share failed for %s: %s", post_url, exc)
        return False
    finally:
        await page.close()


async def get_post_comments(context: BrowserContext, post_url: str) -> list[dict]:
    """Fetch comments from a post. Returns list of {author, text}."""
    page = await context.new_page()
    comments = []
    try:
        await page.goto(post_url, wait_until="domcontentloaded", timeout=20000)
        await page.wait_for_timeout(3000)

        # Comments are in <ul> elements under the post
        comment_items = page.locator('ul ul li, div[role="button"] + ul li')
        count = await comment_items.count()

        for i in range(min(count, 30)):
            try:
                item = comment_items.nth(i)
                text = await item.inner_text()
                # Comment format is typically "username\ncomment text\ntime ago\nReply"
                parts = text.strip().split("\n")
                if len(parts) >= 2:
                    author = parts[0].strip().lstrip("@")
                    comment_text = parts[1].strip()
                    if author and comment_text and len(comment_text) > 2:
                        comments.append({"author": author, "text": comment_text})
            except Exception:
                continue

        log.info("Found %d comments on %s", len(comments), post_url)
    except Exception as exc:
        log.error("Failed to get comments from %s: %s", post_url, exc)
    finally:
        await page.close()
    return comments


async def reply_to_comment(
    context: BrowserContext, post_url: str,
    comment_author: str, reply_text: str,
) -> bool:
    """Reply to a specific comment on a post."""
    page = await context.new_page()
    try:
        await page.goto(post_url, wait_until="domcontentloaded", timeout=20000)
        await page.wait_for_timeout(3000)

        if await _check_for_action_block(page):
            return False

        # Find the comment by author and click "Reply"
        reply_buttons = page.locator('button:has-text("Reply"), span:has-text("Reply")')
        count = await reply_buttons.count()

        # Look for the reply button near the target comment author
        # We search for the author text, then find the nearest "Reply" button
        author_elements = page.locator(f'a:has-text("{comment_author}"), span:has-text("{comment_author}")')
        if await author_elements.count() == 0:
            log.warning("Comment author @%s not found on %s", comment_author, post_url)
            return False

        # Click the first matching Reply button after the author
        # Navigate to author element's parent and find nearby Reply
        replied = False
        for i in range(min(count, 20)):
            try:
                reply_btn = reply_buttons.nth(i)
                # Check if this reply button is in the right section
                parent_text = await reply_btn.evaluate("el => el.closest('li')?.innerText || ''")
                if comment_author.lower() in parent_text.lower():
                    await reply_btn.click()
                    await page.wait_for_timeout(1000)
                    replied = True
                    break
            except Exception:
                continue

        if not replied:
            log.warning("Could not find Reply button for @%s on %s", comment_author, post_url)
            return False

        # Type the reply (input should be focused with @mention prefilled)
        try:
            from bot.stealth import human_type
            await human_type(page, 'textarea[aria-label*="comment"], textarea[placeholder*="comment"]', reply_text)
        except Exception:
            await page.keyboard.type(reply_text, delay=35)
        await page.wait_for_timeout(800)

        # Submit
        post_btn = page.get_by_role("button", name="Post")
        if await post_btn.count() > 0:
            await post_btn.first.click()
        else:
            await page.keyboard.press("Enter")

        await page.wait_for_timeout(2000)

        if await _check_for_action_block(page):
            return False

        log.info("Replied to @%s on %s", comment_author, post_url)
        return True
    except Exception as exc:
        log.error("Reply failed for @%s on %s: %s", comment_author, post_url, exc)
        return False
    finally:
        await page.close()


async def follow_account(context: BrowserContext, username: str) -> bool:
    """Follow an Instagram account. Returns True if followed."""
    page = await context.new_page()
    try:
        url = f"https://www.instagram.com/{username.lstrip('@')}/"
        await page.goto(url, wait_until="domcontentloaded", timeout=20000)
        await page.wait_for_timeout(2500)

        if await _check_for_action_block(page):
            return False

        # Check if already following
        following_btn = page.locator('button:has-text("Following"), button:has-text("Requested")')
        if await following_btn.count() > 0:
            log.info("Already following @%s", username)
            return True

        # Find and click Follow button
        follow_btn = page.locator('button:has-text("Follow")')
        if await follow_btn.count() == 0:
            log.warning("Follow button not found for @%s", username)
            return False

        # Click the first "Follow" button (not "Following")
        for i in range(await follow_btn.count()):
            text = await follow_btn.nth(i).inner_text()
            if text.strip() == "Follow":
                await follow_btn.nth(i).click()
                await page.wait_for_timeout(2000)

                if await _check_for_action_block(page):
                    return False

                log.info("Followed @%s", username)
                return True

        log.warning("No exact 'Follow' button for @%s", username)
        return False
    except Exception as exc:
        log.error("Follow failed for @%s: %s", username, exc)
        return False
    finally:
        await page.close()


async def unfollow_account(context: BrowserContext, username: str) -> bool:
    """Unfollow an Instagram account. Returns True if unfollowed."""
    page = await context.new_page()
    try:
        url = f"https://www.instagram.com/{username.lstrip('@')}/"
        await page.goto(url, wait_until="domcontentloaded", timeout=20000)
        await page.wait_for_timeout(2500)

        if await _check_for_action_block(page):
            return False

        # Find "Following" button
        following_btn = page.locator('button:has-text("Following")')
        if await following_btn.count() == 0:
            log.info("Not following @%s — nothing to unfollow", username)
            return True

        await following_btn.first.click()
        await page.wait_for_timeout(1500)

        # Confirm unfollow in the modal
        unfollow_confirm = page.get_by_role("button", name="Unfollow")
        if await unfollow_confirm.count() > 0:
            await unfollow_confirm.first.click()
            await page.wait_for_timeout(2000)

            if await _check_for_action_block(page):
                return False

            log.info("Unfollowed @%s", username)
            return True
        else:
            log.warning("Unfollow confirmation not found for @%s", username)
            return False
    except Exception as exc:
        log.error("Unfollow failed for @%s: %s", username, exc)
        return False
    finally:
        await page.close()


async def check_follows_back(context: BrowserContext, username: str) -> bool:
    """Check if an account follows us back."""
    page = await context.new_page()
    try:
        url = f"https://www.instagram.com/{username.lstrip('@')}/"
        await page.goto(url, wait_until="domcontentloaded", timeout=20000)
        await page.wait_for_timeout(2500)

        follows_you = page.locator('text="Follows you"')
        result = await follows_you.count() > 0
        log.info("@%s follows back: %s", username, result)
        return result
    except Exception as exc:
        log.error("Follow-back check failed for @%s: %s", username, exc)
        return False
    finally:
        await page.close()


# ── Engagement session orchestrator ──────────────────────────────────────

async def run_engagement_session(
    hashtags: list[str],
    like_count: int,
    comment_count: int,
) -> dict:
    """
    Run a full engagement session: browse hashtag posts, like and comment.
    Uses a single browser context for efficiency.
    Returns summary dict.
    """
    import random
    from bot.ai_filter import generate_engagement_comment
    from bot.engagement_store import already_engaged, record_engagement
    from bot.rate_limiter import can_perform, mark_action_blocked

    stats = {"likes": 0, "comments": 0, "errors": 0, "blocked": False}

    async with async_playwright() as pw:
        context = await _create_context(pw)

        try:
            # Gather target posts via API (explore pages don't render in headless)
            targets: list[dict] = []

            # Use Instagram API to find targets
            from bot.scraper import _create_api_session, _fetch_tag_web_info, _RateLimited

            api_session = _create_api_session()
            try:
                for tag in hashtags:
                    try:
                        posts = _fetch_tag_web_info(api_session, tag, "engagement", 15)
                        for post in posts:
                            source_url = post.get("source_url", "")
                            if source_url:
                                targets.append({"url": source_url, "hashtag": tag})
                        log.info("Found %d posts from #%s via API", len(posts), tag)
                    except _RateLimited:
                        log.info("Tag API rate-limited for #%s, skipping", tag)
                    except Exception as exc:
                        log.error("Failed to get posts for #%s: %s", tag, exc)
                    await asyncio.sleep(3)
            finally:
                api_session.close()

            if not targets:
                log.warning("No engagement targets found")
                return stats

            # Shuffle targets for natural browsing pattern
            random.shuffle(targets)

            # Like posts
            likes_done = 0
            for target in targets:
                if likes_done >= like_count:
                    break

                allowed, reason = can_perform("like")
                if not allowed:
                    log.info("Like cap reached: %s", reason)
                    break

                if already_engaged(target["url"], "like"):
                    continue

                delay = random.uniform(
                    config.ENGAGEMENT_MIN_DELAY_SECONDS,
                    config.ENGAGEMENT_MAX_DELAY_SECONDS,
                )
                await asyncio.sleep(delay)

                success = await like_post(context, target["url"])
                if success:
                    record_engagement("like", target["url"], hashtag_source=target["hashtag"])
                    try:
                        from bot.rate_limiter import get_limiter
                        get_limiter().mark_action_succeeded("like")
                    except Exception:
                        pass
                    likes_done += 1
                    stats["likes"] += 1
                else:
                    stats["errors"] += 1
                    # Check if we got blocked
                    check_page = await context.new_page()
                    blocked = await _check_for_action_block(check_page)
                    await check_page.close()
                    if blocked:
                        mark_action_blocked()
                        stats["blocked"] = True
                        break

            # Comment on posts (pick from different targets than liked)
            comments_done = 0
            comment_targets = [t for t in targets if not already_engaged(t["url"], "comment")]
            random.shuffle(comment_targets)

            for target in comment_targets:
                if comments_done >= comment_count or stats["blocked"]:
                    break

                allowed, reason = can_perform("comment")
                if not allowed:
                    log.info("Comment cap reached: %s", reason)
                    break

                # Get the post caption for context
                caption_page = await context.new_page()
                try:
                    await caption_page.goto(target["url"], wait_until="domcontentloaded", timeout=15000)
                    await caption_page.wait_for_timeout(2000)
                    # Extract caption text from the page
                    caption_el = caption_page.locator('h1, div[role="button"] + span, article span')
                    caption_text = ""
                    if await caption_el.count() > 0:
                        caption_text = await caption_el.first.inner_text()
                    # Extract account name
                    account_el = caption_page.locator('article header a, a[role="link"]')
                    account_name = ""
                    if await account_el.count() > 0:
                        account_name = await account_el.first.inner_text()
                except Exception:
                    caption_text = ""
                    account_name = ""
                finally:
                    await caption_page.close()

                if not caption_text:
                    continue

                # Generate AI comment
                comment_text = generate_engagement_comment(caption_text, account_name)
                if not comment_text:
                    continue

                delay = random.uniform(
                    config.ENGAGEMENT_MIN_DELAY_SECONDS,
                    config.ENGAGEMENT_MAX_DELAY_SECONDS,
                )
                await asyncio.sleep(delay)

                success = await comment_on_post(context, target["url"], comment_text)
                if success:
                    record_engagement(
                        "comment", target["url"],
                        target_account=account_name,
                        hashtag_source=target["hashtag"],
                        comment_text=comment_text,
                    )
                    try:
                        from bot.rate_limiter import get_limiter
                        get_limiter().mark_action_succeeded("comment")
                    except Exception:
                        pass
                    comments_done += 1
                    stats["comments"] += 1
                else:
                    stats["errors"] += 1

            await _save_state(context)
            await context.browser.close()

        except Exception as exc:
            log.error("Engagement session error: %s", exc)
            stats["errors"] += 1
            try:
                await context.browser.close()
            except Exception:
                pass

    log.info(
        "Engagement session complete: %d likes, %d comments, %d errors",
        stats["likes"], stats["comments"], stats["errors"],
    )

    try:
        from bot.health import get_registry
        if stats["blocked"]:
            get_registry().report_failure("engagement", "action_blocked")
        elif stats["likes"] + stats["comments"] > 0:
            get_registry().report_success("engagement")
        elif stats["errors"] > 0:
            get_registry().report_failure("engagement", f"{stats['errors']} errors")
    except Exception:
        pass

    return stats


def run_engagement_session_sync(
    hashtags: list[str],
    like_count: int,
    comment_count: int,
) -> dict:
    """Synchronous wrapper for run_engagement_session."""
    return asyncio.run(run_engagement_session(hashtags, like_count, comment_count))


# ── Comment reply session ────────────────────────────────────────────────

async def run_reply_session(posts_to_check: list[dict]) -> dict:
    """
    Check recent posts for new comments and reply.
    posts_to_check: list of {post_url, caption} dicts.
    """
    import random
    from bot.ai_filter import generate_comment_reply
    from bot.engagement_store import already_replied, record_reply
    from bot.rate_limiter import can_perform, mark_action_blocked

    stats = {"replies": 0, "errors": 0, "checked": 0}
    our_username = config.get_instagram_username().lower()

    async with async_playwright() as pw:
        context = await _create_context(pw)

        try:
            for post_info in posts_to_check:
                post_url = post_info.get("post_url", "")
                post_caption = post_info.get("caption", "")
                if not post_url:
                    continue

                stats["checked"] += 1
                comments = await get_post_comments(context, post_url)

                for comment in comments:
                    author = comment.get("author", "")
                    text = comment.get("text", "")

                    # Skip our own comments
                    if author.lower() == our_username:
                        continue

                    # Skip spam
                    if any(kw in text.lower() for kw in config.COMMENT_REPLY_SKIP_KEYWORDS):
                        continue

                    # Skip already replied
                    if already_replied(post_url, author, text):
                        continue

                    allowed, reason = can_perform("reply")
                    if not allowed:
                        log.info("Reply cap reached: %s", reason)
                        await _save_state(context)
                        await context.browser.close()
                        return stats

                    # Generate AI reply
                    reply_text = generate_comment_reply(text, author, post_caption)
                    if not reply_text:
                        continue

                    # Random delay
                    delay = random.uniform(
                        config.COMMENT_REPLY_MIN_DELAY_MINUTES * 6,  # seconds (scaled down for session)
                        config.COMMENT_REPLY_MAX_DELAY_MINUTES * 3,
                    )
                    delay = min(delay, 60)  # cap at 60s within a session
                    await asyncio.sleep(delay)

                    success = await reply_to_comment(context, post_url, author, reply_text)
                    if success:
                        record_reply(post_url, author, text, reply_text)
                        stats["replies"] += 1
                    else:
                        stats["errors"] += 1

            await _save_state(context)
            await context.browser.close()

        except Exception as exc:
            log.error("Reply session error: %s", exc)
            stats["errors"] += 1
            try:
                await context.browser.close()
            except Exception:
                pass

    log.info("Reply session: checked %d posts, %d replies, %d errors",
             stats["checked"], stats["replies"], stats["errors"])
    return stats


def run_reply_session_sync(posts_to_check: list[dict]) -> dict:
    """Synchronous wrapper for run_reply_session."""
    return asyncio.run(run_reply_session(posts_to_check))


# ── Follow/unfollow session ─────────────────────────────────────────────

async def run_follow_session(accounts_to_follow: list[dict]) -> dict:
    """Follow a batch of accounts."""
    import random
    from bot.engagement_store import record_follow
    from bot.rate_limiter import can_perform

    stats = {"followed": 0, "errors": 0}

    async with async_playwright() as pw:
        context = await _create_context(pw)
        try:
            for info in accounts_to_follow:
                username = info.get("account", "").lstrip("@")
                source_post = info.get("source_post", "")
                if not username:
                    continue

                allowed, reason = can_perform("follow")
                if not allowed:
                    log.info("Follow cap reached: %s", reason)
                    break

                delay = random.uniform(10, 30)
                await asyncio.sleep(delay)

                success = await follow_account(context, username)
                if success:
                    record_follow(username, source_post)
                    stats["followed"] += 1
                else:
                    stats["errors"] += 1

            await _save_state(context)
            await context.browser.close()
        except Exception as exc:
            log.error("Follow session error: %s", exc)
            try:
                await context.browser.close()
            except Exception:
                pass

    return stats


def run_follow_session_sync(accounts_to_follow: list[dict]) -> dict:
    return asyncio.run(run_follow_session(accounts_to_follow))


async def run_unfollow_session(accounts_to_unfollow: list[str]) -> dict:
    """Unfollow a batch of accounts."""
    import random
    from bot.engagement_store import record_unfollow

    stats = {"unfollowed": 0, "errors": 0}

    async with async_playwright() as pw:
        context = await _create_context(pw)
        try:
            for username in accounts_to_unfollow:
                if not username:
                    continue

                delay = random.uniform(10, 25)
                await asyncio.sleep(delay)

                success = await unfollow_account(context, username)
                if success:
                    record_unfollow(username)
                    stats["unfollowed"] += 1
                else:
                    stats["errors"] += 1

            await _save_state(context)
            await context.browser.close()
        except Exception as exc:
            log.error("Unfollow session error: %s", exc)
            try:
                await context.browser.close()
            except Exception:
                pass

    return stats


def run_unfollow_session_sync(accounts_to_unfollow: list[str]) -> dict:
    return asyncio.run(run_unfollow_session(accounts_to_unfollow))
