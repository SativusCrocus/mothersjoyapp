"""
Instagram posting via Playwright (headless Chromium).

Loads browser state from cookies, uploads media, types caption,
intercepts the CreatePost API response for the post ID, and
saves updated browser state.
"""

import asyncio
import json
import logging
import re
import tempfile
from pathlib import Path

import requests
from playwright.async_api import async_playwright, BrowserContext, Page

from bot import config

log = logging.getLogger(__name__)


# ── Media download ───────────────────────────────────────────────────────────

def _download_media(url: str, media_type: str) -> Path | None:
    """Download media from URL to a temporary file."""
    if not url:
        return None

    ext = ".mp4" if media_type == "video" else ".jpg"
    try:
        resp = requests.get(url, timeout=30, stream=True)
        resp.raise_for_status()

        tmp = Path(tempfile.mktemp(suffix=ext, prefix="mjbot_"))
        with open(tmp, "wb") as f:
            for chunk in resp.iter_content(8192):
                f.write(chunk)

        log.info("Downloaded media → %s (%.1f KB)", tmp.name, tmp.stat().st_size / 1024)
        return tmp

    except Exception as exc:
        log.error("Media download failed: %s", exc)
        return None


# ── Browser state ───────────────────────────────────────────────────────────

def _build_storage_state(cookies_path: Path) -> dict:
    """Convert cookies.json to Playwright storage state."""
    if not cookies_path.exists():
        return {"cookies": [], "origins": []}

    raw = json.loads(cookies_path.read_text(encoding="utf-8"))

    if isinstance(raw, dict) and "cookies" in raw:
        return raw

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
    """Launch headless Chromium with saved Instagram session."""
    browser = await playwright.chromium.launch(headless=True)

    storage = _build_storage_state(config.get_cookies_path())
    tmp_state = config.get_account_dir() / "_tmp_poster_state.json"
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


async def _save_state(context: BrowserContext):
    """Persist browser state for next run."""
    state = await context.storage_state()
    config.get_state_path().write_text(json.dumps(state, indent=2), encoding="utf-8")
    log.info("Browser state saved → %s", config.get_state_path().name)


# ── Post creation ────────────────────────────────────────────────────────────

async def _type_with_delay(page: Page, selector: str, text: str, delay: int = 35):
    """Type text character by character with human-like delay."""
    element = await page.wait_for_selector(selector, timeout=10000)
    if element:
        await element.click()
        await page.keyboard.type(text, delay=delay)


async def _post_content(context: BrowserContext, media_path: Path, caption: str) -> str:
    """
    Create an Instagram post.
    Returns the post URL if successful, empty string on failure.
    """
    page = await context.new_page()
    post_id = ""

    # Intercept CreatePost / media_publish API response
    async def on_response(response):
        nonlocal post_id
        url = response.url
        if any(k in url for k in ("create/configure", "media/configure", "media_publish")):
            try:
                data = await response.json()
                media = data.get("media", {})
                code = media.get("code", "") or media.get("shortcode", "")
                if code:
                    post_id = code
                    log.info("Intercepted post ID: %s", code)
            except Exception:
                pass

    page.on("response", on_response)

    try:
        # 1. Navigate to Instagram
        log.info("Opening Instagram...")
        await page.goto("https://www.instagram.com/", wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(3000)

        # Dismiss any modals / "Turn on Notifications" popups
        for dismiss_text in ["Not Now", "Cancel", "Dismiss"]:
            btn = page.get_by_text(dismiss_text, exact=True)
            if await btn.count() > 0:
                await btn.first.click()
                await page.wait_for_timeout(1000)

        # 2. Click new post button
        log.info("Looking for new post button...")
        new_post_btn = None

        # Try multiple selectors for the create/new post button
        selectors = [
            '[data-testid="new-post-button"]',
            '[aria-label="New post"]',
            '[aria-label="New Post"]',
            'svg[aria-label="New post"]',
            'svg[aria-label="New Post"]',
            '[aria-label="Create"]',
        ]

        for sel in selectors:
            try:
                el = page.locator(sel)
                if await el.count() > 0:
                    new_post_btn = el.first
                    break
            except Exception:
                continue

        if not new_post_btn:
            # Fallback: look for the "+" icon in nav
            plus_links = page.locator('a[href="/create/style/"]')
            if await plus_links.count() > 0:
                new_post_btn = plus_links.first

        if not new_post_btn:
            log.error("Could not find new post button")
            return ""

        await new_post_btn.click()
        await page.wait_for_timeout(2000)

        # 3. Upload media file
        log.info("Uploading media: %s", media_path.name)

        # Wait for file input and set the file
        file_input = page.locator('input[type="file"]')
        await file_input.wait_for(timeout=10000)
        await file_input.set_input_files(str(media_path))
        await page.wait_for_timeout(3000)

        # 4. Skip crop / filter steps — click Next/Continue
        for _ in range(3):
            for next_text in ["Next", "Continue"]:
                btn = page.get_by_role("button", name=next_text)
                if await btn.count() > 0:
                    await btn.first.click()
                    await page.wait_for_timeout(1500)
                    break

        # 5. Type caption
        log.info("Typing caption (%d chars)...", len(caption))
        caption_area = page.locator('[aria-label="Write a caption..."], [aria-label="Write a caption…"], textarea, [contenteditable="true"]')

        if await caption_area.count() > 0:
            await caption_area.first.click()
            await page.wait_for_timeout(500)
            await page.keyboard.type(caption, delay=15)
        else:
            log.warning("Could not find caption input — posting without caption")

        await page.wait_for_timeout(1000)

        # 6. Click Share / Post
        log.info("Clicking Share...")
        for share_text in ["Share", "Post"]:
            btn = page.get_by_role("button", name=share_text)
            if await btn.count() > 0:
                await btn.first.click()
                break

        # Wait for the post to be created
        await page.wait_for_timeout(8000)

        # 7. Fallback: extract post ID from profile if not intercepted
        if not post_id:
            log.info("Post ID not intercepted — checking profile for latest post")
            try:
                username = config.get_instagram_username()
                if username:
                    await page.goto(
                        f"https://www.instagram.com/{username}/",
                        wait_until="domcontentloaded",
                        timeout=15000,
                    )
                    await page.wait_for_timeout(3000)

                    # Get first post link
                    post_links = page.locator('a[href*="/p/"]')
                    if await post_links.count() > 0:
                        href = await post_links.first.get_attribute("href")
                        if href:
                            match = re.search(r"/p/([^/]+)/", href)
                            if match:
                                post_id = match.group(1)
                                log.info("Extracted latest post ID from profile: %s", post_id)
            except Exception as exc:
                log.warning("Profile fallback failed: %s", exc)

    except Exception as exc:
        log.error("Posting failed: %s", exc)
    finally:
        await page.close()

    if post_id:
        return f"https://www.instagram.com/p/{post_id}/"
    return ""


# ── Public API ───────────────────────────────────────────────────────────────

async def post_to_instagram(item: dict) -> str:
    """
    Post a queue item to Instagram.
    Downloads media, creates post, saves state.
    Returns the post URL or empty string on failure.
    """
    media_url = item.get("media_url", "")
    media_type = item.get("media_type", "image")
    caption = item.get("generated_caption", "")

    if not caption:
        log.error("No generated caption — cannot post")
        return ""

    # Download media
    media_path = _download_media(media_url, media_type)
    if not media_path:
        log.error("Media download failed — cannot post")
        return ""

    post_url = ""
    try:
        async with async_playwright() as pw:
            context = await _create_context(pw)

            post_url = await _post_content(context, media_path, caption)

            # Save updated browser state
            await _save_state(context)
            await context.browser.close()

    finally:
        # Clean up temp media file
        if media_path and media_path.exists():
            media_path.unlink(missing_ok=True)

    return post_url


def post_to_instagram_sync(item: dict) -> str:
    """Synchronous wrapper for post_to_instagram."""
    return asyncio.run(post_to_instagram(item))
