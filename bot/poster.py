"""
Instagram posting via Playwright (headless Chromium, desktop mode).

Loads browser state from cookies, opens create modal via sidebar,
uploads media, types caption, clicks Share, and extracts the post ID
from API interception or profile fallback.
"""

import asyncio
import json
import logging
import re
import tempfile
from pathlib import Path

import requests
from PIL import Image
from playwright.async_api import async_playwright, BrowserContext, Page

from bot import config
from bot.queue import already_posted

log = logging.getLogger(__name__)

# Desktop browser config — Instagram only shows the create button on desktop
_VIEWPORT = {"width": 1280, "height": 900}
_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


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
    """Launch headless Chromium with saved Instagram session (desktop mode)."""
    browser = await playwright.chromium.launch(headless=True)

    storage = _build_storage_state(config.get_cookies_path())
    tmp_state = config.get_account_dir() / "_tmp_poster_state.json"
    tmp_state.write_text(json.dumps(storage), encoding="utf-8")

    context = await browser.new_context(
        storage_state=str(tmp_state),
        viewport=_VIEWPORT,
        user_agent=_USER_AGENT,
    )

    tmp_state.unlink(missing_ok=True)
    return context


async def _save_state(context: BrowserContext):
    """Persist browser state for next run."""
    state = await context.storage_state()
    config.get_state_path().write_text(json.dumps(state, indent=2), encoding="utf-8")
    log.info("Browser state saved → %s", config.get_state_path().name)


# ── Post creation ────────────────────────────────────────────────────────────

async def _post_content(context: BrowserContext, media_path: Path, caption: str,
                        media_type: str = "image") -> str:
    """
    Create an Instagram post via the desktop web UI.
    Returns the post URL if successful, empty string on failure.
    """
    is_video = media_type == "video"
    page = await context.new_page()
    post_id = ""

    # Intercept CreatePost API response
    async def on_response(response):
        nonlocal post_id
        url = response.url
        if any(k in url for k in (
            "create/configure", "media/configure", "media_publish",
            "configure_to_igtv", "configure_sidecar",
            "configure_to_clips",   # Reels endpoint
            "clip/create",          # Reels alt endpoint
        )):
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
        log.info("Opening Instagram (desktop)...")
        await page.goto("https://www.instagram.com/", wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(4000)

        # Dismiss popups (notifications, cookies, etc.)
        for dismiss_text in ["Not Now", "Decline", "Cancel", "Dismiss"]:
            btn = page.get_by_text(dismiss_text, exact=True)
            if await btn.count() > 0:
                await btn.first.click()
                await page.wait_for_timeout(1000)

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

        # 6. Type caption
        log.info("Typing caption (%d chars)...", len(caption))
        caption_area = page.locator(
            '[aria-label="Write a caption..."], '
            '[aria-label="Write a caption…"], '
            'div[contenteditable="true"], '
            'textarea'
        )

        if await caption_area.count() > 0:
            await caption_area.first.click()
            await page.wait_for_timeout(500)
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
        share_wait = 25000 if is_video else 12000
        log.info("Waiting %ds for %s post creation...", share_wait // 1000, media_type)
        await page.wait_for_timeout(share_wait)

        # 8. Fallback: extract post ID from profile if not intercepted
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

                    # Get first post link from profile grid
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
    Downloads media, processes images, creates post, saves state.
    Returns the post URL or empty string on failure.
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

    # Download media
    media_path = _download_media(media_url, media_type)
    if not media_path:
        log.error("Media download failed (URL may be expired) — cannot post")
        return "SKIP"

    # Process images for proper Instagram sizing
    if media_type in ("image", "carousel"):
        media_path = _prepare_image(media_path)

    post_url = ""
    try:
        async with async_playwright() as pw:
            context = await _create_context(pw)

            post_url = await _post_content(context, media_path, caption, media_type)

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
