"""
Shared Instagram browser session management.

Loads the best available saved state, refreshes auth with credentials when
the session has expired, and persists the renewed state for future runs.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

from playwright.async_api import BrowserContext, Page

from bot import config

log = logging.getLogger(__name__)


class AuthenticationRequired(Exception):
    """Instagram auth failed. NOT a crash — callers decide how to handle."""
    pass


_LOGIN_URL = "https://www.instagram.com/accounts/login/"
_HOME_URL = "https://www.instagram.com/"
_AUTH_COOKIE_NAMES = {"sessionid", "ds_user_id", "csrftoken"}


def _coerce_storage_state(path: Path) -> dict:
    if not path.exists():
        return {"cookies": [], "origins": []}

    raw = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(raw, dict) and "cookies" in raw:
        return {
            "cookies": list(raw.get("cookies", [])),
            "origins": list(raw.get("origins", [])),
        }

    if isinstance(raw, list):
        cookies = []
        for cookie in raw:
            item = {
                "name": cookie.get("name", ""),
                "value": cookie.get("value", ""),
                "domain": cookie.get("domain", ".instagram.com"),
                "path": cookie.get("path", "/"),
                "secure": cookie.get("secure", True),
                "httpOnly": cookie.get("httpOnly", False),
                "sameSite": cookie.get("sameSite", "None"),
            }
            if cookie.get("expires") or cookie.get("expirationDate"):
                item["expires"] = cookie.get("expires") or cookie.get("expirationDate")
            cookies.append(item)
        return {"cookies": cookies, "origins": []}

    return {"cookies": [], "origins": []}


def _merge_state_sources() -> dict:
    """Prefer saved Playwright state, but fill missing auth cookies from cookies.json."""
    state = _coerce_storage_state(config.get_state_path())
    cookies_state = _coerce_storage_state(config.get_cookies_path())

    merged = {
        "cookies": list(state.get("cookies", [])),
        "origins": list(state.get("origins", [])),
    }
    existing = {
        (cookie.get("name"), cookie.get("domain"), cookie.get("path")): index
        for index, cookie in enumerate(merged["cookies"])
    }

    present_auth = {cookie.get("name") for cookie in merged["cookies"] if cookie.get("name") in _AUTH_COOKIE_NAMES}

    for cookie in cookies_state.get("cookies", []):
        key = (cookie.get("name"), cookie.get("domain"), cookie.get("path"))
        name = cookie.get("name")
        should_overlay = key not in existing or (name in _AUTH_COOKIE_NAMES and name not in present_auth)
        if should_overlay:
            if key in existing:
                merged["cookies"][existing[key]] = cookie
            else:
                existing[key] = len(merged["cookies"])
                merged["cookies"].append(cookie)

    return merged


def _write_storage_state(state: dict):
    config.get_state_path().write_text(json.dumps(state, indent=2), encoding="utf-8")
    config.get_cookies_path().write_text(json.dumps(state.get("cookies", []), indent=2), encoding="utf-8")


async def persist_storage_state(context: BrowserContext):
    """Persist Playwright storage state and flattened cookies."""
    state = await context.storage_state()
    _write_storage_state(state)
    log.info("Instagram session saved -> %s, %s", config.get_state_path().name, config.get_cookies_path().name)


async def _click_first(page: Page, labels: list[str]):
    for label in labels:
        button = page.get_by_role("button", name=label)
        if await button.count() > 0:
            await button.first.click()
            await page.wait_for_timeout(1000)
            return True
    return False


async def _dismiss_cookie_banner(page: Page):
    await _click_first(
        page,
        [
            "Allow all cookies",
            "Allow essential and optional cookies",
            "Accept all",
            "Accept all cookies",
            "Only allow essential cookies",
        ],
    )


async def _dismiss_post_login_dialogs(page: Page):
    for _ in range(3):
        clicked = await _click_first(page, ["Not Now", "Not now", "Cancel", "Dismiss"])
        if not clicked:
            break


async def _is_logged_in(page: Page) -> bool:
    if "/accounts/login" in page.url or "/challenge/" in page.url:
        return False

    body_text = (await page.locator("body").inner_text()).lower()
    logged_out_signals = (
        "log into instagram",
        "create new account",
        "forgot password",
        "mobile number, username or email",
        "share everyday moments with your close friends",
        "open instagram",
    )
    if any(signal in body_text for signal in logged_out_signals):
        return False

    login_fields = page.locator(
        'input[name="username"], input[name="email"], input[name="enc_password"], input[name="pass"], '
        'input[aria-label*="username"], input[aria-label*="password"]'
    )
    if await login_fields.count() > 0:
        return False

    logged_in_markers = page.locator(
        'svg[aria-label="New post"], '
        'a[href="/direct/inbox/"], '
        'a[href="/accounts/edit/"], '
        'a[href*="/reels/"], '
        'a[href*="/explore/"], '
        'a[href*="/accounts/activity/"]'
    )
    if await logged_in_markers.count() > 0:
        return True

    return True


async def ensure_authenticated(context: BrowserContext, max_attempts: int = 3) -> bool:
    """
    Confirm the current session is authenticated.
    Falls back to credential login with retry and persists the renewed state.
    """
    for attempt in range(max_attempts):
        page = await context.new_page()
        try:
            # Apply stealth to the auth page
            try:
                from bot.stealth import apply_stealth
                await apply_stealth(page)
            except Exception:
                pass

            await page.goto(_HOME_URL, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(3000)
            await _dismiss_cookie_banner(page)

            if await _is_logged_in(page):
                await persist_storage_state(context)
                return True

            username = config.get_instagram_username()
            password = config.get_instagram_password()
            if not username or not password:
                log.error("Instagram session expired and credentials are unavailable")
                return False

            log.info("Instagram session expired — logging in (attempt %d/%d)", attempt + 1, max_attempts)
            await page.goto(_LOGIN_URL, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(3000)
            await _dismiss_cookie_banner(page)

            username_input = page.locator('input[name="username"], input[name="email"]')
            password_input = page.locator('input[name="enc_password"], input[name="pass"]')
            await username_input.first.wait_for(state="visible", timeout=15000)
            await password_input.first.wait_for(state="visible", timeout=15000)

            # Use human-like typing for stealth
            try:
                from bot.stealth import human_type
                await human_type(page, 'input[name="username"], input[name="email"]', username)
                await human_type(page, 'input[name="enc_password"], input[name="pass"]', password)
            except Exception:
                await username_input.first.fill(username)
                await password_input.first.fill(password)

            submit = page.locator(
                '[role="button"][aria-label="Log In"], '
                '[role="button"][aria-label="Log in"], '
                'button:has-text("Log In"), '
                'button:has-text("Log in")'
            )
            if await submit.count() > 0:
                await submit.first.click()
            else:
                submit_input = page.locator('button[type="submit"], input[type="submit"]')
                if await submit_input.count() > 0:
                    try:
                        await submit_input.first.click()
                    except Exception:
                        await page.keyboard.press("Enter")
                else:
                    await page.keyboard.press("Enter")

            for _ in range(30):
                await page.wait_for_timeout(1000)
                if await _is_logged_in(page):
                    await _dismiss_post_login_dialogs(page)
                    await persist_storage_state(context)
                    return True
                if "/challenge/" in page.url or "/two_factor" in page.url:
                    log.error(
                        "Instagram requires verification at %s (attempt %d/%d)",
                        page.url, attempt + 1, max_attempts,
                    )
                    if attempt + 1 < max_attempts:
                        import asyncio
                        await asyncio.sleep(30 * (attempt + 1))
                    break  # break inner loop, retry outer

            if await _is_logged_in(page):
                await persist_storage_state(context)
                return True

        except Exception as exc:
            log.warning("Auth attempt %d/%d error: %s", attempt + 1, max_attempts, exc)
            if attempt + 1 < max_attempts:
                import asyncio
                await asyncio.sleep(10 * (attempt + 1))
        finally:
            await page.close()

    log.error("Instagram login did not complete after %d attempts", max_attempts)
    return False


async def create_authenticated_context(playwright, *, viewport: dict | None = None,
                                       user_agent: str | None = None) -> BrowserContext:
    """Launch Chromium with stealth, proxy, and saved state. Ensures session is authenticated."""

    # Get stealth fingerprint if no explicit viewport/UA provided
    fingerprint = None
    if viewport is None or user_agent is None:
        try:
            from bot.stealth import create_fingerprint
            fingerprint = create_fingerprint(config.get_account_name())
            viewport = viewport or fingerprint["viewport"]
            user_agent = user_agent or fingerprint["user_agent"]
        except Exception:
            viewport = viewport or {"width": 1280, "height": 900}
            user_agent = user_agent or "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/131.0.0.0"

    # Get proxy config
    proxy_arg = None
    try:
        from bot.proxy import apply_to_playwright
        proxy_arg = apply_to_playwright()
    except Exception:
        pass

    launch_args = [
        "--disable-gpu",
        "--disable-dev-shm-usage",
        "--disable-extensions",
        "--no-sandbox",
        "--single-process",
        "--js-flags=--max-old-space-size=512",
    ]

    browser = await playwright.chromium.launch(
        headless=True,
        args=launch_args,
        proxy=proxy_arg if proxy_arg else None,
    )

    storage = _merge_state_sources()
    tmp_state = config.get_account_dir() / "_tmp_instagram_state.json"
    tmp_state.write_text(json.dumps(storage), encoding="utf-8")

    context_opts = {
        "storage_state": str(tmp_state),
        "viewport": viewport,
        "user_agent": user_agent,
    }

    # Apply fingerprint extras
    if fingerprint:
        context_opts["locale"] = fingerprint.get("locale", "en-GB")
        context_opts["timezone_id"] = fingerprint.get("timezone_id", "Europe/London")

    context = await browser.new_context(**context_opts)
    tmp_state.unlink(missing_ok=True)

    if not await ensure_authenticated(context):
        await context.close()
        try:
            from bot.health import get_registry
            get_registry().report_failure("auth", "authentication_failed")
        except Exception:
            pass
        raise AuthenticationRequired("Instagram authentication failed")

    try:
        from bot.health import get_registry
        get_registry().report_success("auth")
    except Exception:
        pass

    return context
