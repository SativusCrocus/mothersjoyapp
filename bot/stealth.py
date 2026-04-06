"""
Browser anti-detection and human behavior simulation.

Applies playwright-stealth patches, randomizes fingerprints,
and provides human-like interaction methods (typing, clicking, scrolling).
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import random
import time
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

# ── User-Agent pool (real Chrome UAs, rotate weekly) ─────────────────────────

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
]

_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
    {"width": 1920, "height": 1080},
    {"width": 1280, "height": 800},
    {"width": 1600, "height": 900},
]

_LANGUAGES = ["en-GB,en;q=0.9", "en-US,en;q=0.9", "en-GB,en-US;q=0.9,en;q=0.8"]
_PLATFORMS = ["Win32", "MacIntel", "Linux x86_64"]
_CONCURRENCY = [4, 8, 12, 16]


# ── Fingerprint management ───────────────────────────────────────────────────

def create_fingerprint(account_name: str = "") -> dict:
    """Generate a consistent session fingerprint. Seeded by account + week number
    so it rotates weekly but stays consistent within a week."""
    from bot import config
    week = int(time.time() / (7 * 86400))
    seed = hashlib.md5(f"{account_name}:{week}".encode()).hexdigest()
    rng = random.Random(seed)

    return {
        "user_agent": rng.choice(_USER_AGENTS),
        "viewport": rng.choice(_VIEWPORTS),
        "locale": rng.choice(["en-GB", "en-US"]),
        "timezone_id": rng.choice(["Europe/London", "Europe/London", "America/New_York"]),
        "languages": rng.choice(_LANGUAGES),
        "platform": rng.choice(_PLATFORMS),
        "hardware_concurrency": rng.choice(_CONCURRENCY),
        "device_memory": rng.choice([4, 8, 16]),
    }


async def apply_stealth(page):
    """Apply playwright-stealth patches to a page."""
    try:
        from playwright_stealth import stealth_async
        await stealth_async(page)
    except ImportError:
        log.warning("playwright-stealth not installed — running without stealth")
    except Exception as exc:
        log.warning("Stealth patches failed: %s", exc)


def get_context_options(fingerprint: dict | None = None, proxy_config: dict | None = None) -> dict:
    """Return Playwright browser context options with fingerprint applied."""
    fp = fingerprint or create_fingerprint()

    opts = {
        "viewport": fp["viewport"],
        "user_agent": fp["user_agent"],
        "locale": fp["locale"],
        "timezone_id": fp["timezone_id"],
        "permissions": ["geolocation"],
        "java_script_enabled": True,
        "bypass_csp": False,
        "extra_http_headers": {
            "Accept-Language": fp["languages"],
        },
    }

    if proxy_config:
        opts["proxy"] = proxy_config

    return opts


# ── Human behavior simulation ────────────────────────────────────────────────

async def human_type(page, selector: str, text: str, clear_first: bool = True):
    """Type text with human-like variable delays.
    Base delay 50-150ms per char, occasional thinking pauses."""
    element = page.locator(selector).first
    if clear_first:
        await element.click()
        await element.press("Meta+a" if "Mac" in str(page.context.browser.browser_type) else "Control+a")
        await page.wait_for_timeout(random.randint(100, 300))

    for i, char in enumerate(text):
        # Base delay varies by character type
        if char == ' ':
            delay = random.randint(30, 80)
        elif char in '.,!?;:':
            delay = random.randint(80, 200)
        elif char.isupper():
            delay = random.randint(100, 180)
        else:
            delay = random.randint(50, 150)

        # Occasional thinking pause (every 5-15 chars)
        if i > 0 and random.random() < 0.08:
            await page.wait_for_timeout(random.randint(300, 700))

        await element.press(char, delay=delay)

    # Brief pause after finishing
    await page.wait_for_timeout(random.randint(200, 500))


async def human_click(page, selector: str):
    """Click with slight position randomization and variable timing."""
    element = page.locator(selector).first

    try:
        box = await element.bounding_box()
        if box:
            # Click near center with slight offset (human imprecision)
            x = box["x"] + box["width"] / 2 + random.uniform(-3, 3)
            y = box["y"] + box["height"] / 2 + random.uniform(-3, 3)

            # Hover first (humans move mouse before clicking)
            await page.mouse.move(x, y)
            await page.wait_for_timeout(random.randint(50, 200))
            await page.mouse.click(x, y, delay=random.randint(50, 150))
        else:
            await element.click(delay=random.randint(50, 150))
    except Exception:
        # Fallback to basic click
        await element.click()

    await page.wait_for_timeout(random.randint(200, 600))


async def human_scroll(page, distance: int = 300):
    """Scroll with variable speed and occasional pauses."""
    scrolled = 0
    direction = 1 if distance > 0 else -1
    remaining = abs(distance)

    while remaining > 0:
        # Variable chunk size
        chunk = min(remaining, random.randint(50, 150))
        await page.mouse.wheel(0, chunk * direction)
        scrolled += chunk
        remaining -= chunk

        # Occasional reading pause
        if random.random() < 0.2:
            await page.wait_for_timeout(random.randint(500, 2000))
        else:
            await page.wait_for_timeout(random.randint(30, 100))

    # Occasional slight backtrack (humans overshoot)
    if random.random() < 0.15 and abs(distance) > 200:
        await page.wait_for_timeout(random.randint(300, 600))
        await page.mouse.wheel(0, -direction * random.randint(20, 60))


async def human_wait(min_s: float = 1.0, max_s: float = 3.0):
    """Wait with normal distribution (not uniform). Occasionally longer."""
    import asyncio
    mean = (min_s + max_s) / 2
    std = (max_s - min_s) / 4
    delay = max(min_s, random.gauss(mean, std))

    # 5% chance of a "distraction" delay (2x-4x normal)
    if random.random() < 0.05:
        delay *= random.uniform(2, 4)

    await asyncio.sleep(delay)


async def random_browse(page, duration_s: float = 5.0):
    """Simulate casual browsing to make the session look organic.
    Scrolls feed, maybe checks notifications."""
    import asyncio
    start = time.time()

    while time.time() - start < duration_s:
        action = random.choice(["scroll", "scroll", "pause", "scroll"])

        if action == "scroll":
            await human_scroll(page, random.randint(200, 600))
            await asyncio.sleep(random.uniform(1, 3))
        elif action == "pause":
            await asyncio.sleep(random.uniform(2, 5))

    log.debug("Random browse session: %.1fs", time.time() - start)
