"""
Instagram content discovery via authenticated Instagram web endpoints.

Discovery prefers the authenticated `/api/v1/tags/web_info/` endpoint because
it returns recent/top hashtag media without relying on fragile DOM structure.
If that endpoint stops returning usable media, the scraper falls back to the
explore-page shortcode path plus `/api/v1/media/{pk}/info/`.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from collections import Counter, defaultdict, deque
from datetime import datetime, timezone

import requests

from bot import config

log = logging.getLogger(__name__)

_INSTAGRAM_BASE_URL = "https://www.instagram.com"
_TAG_WEB_INFO_URL = _INSTAGRAM_BASE_URL + "/api/v1/tags/web_info/"
_SEARCH_URL = _INSTAGRAM_BASE_URL + "/api/v1/fbsearch/web/top_serp/"
_MEDIA_INFO_URL = _INSTAGRAM_BASE_URL + "/api/v1/media/{pk}/info/"
_DESKTOP_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)
_INSTAGRAM_WEB_APP_ID = "936619743392459"
_STALE_EVENT_PHRASES = (
    "mother's day",
    "mothers day",
    "father's day",
    "fathers day",
    "valentine",
    "easter",
    "christmas",
    "new year",
    "halloween",
    "black friday",
    "ramadan",
    "eid mubarak",
    "back to school",
)
_OFF_TOPIC_COMMERCIAL_PHRASES = (
    "va loan",
    "home loan",
    "loan specialist",
    "mortgage",
    "real estate",
    "realtor",
    "insurance quote",
    "schedule an appointment",
    "book a consultation",
    "free consultation",
    "apply now",
    "wealth management",
    "investment opportunity",
    "business coach",
    "marketing agency",
)
_PARENTING_SIGNALS = (
    "parent",
    "parenting",
    "mum",
    "mom",
    "mother",
    "motherhood",
    "dad",
    "father",
    "baby",
    "babies",
    "newborn",
    "infant",
    "toddler",
    "child",
    "children",
    "kid",
    "kids",
    "postpartum",
    "postnatal",
    "family",
    "families",
    "daughter",
    "son",
)


# ── Helpers ──────────────────────────────────────────────────────────────────

def _niche_to_hashtag(niche: str) -> str:
    """Convert a niche phrase to a hashtag slug: 'UK parenting tips' -> 'ukparentingtips'."""
    return re.sub(r"[^a-z0-9]", "", niche.lower())


def _metric_to_int(text: str) -> int:
    """Convert display metrics like 591K or 4.9M to integers."""
    if not text:
        return 0

    cleaned = text.strip().upper().replace(",", "")
    match = re.search(r"(\d+(?:\.\d+)?)([KMB])?", cleaned)
    if not match:
        return 0

    value = float(match.group(1))
    suffix = match.group(2)
    multiplier = {"K": 1_000, "M": 1_000_000, "B": 1_000_000_000}.get(suffix, 1)
    return int(value * multiplier)


def _parse_meta_description(text: str) -> tuple[int, str, str, int]:
    """
    Extract likes, username, caption, and date from Instagram's meta description.
    Example:
    '12K likes, 131 comments - user on February 22, 2026: "Caption".'
    """
    if not text:
        return 0, "", "", 0

    match = re.search(
        r"^(?P<likes>[\d.,KMB]+)\s+likes(?:,\s+[\d.,KMB]+\s+comments)?\s+-\s+"
        r"(?P<account>[A-Za-z0-9._]+)\s+on\s+(?P<date>[^:]+):\s+\"(?P<caption>.*)\"\.?\s*$",
        text.strip(),
        re.IGNORECASE,
    )
    if not match:
        return 0, "", "", 0

    taken_at = 0
    date_text = match.group("date").strip()
    for fmt in ("%B %d, %Y", "%b %d, %Y"):
        try:
            taken_at = int(datetime.strptime(date_text, fmt).replace(tzinfo=timezone.utc).timestamp())
            break
        except ValueError:
            continue

    return (
        _metric_to_int(match.group("likes")),
        match.group("account"),
        match.group("caption"),
        taken_at,
    )


def _load_cookie_dict() -> dict[str, str]:
    """Load Instagram auth cookies from the saved account state."""
    cookie_path = config.get_cookies_path()
    state_path = config.get_state_path()

    raw_cookies = []
    if cookie_path.exists():
        try:
            raw_cookies = json.loads(cookie_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            raw_cookies = []

    if not raw_cookies and state_path.exists():
        try:
            state = json.loads(state_path.read_text(encoding="utf-8"))
            raw_cookies = state.get("cookies", []) if isinstance(state, dict) else []
        except json.JSONDecodeError:
            raw_cookies = []

    return {
        cookie.get("name", ""): cookie.get("value", "")
        for cookie in raw_cookies
        if cookie.get("name") and cookie.get("value")
    }


def _create_api_session() -> requests.Session:
    """Create an authenticated requests session against Instagram web APIs."""
    cookies = _load_cookie_dict()
    session = requests.Session()
    session.cookies.update(cookies)
    session.headers.update(
        {
            "User-Agent": _DESKTOP_USER_AGENT,
            "X-CSRFToken": cookies.get("csrftoken", ""),
            "X-IG-App-ID": _INSTAGRAM_WEB_APP_ID,
            "X-Requested-With": "XMLHttpRequest",
            "Referer": _INSTAGRAM_BASE_URL + "/",
        }
    )
    return session


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

    for phrase in _OFF_TOPIC_COMMERCIAL_PHRASES:
        if phrase in caption_lower:
            return True

    return False


def _contains_stale_event_reference(caption: str) -> bool:
    """Detect obviously time-sensitive seasonal/event language."""
    caption_lower = caption.lower()
    return any(phrase in caption_lower for phrase in _STALE_EVENT_PHRASES)


def _has_parenting_context(post: dict) -> bool:
    """Reject obvious hashtag pollution that is not actually about parenting."""
    text = " ".join(
        [
            post.get("caption", ""),
            post.get("account", ""),
            post.get("discovery_term", ""),
        ]
    ).lower()
    return any(signal in text for signal in _PARENTING_SIGNALS)


def _passes_hard_filters(post: dict) -> bool:
    """Apply non-negotiable filters before sending content to the AI curator."""
    caption = post.get("caption", "")
    owner = post.get("account", "")
    active_account = config.get_instagram_username().strip().lower()

    if active_account and owner.strip().lower() == active_account:
        log.debug("Skipping self-post discovery: %s", post.get("source_url"))
        return False

    # Video-only mode: reject non-video content
    if config.VIDEO_ONLY_MODE:
        if post.get("media_type", "image") not in config.ALLOWED_MEDIA_TYPES:
            log.debug("Rejected non-video (%s): %s", post.get("media_type"), post.get("source_url"))
            return False

    if _is_blocked(caption, owner):
        log.debug("Blocked content: %s (%s)", post.get("source_url"), owner)
        return False

    if not _has_parenting_context(post):
        log.debug("Missing parenting context: %s", post.get("source_url"))
        return False

    is_video = post.get("media_type") == "video"
    min_len = 0 if is_video else config.MIN_CAPTION_LENGTH
    if len(caption.strip()) < min_len:
        log.debug("Caption too short (%d chars): %s", len(caption), post.get("source_url"))
        return False

    taken_at = post.get("taken_at")
    if taken_at:
        age_hours = (time.time() - taken_at) / 3600
        if age_hours > config.SCRAPE_HOURS_BACK:
            log.debug("Too old (%.0fh): %s", age_hours, post.get("source_url"))
            return False

        if _contains_stale_event_reference(caption) and age_hours > config.SEASONAL_CONTENT_MAX_AGE_HOURS:
            log.debug("Stale event content (%.0fh): %s", age_hours, post.get("source_url"))
            return False

    if not post.get("media_url"):
        log.debug("Missing media URL: %s", post.get("source_url"))
        return False

    return True


# ── Shortcode / media PK conversion ──────────────────────────────────────────

_B64_CHARSET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"


def _shortcode_to_media_pk(shortcode: str) -> int:
    """Convert an Instagram shortcode to its numeric media PK."""
    pk = 0
    for char in shortcode:
        pk = pk * 64 + _B64_CHARSET.index(char)
    return pk


# ── API response parsing ─────────────────────────────────────────────────────

def _api_media_to_post(media: dict) -> dict:
    """Convert an Instagram API media object into the bot's flat post shape."""
    user = media.get("user", {})
    shortcode = media.get("code", "") or media.get("shortcode", "")
    media_type = media.get("media_type", 1)  # 1=image, 2=video, 8=carousel

    caption_obj = media.get("caption") or {}
    caption = caption_obj.get("text", "") if isinstance(caption_obj, dict) else ""

    source_media = media
    if media_type == 8:
        carousel_media = media.get("carousel_media", [])
        if carousel_media:
            first_media = carousel_media[0]
            if first_media.get("media_type") == 2:
                media_type = 2
                source_media = first_media

    image_candidates = source_media.get("image_versions2", {}).get("candidates", [])
    image_url = image_candidates[0].get("url", "") if image_candidates else ""

    video_url = source_media.get("video_url") or source_media.get("play_url") or ""
    if not video_url:
        video_versions = source_media.get("video_versions", [])
        if video_versions:
            video_url = video_versions[0].get("url", "")

    return {
        "account": user.get("username", ""),
        "source_url": f"{_INSTAGRAM_BASE_URL}/p/{shortcode}/" if shortcode else "",
        "caption": caption,
        "likes": int(media.get("like_count", 0) or 0),
        "media_type": "video" if media_type == 2 else "carousel" if media_type == 8 else "image",
        "media_url": video_url or image_url,
        "taken_at": int(media.get("taken_at", 0) or 0),
    }


def _extract_posts_from_sections(sections: list[dict], hashtag: str, group: str) -> list[dict]:
    posts = []
    for section in sections:
        medias = section.get("layout_content", {}).get("medias", [])
        for media_wrapper in medias:
            media = media_wrapper.get("media", {})
            post = _api_media_to_post(media)
            if not post.get("source_url"):
                continue
            post["discovery_group"] = group
            post["discovery_term"] = hashtag
            post["discovery_niche"] = hashtag
            posts.append(post)
    return posts


def _extract_posts_from_tag_web_info(payload: dict, hashtag: str, group: str) -> list[dict]:
    """Parse `/api/v1/tags/web_info` JSON into flat posts."""
    data = payload.get("data", {})
    posts = []
    posts.extend(_extract_posts_from_sections((data.get("recent") or {}).get("sections", []), hashtag, group))
    posts.extend(_extract_posts_from_sections((data.get("top") or {}).get("sections", []), hashtag, group))
    return _dedupe_posts(posts)


class _RateLimited(Exception):
    """Raised when Instagram returns 429 so callers can back off."""
    pass


# Per-endpoint adaptive health tracking (replaces single global rate limit)
class _EndpointHealth:
    """Tracks health and adaptive backoff for each Instagram API endpoint."""
    def __init__(self, name: str):
        self.name = name
        self.consecutive_429s = 0
        self.cooldown_until = 0.0

    def mark_rate_limited(self):
        self.consecutive_429s += 1
        backoff = min(30 * (2 ** (self.consecutive_429s - 1)), 900)  # 30s -> 900s
        self.cooldown_until = time.time() + backoff
        log.warning("%s: rate-limited (429 #%d), backoff %ds", self.name, self.consecutive_429s, backoff)

    def mark_success(self):
        self.consecutive_429s = 0

    def is_available(self) -> bool:
        return time.time() >= self.cooldown_until

    def wait_remaining(self) -> int:
        return max(0, int(self.cooldown_until - time.time()))

_search_health = _EndpointHealth("search")
_tag_api_health = _EndpointHealth("tag_api")
_playwright_health = _EndpointHealth("playwright")

# Backward-compatible global (delegates to tag API health)
_rate_limited_until: float = 0


def _fetch_tag_web_info(session: requests.Session, hashtag: str, group: str, limit: int) -> list[dict]:
    """Fetch hashtag media directly from Instagram's authenticated web tag endpoint."""
    global _rate_limited_until
    if time.time() < _rate_limited_until:
        log.debug("#%s (%s): skipping — still in rate-limit cooldown", hashtag, group)
        raise _RateLimited(hashtag)
    try:
        response = session.get(
            _TAG_WEB_INFO_URL,
            params={"tag_name": hashtag},
            headers={"Referer": f"{_INSTAGRAM_BASE_URL}/explore/tags/{hashtag}/"},
            timeout=15,
        )
        if response.status_code == 404:
            log.warning("#%s (%s): tag web_info returned 404", hashtag, group)
            return []
        if response.status_code == 429:
            _rate_limited_until = time.time() + 300  # back off 5 minutes
            log.warning("#%s (%s): tag web_info rate-limited (429) — backing off 5 min", hashtag, group)
            raise _RateLimited(hashtag)
        response.raise_for_status()
        posts = _extract_posts_from_tag_web_info(response.json(), hashtag, group)
        if posts:
            posts.sort(key=lambda p: (p.get("taken_at", 0), p.get("likes", 0)), reverse=True)
            log.info("#%s (%s) -> %d posts from tag web_info", hashtag, group, len(posts))
        return posts[:limit]
    except _RateLimited:
        raise
    except Exception as exc:
        log.warning("#%s (%s): tag web_info failed: %s", hashtag, group, exc)
        return []


def _fetch_search_results(session: requests.Session, query: str, group: str, limit: int) -> list[dict]:
    """Fetch media via Instagram's search endpoint (more resilient than tag web_info)."""
    try:
        response = session.get(
            _SEARCH_URL,
            params={"query": query},
            headers={"Referer": f"{_INSTAGRAM_BASE_URL}/explore/search/"},
            timeout=15,
        )
        if response.status_code == 429:
            log.warning("Search endpoint rate-limited (429) for query '%s'", query)
            raise _RateLimited(query)
        if response.status_code != 200:
            log.warning("Search endpoint returned %d for query '%s'", response.status_code, query)
            return []

        data = response.json()
        media_grid = data.get("media_grid", {})
        if not isinstance(media_grid, dict):
            return []

        sections = media_grid.get("sections", [])
        posts = _extract_posts_from_sections(sections, query, group)
        if posts:
            posts.sort(key=lambda p: (p.get("taken_at", 0), p.get("likes", 0)), reverse=True)
            log.info("Search '%s' (%s) -> %d posts", query, group, len(posts))
        return posts[:limit]
    except _RateLimited:
        raise
    except Exception as exc:
        log.warning("Search failed for '%s' (%s): %s", query, group, exc)
        return []


def _dedupe_posts(posts: list[dict]) -> list[dict]:
    seen = set()
    unique = []
    for post in posts:
        url = post.get("source_url", "")
        if url and url not in seen:
            seen.add(url)
            unique.append(post)
    return unique


async def _scrape_hashtag_page_with_intercept(page, hashtag: str, group: str, limit: int) -> list[dict]:
    """
    Load a hashtag explore page via Playwright and capture media data from
    the API responses that Instagram's own JS makes.  Falls back to visiting
    individual post pages if no API traffic is intercepted.
    """
    captured_media: list[dict] = []

    async def _on_response(response):
        """Intercept Instagram API responses triggered by page JS."""
        url = response.url
        try:
            # Match any endpoint that returns media grid sections
            is_tag_api = "/api/v1/tags/" in url or "/api/v1/feed/tag/" in url
            is_search_api = "/fbsearch/" in url or "top_serp" in url
            is_graphql = "graphql" in url
            
            if not (is_tag_api or is_search_api or is_graphql):
                return

            ct = response.headers.get("content-type", "")
            if "json" not in ct and "javascript" not in ct:
                return

            body_text = await response.text()
            if "media" not in body_text or "image_versions" not in body_text:
                return

            body = json.loads(body_text) if body_text.startswith("{") else None
            if not body:
                return

            # Extract sections from various response shapes
            sections = []

            # /fbsearch/web/top_serp format: {media_grid: {sections: [...]}}
            media_grid = body.get("media_grid", {})
            if isinstance(media_grid, dict):
                sections.extend(media_grid.get("sections", []))

            # tags/web_info format: {data: {recent: {sections}, top: {sections}}}
            data = body.get("data", {})
            if isinstance(data, dict):
                sections.extend((data.get("recent") or {}).get("sections", []))
                sections.extend((data.get("top") or {}).get("sections", []))

            # Direct sections at top level
            sections.extend(body.get("sections", []))

            for section in sections:
                medias = section.get("layout_content", {}).get("medias", [])
                for mw in medias:
                    media = mw.get("media", {})
                    if media:
                        post = _api_media_to_post(media)
                        if post.get("source_url"):
                            post["discovery_group"] = group
                            post["discovery_term"] = hashtag
                            post["discovery_niche"] = hashtag
                            captured_media.append(post)
        except Exception:
            pass

    page.on("response", _on_response)
    try:
        url = f"{_INSTAGRAM_BASE_URL}/explore/tags/{hashtag}/"
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(5000)
        except Exception as exc:
            log.warning("#%s: failed to load explore page: %s", hashtag, exc)
            return []

        # If the intercepted responses already gave us media, great.
        if captured_media:
            deduped = _dedupe_posts(captured_media)
            deduped.sort(key=lambda p: (p.get("taken_at", 0), p.get("likes", 0)), reverse=True)
            log.info("#%s (%s) -> %d posts from intercepted API", hashtag, group, len(deduped))
            return deduped[:limit]

        # Scroll to trigger lazy loading of content
        for _ in range(3):
            await page.evaluate("window.scrollBy(0, window.innerHeight)")
            await page.wait_for_timeout(2000)
            if captured_media:
                break

        if captured_media:
            deduped = _dedupe_posts(captured_media)
            deduped.sort(key=lambda p: (p.get("taken_at", 0), p.get("likes", 0)), reverse=True)
            log.info("#%s (%s) -> %d posts from intercepted API (after scroll)", hashtag, group, len(deduped))
            return deduped[:limit]

        # Fallback: scrape post pages individually via Playwright
        links = await page.locator('a[href*="/p/"], a[href*="/reel/"]').all()
        if not links:
            await page.wait_for_timeout(3000)
            links = await page.locator('a[href*="/p/"], a[href*="/reel/"]').all()

        shortcodes: list[tuple[str, bool]] = []  # (shortcode, is_reel)
        seen: set[str] = set()
        for link in links:
            href = await link.get_attribute("href") or ""
            m = re.search(r"/(?:p|reel)/([A-Za-z0-9_-]+)/", href)
            if m and m.group(1) not in seen:
                seen.add(m.group(1))
                is_reel = "/reel/" in href
                shortcodes.append((m.group(1), is_reel))

        if not shortcodes:
            log.warning("#%s (%s): no posts found on explore page", hashtag, group)
            return []

        posts: list[dict] = []
        for sc, is_reel in shortcodes[:limit]:
            post = await _scrape_post_page(page, sc, group, hashtag)
            if post and is_reel:
                post["media_type"] = "video"  # reel links are always video
            if post:
                posts.append(post)

        posts.sort(key=lambda p: (p.get("taken_at", 0), p.get("likes", 0)), reverse=True)
        log.info("#%s (%s) -> %d posts from %d shortcodes (page scrape)", hashtag, group, len(posts), len(shortcodes))
        return posts[:limit]
    finally:
        page.remove_listener("response", _on_response)


async def _scrape_post_page(page, shortcode: str, group: str, hashtag: str) -> dict | None:
    """Visit an individual post page and extract data from meta tags / embedded JSON."""
    url = f"{_INSTAGRAM_BASE_URL}/p/{shortcode}/"
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=20000)
        await page.wait_for_timeout(2000)
    except Exception as exc:
        log.debug("Failed to load post page %s: %s", shortcode, exc)
        return None

    try:
        data = await page.evaluate("""() => {
            const getMeta = (name) => {
                const el = document.querySelector(`meta[property="${name}"], meta[name="${name}"]`);
                return el ? el.getAttribute("content") : "";
            };
            // Check for video elements on the page
            const hasVideo = !!(
                document.querySelector('video') ||
                document.querySelector('[data-visualcompletion="media-vc-image"] video')
            );
            // Check the og:type tag
            const ogType = getMeta("og:type") || "";
            return {
                description: getMeta("og:description") || getMeta("description") || "",
                image: getMeta("og:image") || "",
                video: getMeta("og:video") || getMeta("og:video:url") || getMeta("og:video:secure_url") || "",
                type: ogType,
                title: getMeta("og:title") || "",
                hasVideo: hasVideo,
                isVideoType: ogType.includes("video"),
                pageUrl: window.location.href,
            };
        }""")
    except Exception as exc:
        log.debug("Failed to extract meta from %s: %s", shortcode, exc)
        return None

    likes, account, caption, taken_at = _parse_meta_description(data.get("description", ""))
    video_url = data.get("video", "")
    image_url = data.get("image", "")

    # Detect video: og:video tag, og:type=video, <video> element on page, or /reel/ in URL
    is_video = bool(video_url) or data.get("isVideoType", False) or data.get("hasVideo", False)
    page_url = data.get("pageUrl", "")
    if "/reel/" in page_url or "/reel/" in url:
        is_video = True

    media_type = "video" if is_video else "image"

    if not account and not caption:
        return None

    return {
        "account": account,
        "source_url": f"{_INSTAGRAM_BASE_URL}/p/{shortcode}/",
        "caption": caption,
        "likes": likes,
        "media_type": media_type,
        "media_url": video_url or image_url,
        "taken_at": taken_at,
        "discovery_group": group,
        "discovery_term": hashtag,
        "discovery_niche": hashtag,
    }


async def _fetch_hashtag_posts(page, session: requests.Session, hashtag: str, group: str, limit: int) -> list[dict]:
    """Fetch live posts for a hashtag, preferring tag web_info API over DOM scraping."""
    try:
        api_posts = _fetch_tag_web_info(session, hashtag, group, limit)
        if api_posts:
            return api_posts[:limit]
    except _RateLimited:
        log.debug("#%s: API rate-limited, falling through to DOM scrape", hashtag)

    return await _scrape_hashtag_page_with_intercept(page, hashtag, group, limit)


# Search queries mapped to discovery groups (for fbsearch/web/top_serp endpoint)
_SEARCH_QUERIES: dict[str, list[str]] = {
    "general_parenting": [
        "gentle parenting", "new mum advice", "motherhood journey",
        "postnatal wellness", "toddler parenting tips",
    ],
    "black_parenting": [
        "black motherhood", "black mom joy", "melanin mama",
    ],
    "south_asian_parenting": [
        "desi parenting", "asian mum life", "south asian motherhood",
    ],
    "multicultural_parenting": [
        "multicultural parenting", "mixed race family", "diverse families",
    ],
}


def _default_discovery_terms() -> list[tuple[str, str]]:
    terms: list[tuple[str, str]] = []
    for group, hashtags in config.DISCOVERY_POOLS.items():
        for hashtag in hashtags:
            if hashtag:
                terms.append((group, hashtag))
    return terms


def _default_search_terms() -> list[tuple[str, str]]:
    """Return (group, query) pairs for the search endpoint."""
    terms: list[tuple[str, str]] = []
    for group, queries in _SEARCH_QUERIES.items():
        for query in queries:
            terms.append((group, query))
    return terms


def _custom_discovery_terms(niches: list[str]) -> list[tuple[str, str]]:
    terms = []
    for niche in niches:
        hashtag = _niche_to_hashtag(niche)
        if hashtag:
            terms.append(("custom", hashtag))
    return terms


def _limit_per_account(posts: list[dict], limit: int) -> list[dict]:
    """Cap the number of queued items contributed by one source account."""
    if limit <= 0:
        return posts

    counts = Counter()
    limited = []
    for post in posts:
        account = post.get("account", "").strip().lower()
        if not account:
            continue
        if counts[account] >= limit:
            continue
        counts[account] += 1
        limited.append(post)
    return limited


def _balanced_discovery_order(posts: list[dict], group_order: list[str]) -> list[dict]:
    """
    Interleave posts across discovery groups and prefer videos within each group.

    This improves source diversity without trying to infer anyone's identity from
    the media itself. We diversify based on the explicit discovery tags we chose.
    """
    buckets: dict[str, dict[str, deque]] = defaultdict(lambda: {"video": deque(), "other": deque()})
    seen_groups = set()

    for post in posts:
        group = post.get("discovery_group", "other")
        seen_groups.add(group)
        bucket_key = "video" if post.get("media_type") == "video" else "other"
        buckets[group][bucket_key].append(post)

    ordered_groups = [group for group in group_order if group in seen_groups]
    ordered_groups.extend(sorted(seen_groups - set(ordered_groups)))

    ordered: list[dict] = []
    while True:
        added_any = False
        for group in ordered_groups:
            bucket = buckets[group]
            queue = bucket["video"] if bucket["video"] else bucket["other"]
            if not queue:
                continue
            ordered.append(queue.popleft())
            added_any = True
        if not added_any:
            break

    return ordered


# ── Public API ───────────────────────────────────────────────────────────────

def _is_session_expired(session: requests.Session) -> bool:
    """
    Quick probe to check whether the saved cookies are still valid.
    Uses the lightweight /api/v1/accounts/current_user/ endpoint.
    Returns True when the session looks expired (401 / redirect to login).
    """
    try:
        r = session.get(
            _INSTAGRAM_BASE_URL + "/api/v1/accounts/current_user/",
            timeout=10,
            allow_redirects=False,
        )
        if r.status_code in (401, 403):
            return True
        if r.status_code == 302:
            location = r.headers.get("Location", "")
            if "login" in location:
                return True
        return False
    except Exception:
        # Network error — assume session is still fine; scraper will tell us.
        return False


async def _refresh_session_with_playwright() -> bool:
    """
    Use Playwright to re-authenticate and persist fresh cookies.
    Returns True on success, False on failure.
    """
    try:
        from playwright.async_api import async_playwright
        from bot.instagram_auth import create_authenticated_context

        async with async_playwright() as pw:
            ctx = await create_authenticated_context(
                pw,
                viewport={"width": 1280, "height": 900},
                user_agent=_DESKTOP_USER_AGENT,
            )
            await ctx.browser.close()
        log.info("Auth refreshed via Playwright successfully")
        return True
    except Exception as exc:
        log.error("Playwright auth refresh failed: %s", exc)
        return False


async def _discover_via_search(
    session: requests.Session,
    search_terms: list[tuple[str, str]],
) -> list[dict]:
    """
    Primary fast path: use the fbsearch/web/top_serp endpoint.
    Adaptive backoff: waits but ALWAYS resumes (never breaks permanently).
    """
    all_posts: list[dict] = []
    for group, query in search_terms:
        if not _search_health.is_available():
            wait = _search_health.wait_remaining()
            if wait > 60:
                log.debug("Search endpoint cooling down (%ds), skipping '%s'", wait, query)
                continue
            else:
                await asyncio.sleep(wait)

        try:
            posts = _fetch_search_results(session, query, group, config.SEARCH_PER_NICHE)
            all_posts.extend(posts)
            _search_health.mark_success()
        except _RateLimited:
            _search_health.mark_rate_limited()
            await asyncio.sleep(min(_search_health.wait_remaining(), 30))
            continue
        except Exception as exc:
            log.warning("Search error for '%s' (%s): %s", query, group, exc)
        await asyncio.sleep(3 + random.uniform(0, 3))
    return all_posts


async def _discover_via_api(
    session: requests.Session,
    terms: list[tuple[str, str]],
) -> list[dict]:
    """
    Secondary fast path: fetch hashtags using tag web_info API.
    Adaptive backoff: waits but ALWAYS resumes (never breaks permanently).
    """
    all_posts: list[dict] = []
    for group, hashtag in terms:
        if not _tag_api_health.is_available():
            wait = _tag_api_health.wait_remaining()
            if wait > 60:
                log.debug("Tag API cooling down (%ds), skipping #%s", wait, hashtag)
                continue
            else:
                await asyncio.sleep(wait)

        try:
            posts = _fetch_tag_web_info(session, hashtag, group, config.SEARCH_PER_NICHE)
            all_posts.extend(posts)
            _tag_api_health.mark_success()
        except _RateLimited:
            _tag_api_health.mark_rate_limited()
            await asyncio.sleep(min(_tag_api_health.wait_remaining(), 30))
            continue
        except Exception as exc:
            log.warning("API path error for #%s (%s): %s", hashtag, group, exc)
        await asyncio.sleep(5 + random.uniform(0, 3))
    return all_posts


async def _discover_via_playwright(
    terms: list[tuple[str, str]],
    session: requests.Session,
) -> list[dict]:
    """
    Slow path (DOM fallback): use Playwright to scrape hashtag pages.
    Only called when the API path returns nothing useful.
    """
    from playwright.async_api import async_playwright
    from bot.instagram_auth import create_authenticated_context

    all_posts: list[dict] = []
    async with async_playwright() as pw:
        ctx = await create_authenticated_context(
            pw,
            viewport={"width": 1280, "height": 900},
            user_agent=_DESKTOP_USER_AGENT,
        )
        page = await ctx.new_page()
        try:
            for group, hashtag in terms:
                try:
                    posts = await _fetch_hashtag_posts(
                        page, session, hashtag, group, config.SEARCH_PER_NICHE,
                    )
                    all_posts.extend(posts)
                except Exception as exc:
                    log.warning("Playwright path error for #%s (%s): %s", hashtag, group, exc)
                await asyncio.sleep(2)
        finally:
            await ctx.browser.close()
    return all_posts


async def _discover_via_playwright_explore(search_terms: list[tuple[str, str]]) -> list[dict]:
    """
    Fallback: use Playwright to visit the explore page and capture media
    from intercepted API responses. Works when tag pages are empty.
    """
    from playwright.async_api import async_playwright
    from bot.instagram_auth import create_authenticated_context

    all_posts: list[dict] = []

    async with async_playwright() as pw:
        ctx = await create_authenticated_context(
            pw,
            viewport={"width": 1280, "height": 900},
            user_agent=_DESKTOP_USER_AGENT,
        )
        page = await ctx.new_page()
        try:
            # Capture media from intercepted responses on explore page
            captured: list[dict] = []

            async def _on_response(response):
                try:
                    ct = response.headers.get("content-type", "")
                    if "json" not in ct:
                        return
                    body_text = await response.text()
                    if "image_versions" not in body_text:
                        return
                    body = json.loads(body_text) if body_text.startswith("{") else None
                    if not body:
                        return

                    sections = []
                    media_grid = body.get("media_grid", {})
                    if isinstance(media_grid, dict):
                        sections.extend(media_grid.get("sections", []))
                    data = body.get("data", {})
                    if isinstance(data, dict):
                        sections.extend((data.get("recent") or {}).get("sections", []))
                        sections.extend((data.get("top") or {}).get("sections", []))
                    sections.extend(body.get("sections", []))

                    for section in sections:
                        medias = section.get("layout_content", {}).get("medias", [])
                        for mw in medias:
                            media = mw.get("media", {})
                            if media:
                                post = _api_media_to_post(media)
                                if post.get("source_url"):
                                    post["discovery_group"] = "explore"
                                    post["discovery_term"] = "explore"
                                    post["discovery_niche"] = "explore"
                                    captured.append(post)
                except Exception:
                    pass

            page.on("response", _on_response)

            # Visit explore page
            await page.goto(f"{_INSTAGRAM_BASE_URL}/explore/", wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(5000)

            # Scroll to trigger more content
            for _ in range(3):
                await page.evaluate("window.scrollBy(0, window.innerHeight)")
                await page.wait_for_timeout(2000)

            all_posts.extend(captured)
            log.info("Playwright explore page: %d posts captured via interception", len(captured))

            # Also try search via browser for each query term
            for group, query in search_terms[:8]:  # limit to avoid rate limits
                captured.clear()
                try:
                    search_url = f"{_INSTAGRAM_BASE_URL}/explore/search/?q={query.replace(' ', '+')}"
                    await page.goto(search_url, wait_until="domcontentloaded", timeout=20000)
                    await page.wait_for_timeout(3000)
                    all_posts.extend(captured)
                except Exception as exc:
                    log.debug("Playwright search for '%s' failed: %s", query, exc)
                await asyncio.sleep(2)

            page.remove_listener("response", _on_response)

            # Also scrape shortcodes from explore page links
            await page.goto(f"{_INSTAGRAM_BASE_URL}/explore/", wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(3000)
            links = await page.locator('a[href*="/p/"], a[href*="/reel/"]').all()
            shortcodes: list[tuple[str, bool]] = []
            seen: set[str] = set()
            for link in links:
                href = await link.get_attribute("href") or ""
                m = re.search(r"/(?:p|reel)/([A-Za-z0-9_-]+)/", href)
                if m and m.group(1) not in seen:
                    seen.add(m.group(1))
                    shortcodes.append((m.group(1), "/reel/" in href))

            if shortcodes:
                log.info("Explore page: scraping %d shortcodes", len(shortcodes))
                for sc, is_reel in shortcodes[:20]:
                    post = await _scrape_post_page(page, sc, "explore", "explore")
                    if post and is_reel:
                        post["media_type"] = "video"
                    if post:
                        all_posts.append(post)
        finally:
            await ctx.browser.close()

    return all_posts


async def discover_content(niches: list[str] | None = None) -> list[dict]:
    """
    Discover fresh Instagram content across balanced niche pools.

    Strategy (in order):
    1. Try the search endpoint (fbsearch/web/top_serp) — most resilient.
    2. Try tag web_info API if search didn't return enough.
    3. If cookies look expired, refresh via Playwright and retry.
    4. If APIs still yield nothing, fall back to Playwright explore page.

    The returned order is intentionally interleaved by discovery group and
    biased toward video so the queue does not collapse into one homogeneous
    creator bucket.
    """
    tag_terms = _custom_discovery_terms(niches) if niches is not None else _default_discovery_terms()
    search_terms = _default_search_terms() if niches is None else [(
        "custom", niche) for niche in niches]
    group_order = list(dict.fromkeys(
        [group for group, _ in search_terms] + [group for group, _ in tag_terms]
    ))

    session = _create_api_session()
    all_posts: list[dict] = []

    try:
        # ── Step 0: check if session is valid ─────────────────────────────────
        if _is_session_expired(session):
            log.warning("Instagram session appears expired — refreshing via Playwright")
            refreshed = await _refresh_session_with_playwright()
            if refreshed:
                session.close()
                session = _create_api_session()
            else:
                log.error("Auth refresh failed — will attempt with existing cookies")

        # ── Step 1: search endpoint (most resilient) ──────────────────────────
        all_posts = await _discover_via_search(session, search_terms)
        log.info("Search discovery: %d raw posts", len(all_posts))

        # ── Step 2: tag web_info API if search didn't return enough ───────────
        if len(all_posts) < 10:
            log.info("Search returned few results (%d), trying tag web_info API", len(all_posts))
            tag_posts = await _discover_via_api(session, tag_terms)
            log.info("Tag API discovery: %d raw posts", len(tag_posts))
            all_posts.extend(tag_posts)

        # ── Step 3: Playwright explore fallback if APIs returned nothing ──────
        if not all_posts:
            log.warning("All API paths returned no posts — falling back to Playwright explore")
            try:
                all_posts = await _discover_via_playwright_explore(search_terms)
                log.info("Playwright explore discovery: %d raw posts", len(all_posts))
            except Exception as exc:
                log.error("Playwright explore fallback failed: %s", exc)

    finally:
        session.close()

    filtered = [post for post in all_posts if _passes_hard_filters(post)]
    deduped = _dedupe_posts(filtered)
    deduped.sort(key=lambda post: (post.get("taken_at", 0), post.get("likes", 0)), reverse=True)

    limited = _limit_per_account(deduped, config.MAX_DISCOVERY_POSTS_PER_ACCOUNT)
    ordered = _balanced_discovery_order(limited, group_order)

    log.info(
        "Discovery complete: %d posts after filters (from %d raw, %d after account cap)",
        len(ordered),
        len(all_posts),
        len(limited),
    )
    return ordered


def discover_content_sync(niches: list[str] | None = None) -> list[dict]:
    """Synchronous wrapper for discover_content."""
    return asyncio.run(discover_content(niches))
