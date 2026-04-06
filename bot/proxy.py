"""
Proxy management for Mother's Joy Instagram bot.

Routes browser and API traffic through configurable proxies.
Supports HTTP, HTTPS, and SOCKS5 proxies.
Gracefully degrades when no proxy is configured.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlparse

import requests

from bot import config

log = logging.getLogger(__name__)


@dataclass
class ProxyConfig:
    url: str
    type: str = "http"      # http, https, socks5
    server: str = ""        # host:port for Playwright
    username: str = ""
    password: str = ""

    @classmethod
    def from_url(cls, url: str) -> Optional["ProxyConfig"]:
        """Parse proxy URL like socks5://user:pass@host:port."""
        if not url:
            return None

        parsed = urlparse(url)
        proxy_type = parsed.scheme or "http"
        server = f"{parsed.hostname}:{parsed.port}" if parsed.port else parsed.hostname or ""
        return cls(
            url=url,
            type=proxy_type,
            server=f"{proxy_type}://{server}",
            username=parsed.username or "",
            password=parsed.password or "",
        )


def get_proxy() -> Optional[ProxyConfig]:
    """Get configured proxy or None. Checks .env PROXY_URL."""
    proxy_url = getattr(config, "PROXY_URL", "") or os.getenv("PROXY_URL", "")
    if not proxy_url:
        return None
    return ProxyConfig.from_url(proxy_url)


def apply_to_playwright(proxy_config: Optional[ProxyConfig] = None) -> dict | None:
    """Return Playwright proxy argument dict, or None if no proxy."""
    pc = proxy_config or get_proxy()
    if not pc:
        return None

    result = {"server": pc.server}
    if pc.username:
        result["username"] = pc.username
    if pc.password:
        result["password"] = pc.password

    return result


def apply_to_requests(proxy_config: Optional[ProxyConfig] = None,
                      session: requests.Session | None = None) -> requests.Session:
    """Configure a requests.Session with proxy. Creates one if not provided."""
    if session is None:
        session = requests.Session()

    pc = proxy_config or get_proxy()
    if not pc:
        return session

    proxies = {
        "http": pc.url,
        "https": pc.url,
    }
    session.proxies.update(proxies)
    log.debug("Requests session proxy set: %s", pc.type)
    return session


def test_proxy(proxy_config: Optional[ProxyConfig] = None, timeout: int = 10) -> bool:
    """Verify proxy connectivity by fetching a test URL."""
    pc = proxy_config or get_proxy()
    if not pc:
        return True  # No proxy = direct connection works

    try:
        proxies = {"http": pc.url, "https": pc.url}
        resp = requests.get(
            "https://httpbin.org/ip",
            proxies=proxies,
            timeout=timeout,
        )
        if resp.status_code == 200:
            data = resp.json()
            log.info("Proxy working — external IP: %s", data.get("origin", "unknown"))
            return True
        log.warning("Proxy test returned HTTP %d", resp.status_code)
        return False
    except Exception as exc:
        log.warning("Proxy test failed: %s", exc)
        return False


def get_proxy_status() -> dict:
    """Proxy status for dashboard display."""
    pc = get_proxy()
    if not pc:
        return {"configured": False, "type": "none", "status": "direct"}

    return {
        "configured": True,
        "type": pc.type,
        "server": pc.server,
        "has_auth": bool(pc.username),
        "status": "configured",
    }
