"""
Local media cache for Mother's Joy Instagram bot.

Downloads and stores media files locally so expired Instagram CDN URLs
never cause posting failures. Media is cached at discovery/enqueue time.
"""

from __future__ import annotations

import hashlib
import logging
import os
import shutil
import time
from pathlib import Path

import requests

from bot import config

log = logging.getLogger(__name__)

# Limits
MAX_CACHE_SIZE_MB = 2000       # 2 GB cap
CACHE_RETENTION_HOURS = 168    # 7 days
DOWNLOAD_TIMEOUT = 60


def _cache_dir() -> Path:
    d = config.get_media_cache_dir()
    d.mkdir(parents=True, exist_ok=True)
    return d


def _content_hash(source_url: str) -> str:
    """Deterministic filename from source URL."""
    return hashlib.sha256(source_url.encode()).hexdigest()[:24]


def cache_media(source_url: str, media_url: str, media_type: str = "image") -> Path | None:
    """Download and cache media. Returns local path or None on failure.
    Idempotent: returns existing cached file if present."""
    if not source_url or not media_url:
        return None

    # Check existing
    existing = get_cached_path(source_url)
    if existing:
        return existing

    ext = ".mp4" if media_type == "video" else ".jpg"
    filename = _content_hash(source_url) + ext
    dest = _cache_dir() / filename

    try:
        resp = requests.get(media_url, timeout=DOWNLOAD_TIMEOUT, stream=True)
        resp.raise_for_status()

        with open(dest, "wb") as f:
            for chunk in resp.iter_content(chunk_size=65536):
                f.write(chunk)

        size_mb = dest.stat().st_size / (1024 * 1024)
        log.debug("Cached media: %s (%.1f MB) -> %s", source_url[:60], size_mb, filename)
        return dest

    except Exception as exc:
        log.debug("Media cache download failed for %s: %s", source_url[:60], exc)
        # Clean up partial download
        if dest.exists():
            try:
                dest.unlink()
            except Exception:
                pass
        return None


def get_cached_path(source_url: str) -> Path | None:
    """Return cached file path if it exists, None otherwise."""
    if not source_url:
        return None

    base_hash = _content_hash(source_url)
    cache = _cache_dir()

    for ext in (".mp4", ".jpg", ".jpeg", ".png"):
        candidate = cache / (base_hash + ext)
        if candidate.exists() and candidate.stat().st_size > 0:
            return candidate

    return None


def is_cached(source_url: str) -> bool:
    return get_cached_path(source_url) is not None


def evict_stale():
    """Remove files older than retention period and enforce size cap."""
    cache = _cache_dir()
    if not cache.exists():
        return

    cutoff = time.time() - (CACHE_RETENTION_HOURS * 3600)
    removed_age = 0

    # Phase 1: remove files older than retention
    for f in cache.iterdir():
        if not f.is_file():
            continue
        try:
            if f.stat().st_mtime < cutoff:
                f.unlink()
                removed_age += 1
        except Exception:
            pass

    # Phase 2: enforce size cap (LRU eviction)
    files = []
    total_size = 0
    for f in cache.iterdir():
        if not f.is_file():
            continue
        try:
            stat = f.stat()
            files.append((f, stat.st_mtime, stat.st_size))
            total_size += stat.st_size
        except Exception:
            pass

    max_bytes = MAX_CACHE_SIZE_MB * 1024 * 1024
    removed_size = 0
    if total_size > max_bytes:
        # Sort by oldest first
        files.sort(key=lambda x: x[1])
        for f, _, size in files:
            if total_size <= max_bytes:
                break
            try:
                f.unlink()
                total_size -= size
                removed_size += 1
            except Exception:
                pass

    if removed_age or removed_size:
        log.info("Media cache eviction: %d aged out, %d size-capped", removed_age, removed_size)


def cache_stats() -> dict:
    """Return cache statistics for dashboard."""
    cache = _cache_dir()
    if not cache.exists():
        return {"files": 0, "size_mb": 0}

    files = 0
    total_size = 0
    for f in cache.iterdir():
        if f.is_file():
            files += 1
            try:
                total_size += f.stat().st_size
            except Exception:
                pass

    return {
        "files": files,
        "size_mb": round(total_size / (1024 * 1024), 1),
        "max_mb": MAX_CACHE_SIZE_MB,
    }
