from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Callable, TypeVar

try:
    from flask_caching import Cache
except Exception:  # pragma: no cover
    Cache = None  # type: ignore[misc,assignment]


T = TypeVar("T")

_CACHE: "Cache | None" = None


def init_cache(flask_server: Any) -> "Cache | None":
    """
    Initialize a process-local Cache instance.

    PythonAnywhere deployments may run multiple worker processes; each will have its own
    in-process cache. Using filesystem cache provides persistence per-worker and avoids
    recomputing expensive lookups on each callback.
    """
    global _CACHE
    if _CACHE is not None:
        return _CACHE
    if Cache is None:
        return None

    cache_dir = os.environ.get("DASH_CACHE_DIR")
    if not cache_dir:
        cache_dir = str(Path(os.environ.get("TMPDIR", "/tmp")) / "biomarker-dashboard-cache")
    Path(cache_dir).mkdir(parents=True, exist_ok=True)

    default_timeout = int(os.environ.get("DASH_CACHE_DEFAULT_TIMEOUT", "3600"))
    cache_type = os.environ.get("DASH_CACHE_TYPE", "filesystem")

    _CACHE = Cache(
        flask_server,
        config={
            "CACHE_TYPE": cache_type,
            "CACHE_DIR": cache_dir,
            "CACHE_DEFAULT_TIMEOUT": default_timeout,
        },
    )
    return _CACHE


def get_cache() -> "Cache | None":
    return _CACHE


def memoize(timeout: int | None = None) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Safe memoize decorator: if caching isn't initialized, it becomes a no-op.
    """

    def _decorator(fn: Callable[..., T]) -> Callable[..., T]:
        cache = get_cache()
        if cache is None:
            return fn
        return cache.memoize(timeout=timeout)(fn)  # type: ignore[return-value]

    return _decorator

