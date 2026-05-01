"""Configuration helpers for the local crawler baseline."""

from __future__ import annotations

from dataclasses import dataclass
import os


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value in (None, ""):
        return default
    return int(value)


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value in (None, ""):
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(slots=True)
class CrawlerConfig:
    request_interval_ms: int = 1500
    max_concurrency: int = 1
    max_retries: int = 2
    request_timeout_seconds: int = 20
    enable_fallback_fetcher: bool = False
    fetcher_mode: str = "default"

    @classmethod
    def from_env(cls) -> "CrawlerConfig":
        return cls(
            request_interval_ms=_env_int("REQUEST_INTERVAL_MS", 1500),
            max_concurrency=_env_int("MAX_CONCURRENCY", 1),
            max_retries=_env_int("MAX_RETRIES", 2),
            request_timeout_seconds=_env_int("REQUEST_TIMEOUT_SECONDS", 20),
            enable_fallback_fetcher=_env_bool("ENABLE_FALLBACK_FETCHER", False),
            fetcher_mode=os.getenv("FETCHER_MODE", "default").strip().lower() or "default",
        )
