"""Fetcher factory."""

from __future__ import annotations

from event_log_baseline.config import CrawlerConfig

from .base import BaseFetcher
from .browser import BrowserFetcher
from .default import DefaultFetcher
from .proxy import ProxyFetcher


def build_fetcher(config: CrawlerConfig) -> BaseFetcher:
    mode = config.fetcher_mode
    if mode == "default":
        return DefaultFetcher(config)
    if mode == "proxy":
        return ProxyFetcher(config)
    if mode == "browser":
        return BrowserFetcher(config)
    raise ValueError(f"Unsupported fetcher mode: {mode}")
