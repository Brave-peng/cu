"""High-level crawler baseline composition."""

from __future__ import annotations

from dataclasses import asdict

from .config import CrawlerConfig
from .fetchers import build_fetcher
from .models import DetailItem, FetchResult, ListItem
from .parsers import make_dedupe_key, parse_detail_html, parse_list_html


class CrawlerBaseline:
    def __init__(self, config: CrawlerConfig | None = None) -> None:
        self.config = config or CrawlerConfig.from_env()
        self.fetcher = build_fetcher(self.config)

    def fetch(self, url: str) -> FetchResult:
        return self.fetcher.fetch(url)

    def parse_list(self, html: str) -> list[ListItem]:
        return parse_list_html(html)

    def parse_detail(self, html: str) -> DetailItem:
        return parse_detail_html(html)

    def dedupe_key(self, url: str) -> str:
        return make_dedupe_key(url)

    def smoke_summary(self) -> dict[str, object]:
        return {
            "fetcher_mode": self.fetcher.mode,
            "fallback_enabled": self.config.enable_fallback_fetcher,
            "request_interval_ms": self.config.request_interval_ms,
            "max_concurrency": self.config.max_concurrency,
            "max_retries": self.config.max_retries,
            "request_timeout_seconds": self.config.request_timeout_seconds,
        }
