"""Browser fetcher interface placeholder."""

from __future__ import annotations

from event_log_baseline.config import CrawlerConfig

from .default import DefaultFetcher


class BrowserFetcher(DefaultFetcher):
    mode = "browser"

    def __init__(self, config: CrawlerConfig) -> None:
        super().__init__(config)
