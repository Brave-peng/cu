"""Default low-pressure HTTP fetcher."""

from __future__ import annotations

from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from event_log_baseline.config import CrawlerConfig
from event_log_baseline.models import FetchResult

from .base import BaseFetcher


class DefaultFetcher(BaseFetcher):
    mode = "default"

    def __init__(self, config: CrawlerConfig) -> None:
        self.config = config

    def fetch(self, url: str) -> FetchResult:
        request = Request(
            url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/135.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
            },
        )
        try:
            with urlopen(request, timeout=self.config.request_timeout_seconds) as response:
                payload = response.read().decode("utf-8", errors="replace")
                headers = {key: value for key, value in response.headers.items()}
                status_code = getattr(response, "status", None) or response.getcode() or 200
                return FetchResult(
                    url=url,
                    status_code=status_code,
                    content=payload,
                    headers=headers,
                )
        except HTTPError as exc:
            return FetchResult(url=url, status_code=exc.code, content="", error=str(exc))
        except URLError as exc:
            return FetchResult(url=url, status_code=0, content="", error=str(exc.reason))
