"""CLI entrypoints for local crawler baseline checks."""

from __future__ import annotations

from dataclasses import asdict
import json

from .config import CrawlerConfig
from .pipeline import CrawlerBaseline


def run_smoke() -> dict[str, object]:
    baseline = CrawlerBaseline(CrawlerConfig.from_env())
    fetch_result = baseline.fetch("data:text/plain,hello-event-log")
    list_items = baseline.parse_list(
        '<html><body><a href="https://example.com/article?utm_source=x">Copper story</a></body></html>'
    )
    detail = baseline.parse_detail(
        """
        <html><head><title>Copper Story</title></head>
        <body><article class="entry-content"><time>2026-04-19 09:00:00</time><p>Hello copper market.</p></article></body></html>
        """
    )
    return {
        "config": baseline.smoke_summary(),
        "fetch_status_code": fetch_result.status_code,
        "fetch_content": fetch_result.content,
        "list_items": [asdict(item) for item in list_items],
        "detail_title": detail.title,
        "detail_published_text": detail.published_text,
        "detail_content": detail.content,
        "dedupe_key": baseline.dedupe_key("https://Example.com/article/?utm_source=x#frag"),
    }


if __name__ == "__main__":
    print(json.dumps(run_smoke(), ensure_ascii=False, indent=2))
