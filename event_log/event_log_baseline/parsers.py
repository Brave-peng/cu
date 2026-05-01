"""Generic parsing helpers for source-specific crawler work."""

from __future__ import annotations

from datetime import datetime
from html.parser import HTMLParser
from urllib.parse import parse_qsl, urlsplit, urlunsplit, urlencode

from .models import DetailItem, ListItem


TRACKING_PREFIXES = ("utm_",)
DROP_QUERY_KEYS = {"fbclid", "gclid"}


class _AnchorParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.items: list[ListItem] = []
        self._href: str | None = None
        self._text_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "a":
            attr_map = dict(attrs)
            self._href = attr_map.get("href")
            self._text_parts = []

    def handle_data(self, data: str) -> None:
        if self._href is not None:
            self._text_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag == "a" and self._href:
            title = clean_text(" ".join(self._text_parts))
            if title:
                self.items.append(ListItem(title=title, url=self._href))
            self._href = None
            self._text_parts = []


class _DetailParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.in_title = False
        self.in_time = False
        self.title_parts: list[str] = []
        self.time_parts: list[str] = []
        self.body_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_map = {key: (value or "") for key, value in attrs}
        classes = attr_map.get("class", "")
        tag_id = attr_map.get("id", "")
        if tag in {"title", "h1"}:
            self.in_title = True
        if "time" in classes or tag == "time":
            self.in_time = True
        if tag in {"article", "main", "p", "div", "section"}:
            if any(token in classes for token in ("content", "article", "post", "entry")) or tag_id in {
                "content",
                "article",
                "main",
            }:
                self.body_parts.append(" ")

    def handle_data(self, data: str) -> None:
        if self.in_title:
            self.title_parts.append(data)
        if self.in_time:
            self.time_parts.append(data)
        self.body_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag in {"title", "h1"}:
            self.in_title = False
        if tag == "time":
            self.in_time = False


def clean_text(text: str) -> str:
    return " ".join(text.replace("\xa0", " ").split())


def parse_list_html(html: str) -> list[ListItem]:
    parser = _AnchorParser()
    parser.feed(html)
    return parser.items


def parse_detail_html(html: str) -> DetailItem:
    parser = _DetailParser()
    parser.feed(html)
    title = clean_text(" ".join(parser.title_parts)) or None
    published_text = clean_text(" ".join(parser.time_parts)) or None
    content = clean_text(" ".join(parser.body_parts))
    return DetailItem(title=title, published_text=published_text, content=content)


def parse_datetime_text(value: str) -> datetime | None:
    normalized = clean_text(value)
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%B %d, %Y %I:%M %p",
        "%b %d, %Y %I:%M %p",
        "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(normalized, fmt)
        except ValueError:
            continue
    return None


def normalize_url(url: str) -> str:
    split = urlsplit(url.strip())
    query = [
        (key, value)
        for key, value in parse_qsl(split.query, keep_blank_values=True)
        if key not in DROP_QUERY_KEYS and not key.startswith(TRACKING_PREFIXES)
    ]
    path = split.path.rstrip("/") or "/"
    netloc = split.netloc.lower()
    return urlunsplit(("", netloc, path, urlencode(query), ""))


def make_dedupe_key(url: str) -> str:
    return normalize_url(url)
