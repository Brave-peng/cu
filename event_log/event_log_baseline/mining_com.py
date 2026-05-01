"""Source-specific helpers for mining.com local real validation."""

from __future__ import annotations

from dataclasses import dataclass
from html import unescape
from html.parser import HTMLParser
import json
import re

from .models import DetailItem, ListItem
from .parsers import clean_text, make_dedupe_key


COPPER_PAGE_URL = "https://www.mining.com/copper/"
COPPER_TERM_API_URL = "https://www.mining.com/wp-json/wp/v2/commodity?search=copper&per_page=10"
POSTS_API_TEMPLATE = (
    "https://www.mining.com/wp-json/wp/v2/posts?commodity={term_id}&per_page={limit}"
    "&_fields=id,date,date_gmt,link,title,excerpt"
)
NOISE_MARKERS = (
    "Share Comments",
    "Cancel reply",
    "No comments found.",
    "More News",
    "MINING.COM TV",
    "Load more news >",
    "More videos >",
    "Policies & Terms",
    "About Us ©",
)


@dataclass(slots=True)
class MiningComListEntry:
    post_id: int
    title: str
    url: str
    published_text: str | None
    excerpt: str | None

    def to_list_item(self) -> ListItem:
        return ListItem(title=self.title, url=self.url, excerpt=self.excerpt)


class _MiningComDetailParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._capture_title = False
        self._capture_meta = False
        self._content_depth = 0
        self._skip_depth = 0
        self.title_parts: list[str] = []
        self.meta_parts: list[str] = []
        self.body_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_map = {key: (value or "") for key, value in attrs}
        classes = attr_map.get("class", "")

        if tag in {"script", "style"}:
            self._skip_depth += 1
            return

        if tag == "h1" and "single-title" in classes:
            self._capture_title = True

        if tag == "div" and "post-meta" in classes:
            self._capture_meta = True

        if tag == "div" and "content" in classes.split():
            self._content_depth = 1
            return

        if self._content_depth:
            self._content_depth += 1
            if tag in {"p", "li", "h2", "h3", "blockquote"}:
                self.body_parts.append(" ")

    def handle_endtag(self, tag: str) -> None:
        if tag in {"script", "style"} and self._skip_depth:
            self._skip_depth -= 1
            return

        if tag == "h1":
            self._capture_title = False

        if tag == "div" and self._capture_meta:
            self._capture_meta = False

        if self._content_depth:
            self._content_depth -= 1

    def handle_data(self, data: str) -> None:
        if self._skip_depth:
            return

        if self._capture_title:
            self.title_parts.append(data)

        if self._capture_meta:
            self.meta_parts.append(data)

        if self._content_depth:
            self.body_parts.append(data)


def parse_copper_term_id(api_payload: str) -> int:
    data = json.loads(api_payload)
    for item in data:
        if item.get("slug") == "copper":
            return int(item["id"])
    raise ValueError("Copper commodity term not found in mining.com API response.")


def parse_posts_api(api_payload: str) -> list[MiningComListEntry]:
    data = json.loads(api_payload)
    items: list[MiningComListEntry] = []
    for item in data:
        title = clean_text(unescape(item["title"]["rendered"]))
        excerpt = clean_text(strip_html_tags(unescape(item.get("excerpt", {}).get("rendered", "")))) or None
        items.append(
            MiningComListEntry(
                post_id=int(item["id"]),
                title=title,
                url=item["link"],
                published_text=item.get("date"),
                excerpt=excerpt,
            )
        )
    return items


def parse_detail_page(html: str) -> DetailItem:
    parser = _MiningComDetailParser()
    parser.feed(html)
    title = clean_text(" ".join(parser.title_parts)) or None
    content = trim_noise(clean_text(" ".join(parser.body_parts)))
    published_text = extract_published_text(html, clean_text(" ".join(parser.meta_parts)) or None)
    return DetailItem(title=title, published_text=published_text, content=content)


def extract_published_text(html: str, meta_text: str | None = None) -> str | None:
    meta_match = re.search(
        r'<meta[^>]+property=["\']article:published_time["\'][^>]+content=["\']([^"\']+)["\']',
        html,
        flags=re.IGNORECASE,
    )
    if meta_match:
        return clean_text(meta_match.group(1))

    if meta_text:
        match = re.search(
            r"([A-Z][a-z]+ \d{1,2}, \d{4}\s*\|\s*\d{1,2}:\d{2}\s*[ap]m)",
            meta_text,
            flags=re.IGNORECASE,
        )
        if match:
            return clean_text(match.group(1))
    return None


def strip_html_tags(value: str) -> str:
    return re.sub(r"<[^>]+>", " ", value)


def trim_noise(content: str) -> str:
    trimmed = content
    cut_positions = [trimmed.find(marker) for marker in NOISE_MARKERS if marker in trimmed]
    cut_positions = [pos for pos in cut_positions if pos >= 0]
    if cut_positions:
        trimmed = trimmed[: min(cut_positions)]
    return clean_text(trimmed)


def build_sample_result(entry: MiningComListEntry, detail: DetailItem) -> dict[str, object]:
    return {
        "post_id": entry.post_id,
        "title": entry.title,
        "article_url": entry.url,
        "published_text": detail.published_text or entry.published_text,
        "content_length": len(detail.content),
        "content_preview": detail.content[:280],
        "dedupe_key": make_dedupe_key(entry.url),
    }
