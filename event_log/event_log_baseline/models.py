"""Shared models for the local crawler baseline."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class FetchResult:
    url: str
    status_code: int
    content: str
    headers: dict[str, str] = field(default_factory=dict)
    error: str | None = None


@dataclass(slots=True)
class ListItem:
    title: str
    url: str
    excerpt: str | None = None


@dataclass(slots=True)
class DetailItem:
    title: str | None
    published_text: str | None
    content: str
