"""Reusable local crawler baseline for event_log source work."""

from .config import CrawlerConfig
from .pipeline import CrawlerBaseline

__all__ = ["CrawlerBaseline", "CrawlerConfig"]
