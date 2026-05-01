"""Fetcher implementations for the crawler baseline."""

from .base import BaseFetcher
from .browser import BrowserFetcher
from .default import DefaultFetcher
from .factory import build_fetcher
from .proxy import ProxyFetcher

__all__ = ["BaseFetcher", "BrowserFetcher", "DefaultFetcher", "ProxyFetcher", "build_fetcher"]
