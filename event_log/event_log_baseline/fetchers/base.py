"""Fetcher interface."""

from __future__ import annotations

from abc import ABC, abstractmethod

from event_log_baseline.models import FetchResult


class BaseFetcher(ABC):
    mode: str

    @abstractmethod
    def fetch(self, url: str) -> FetchResult:
        raise NotImplementedError
