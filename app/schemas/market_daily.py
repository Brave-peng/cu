"""Schemas for market daily create/read/query flows."""

from __future__ import annotations

from datetime import date as Date
from datetime import datetime as DateTime

from sqlmodel import Field, SQLModel


class MarketDailyBase(SQLModel):
    """Shared schema fields for market daily payloads."""

    date: Date
    symbol: str = Field(max_length=16)
    contract: str = Field(max_length=32)
    open: float
    high: float
    low: float
    close: float
    settlement: float
    volume: float = Field(ge=0)
    open_interest: float = Field(ge=0)
    source: str = Field(max_length=32)
    fetched_at: DateTime
    note: str | None = None


class MarketDailyCreate(MarketDailyBase):
    """Schema for creating or upserting one market daily record."""


class MarketDailyRead(MarketDailyBase):
    """Schema returned by read endpoints and query services."""


class MarketDailyFilter(SQLModel):
    """Query parameters for market daily lookups."""

    start_date: Date | None = Field(default=None, description="Inclusive start date.")
    end_date: Date | None = Field(default=None, description="Inclusive end date.")
    symbol: str | None = Field(default=None, max_length=16)
    contract: str | None = Field(default=None, max_length=32)
    limit: int = Field(default=100, ge=1, le=1000)
