"""SQLModel table definitions for market daily data."""

from __future__ import annotations

from datetime import date, datetime

from sqlmodel import Field, SQLModel


class MarketDailyBase(SQLModel):
    """Shared fields for `market_daily` records."""

    date: date = Field(description="Trade date.")
    symbol: str = Field(max_length=16, description="Futures symbol, stage 1 defaults to CU.")
    contract: str = Field(max_length=32, description="Exchange contract code, for example CU2505.")
    open: float = Field(description="Open price.")
    high: float = Field(description="High price.")
    low: float = Field(description="Low price.")
    close: float = Field(description="Close price.")
    settlement: float = Field(description="Settlement price.")
    volume: float = Field(ge=0, description="Trading volume.")
    open_interest: float = Field(ge=0, description="Open interest.")
    source: str = Field(max_length=32, description="Data source, for example SHFE.")
    fetched_at: datetime = Field(description="Ingestion timestamp.")
    note: str | None = Field(default=None, description="Optional note for missing fields or anomalies.")


class MarketDaily(MarketDailyBase, table=True):
    """`market_daily` table model.

    Stage 1 uses `(date, symbol, contract)` as the composite primary key.
    """

    __tablename__ = "market_daily"

    date: date = Field(primary_key=True)
    symbol: str = Field(primary_key=True, max_length=16)
    contract: str = Field(primary_key=True, max_length=32)
