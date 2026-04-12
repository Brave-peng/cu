"""market_daily 入库服务。"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from sqlmodel import Session

from app.db import DEFAULT_DATABASE_URL, create_db_engine, init_db
from app.models import MarketDaily


def save_market_daily_rows(
    rows: Iterable[dict[str, Any]],
    database_url: str = DEFAULT_DATABASE_URL,
) -> int:
    """按主键 upsert market_daily 行并返回处理条数。"""

    materialized_rows = list(rows)
    if not materialized_rows:
        return 0

    engine = create_db_engine(database_url)
    init_db(engine)

    with Session(engine) as session:
        for row in materialized_rows:
            session.merge(MarketDaily.model_validate(row))
        session.commit()

    return len(materialized_rows)
