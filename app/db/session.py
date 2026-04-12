"""SQLModel 数据库连接与初始化。"""

from __future__ import annotations

from pathlib import Path

from sqlalchemy.engine import Engine
from sqlmodel import SQLModel, create_engine

from app.models import MarketDaily

DEFAULT_DATABASE_URL = "sqlite:///data/cu.db"


def _ensure_sqlite_parent(database_url: str) -> None:
    """SQLite 文件库需要先创建父目录。"""

    if not database_url.startswith("sqlite:///") or database_url == "sqlite:///:memory:":
        return

    raw_path = database_url.removeprefix("sqlite:///")
    if not raw_path:
        return

    path = Path(raw_path)
    if path.parent != Path("."):
        path.parent.mkdir(parents=True, exist_ok=True)


def create_db_engine(database_url: str = DEFAULT_DATABASE_URL) -> Engine:
    """创建数据库 engine。"""

    _ensure_sqlite_parent(database_url)
    return create_engine(database_url)


def init_db(engine: Engine) -> None:
    """创建当前阶段需要的表。"""

    # 保证模型已导入并注册到 SQLModel.metadata。
    _ = MarketDaily
    SQLModel.metadata.create_all(engine)
