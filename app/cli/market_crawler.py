"""市场日线抓取命令。

当前文件只做三件事：
1. 从 AKShare 抓取上期所日线数据。
2. 过滤并规范化为 `market_daily` 结构。
3. 可选写入本地数据库并打印样例。
"""

from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any

import akshare as ak
import typer

from app.db import DEFAULT_DATABASE_URL
from app.services import save_market_daily_rows

app = typer.Typer(help="抓取 SHFE 日线数据并保存到数据库。")

DEFAULT_MARKET = "SHFE"
DEFAULT_SYMBOL = "CU"


class MarketCrawlerError(RuntimeError):
    """抓取或解析市场数据时抛出的错误。"""


def _pick_first(row: dict[str, Any], *keys: str) -> Any:
    """按字段名顺序取值，忽略大小写。"""

    lowered = {str(key).strip().lower(): value for key, value in row.items()}
    for key in keys:
        value = lowered.get(key.lower())
        if value not in (None, ""):
            return value
    return None


def crawl_market_daily(
    start_date: str,
    end_date: str | None = None,
    symbol: str = DEFAULT_SYMBOL,
) -> list[dict[str, Any]]:
    """抓取并规范化一个品种的 market_daily 数据。
    逻辑：
    1. 根据start_date和end_date，计算出需要抓取的日期范围。
    2. 根据日期范围，抓取数据。
    3. 将抓取的数据转换为market_daily的格式。
    4. 返回market_daily的格式。
    """

    try:
        start = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date) if end_date else start
    except ValueError as exc:
        raise MarketCrawlerError("start_date 和 end_date 必须是 YYYY-MM-DD 格式") from exc

    if start > end:
        raise MarketCrawlerError("start_date 不能晚于 end_date")

    try:
        frame = ak.get_futures_daily(
            start_date=start.strftime("%Y%m%d"),
            end_date=end.strftime("%Y%m%d"),
            market=DEFAULT_MARKET,
        )
    except Exception as exc:  # pragma: no cover
        raise MarketCrawlerError(f"抓取 {DEFAULT_MARKET} 日线失败: {exc}") from exc

    if frame is None:
        return []
    if not hasattr(frame, "to_dict"):
        raise MarketCrawlerError(f"AKShare 返回了不支持的数据类型: {type(frame)!r}")

    fetched_at = datetime.now().astimezone().isoformat(timespec="seconds")
    rows = frame.to_dict(orient="records")
    symbol = symbol.upper()
    result: list[dict[str, Any]] = []

    for row in rows:
        contract = str(
            _pick_first(row, "symbol", "contract", "contract_code", "合约", "合约代码") or ""
        ).strip().upper()
        if not contract.startswith(symbol):
            continue

        trade_date = _pick_first(row, "date", "trade_date", "日期", "交易日期")
        if isinstance(trade_date, datetime):
            trade_date = trade_date.date().isoformat()
        elif isinstance(trade_date, date):
            trade_date = trade_date.isoformat()
        else:
            trade_date = str(trade_date).strip()
            for fmt in ("%Y-%m-%d", "%Y%m%d", "%Y/%m/%d"):
                try:
                    trade_date = datetime.strptime(trade_date, fmt).date().isoformat()
                    break
                except ValueError:
                    continue
            else:
                raise ValueError(f"无法解析交易日期: {trade_date!r}")

        settlement = _pick_first(row, "settlement", "结算价") or _pick_first(
            row, "close", "收盘价"
        )

        result.append(
            {
                "date": trade_date,
                "symbol": symbol,
                "contract": contract if contract.startswith(symbol) else f"{symbol}{contract}",
                "open": float(_pick_first(row, "open", "开盘价")),
                "high": float(_pick_first(row, "high", "最高价")),
                "low": float(_pick_first(row, "low", "最低价")),
                "close": float(_pick_first(row, "close", "收盘价")),
                "settlement": float(settlement),
                "volume": float(_pick_first(row, "volume", "成交量")),
                "open_interest": float(_pick_first(row, "hold", "open_interest", "持仓量")),
                "source": DEFAULT_MARKET,
                "fetched_at": fetched_at,
            }
        )

    return result


@app.command()
def main(
    start_date: str = typer.Option(..., help="起始交易日，例如：2026-03-28"),
    end_date: str | None = typer.Option(None, help="结束交易日，默认等于 start_date"),
    symbol: str = typer.Option(DEFAULT_SYMBOL, help="品种代码，阶段一默认 CU"),
    save: bool = typer.Option(True, "--save/--no-save", help="是否保存到数据库"),
    db_url: str = typer.Option(DEFAULT_DATABASE_URL, help="SQLAlchemy 数据库 URL"),
    limit: int = typer.Option(20, min=1, help="最多打印多少条记录"),
    pretty: bool = typer.Option(True, help="是否格式化 JSON 输出"),
) -> None:
    """CLI 入口。"""

    try:
        rows = crawl_market_daily(start_date=start_date, end_date=end_date, symbol=symbol)
    except (MarketCrawlerError, ValueError, TypeError) as exc:
        typer.echo(f"market crawler failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    typer.echo(f"rows: {len(rows)}")
    if save:
        saved_count = save_market_daily_rows(rows, database_url=db_url)
        typer.echo(f"saved: {saved_count}")

    for row in rows[:limit]:
        typer.echo(json.dumps(row, ensure_ascii=False, indent=2 if pretty else None))


if __name__ == "__main__":
    app()
