"""Backfill failed or empty Splash247 Dry Cargo stage rows and refresh acceptance."""

from __future__ import annotations

import os
from pathlib import Path
import sqlite3
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.environ.setdefault("RUN_DATE", "2026-04-20")
os.environ.setdefault(
    "RUN_DIR",
    "/data/event_log/runs/2026-04-20/splash247_dry_cargo_full_stage_relay_v1",
)

from scripts import run_splash247_dry_cargo_full_stage_server_relay as job


def fetch_failed_rows() -> list[dict[str, str]]:
    with sqlite3.connect(job.STAGE_DB_PATH, timeout=60) as conn:
        job.init_db(conn)
        rows = conn.execute(
            """
            SELECT article_url, title, COALESCE(detail_title, ''), COALESCE(published_text, '')
            FROM event_log_articles
            WHERE source_name = ?
              AND ((http_status != 200 OR http_status IS NULL) OR length(trim(coalesce(content, ''))) = 0)
            ORDER BY article_url
            """,
            (job.SOURCE_NAME,),
        ).fetchall()
    return [
        {
            "article_url": row[0],
            "title": row[1],
            "detail_title": row[2],
            "published_text": row[3],
        }
        for row in rows
    ]


def recalc_state(state: dict[str, object]) -> dict[str, object]:
    with sqlite3.connect(job.STAGE_DB_PATH, timeout=60) as conn:
        job.init_db(conn)
        row = conn.execute(
            """
            SELECT
              COUNT(1),
              SUM(CASE WHEN http_status = 200 THEN 1 ELSE 0 END),
              SUM(CASE WHEN http_status != 200 OR http_status IS NULL THEN 1 ELSE 0 END),
              SUM(CASE WHEN length(trim(coalesce(content, ''))) > 0 THEN 1 ELSE 0 END),
              SUM(CASE WHEN length(trim(coalesce(content, ''))) = 0 THEN 1 ELSE 0 END)
            FROM event_log_articles
            WHERE source_name = ?
            """,
            (job.SOURCE_NAME,),
        ).fetchone()
    total, detail_ok, detail_fail, content_ok, content_fail = row
    state["deduped_count"] = int(total or 0)
    state["detail_success_count"] = int(detail_ok or 0)
    state["detail_failed_count"] = int(detail_fail or 0)
    state["content_success_count"] = int(content_ok or 0)
    state["content_failed_count"] = int(content_fail or 0)
    return state


def main() -> int:
    config = job.CrawlerConfig.from_env()
    relay_enabled = bool(os.getenv("HTTP_PROXY") or os.getenv("HTTPS_PROXY"))
    state = job.load_state(main_db_before=job.db_count(job.MAIN_DB_PATH, job.SOURCE_NAME))
    failed_rows = fetch_failed_rows()
    job.append_debug(f"backfill_start failures={len(failed_rows)}")

    repaired = 0
    for row in failed_rows:
        job.sleep_low_pressure(config)
        result = job.fetch_with_retry(config, row["article_url"], "backfill detail")
        if result.status_code != 200:
            continue
        detail = job.parse_detail(result.content)
        content_full = detail["content_full"]
        sample = {
            "title": row["title"],
            "detail_title": detail["detail_title"] or row["detail_title"] or row["title"],
            "article_url": row["article_url"],
            "published_text": detail["detail_published_text"] or row["published_text"],
            "content_length": len(content_full),
            "content_preview": content_full[:280],
            "content_full": content_full,
            "dedupe_key": job.normalize_dedupe_key(row["article_url"]),
            "detail_status_code": result.status_code,
            "fetched_at": job.now_iso(),
            "fetch_error": None,
        }
        job.upsert_article(job.STAGE_DB_PATH, sample)
        repaired += 1
        job.append_debug(
            f"backfill_repaired url={row['article_url']} content_length={sample['content_length']}"
        )

    state = recalc_state(state)
    job.save_state(state)
    job.update_result(state, config, relay_enabled, "completed")
    job.write_acceptance(state, config, relay_enabled, "completed")
    job.append_debug(
        f"backfill_done repaired={repaired} detail_failed_count={state['detail_failed_count']} content_failed_count={state['content_failed_count']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
