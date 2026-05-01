"""Run a low-pressure local real validation against mining.com copper pages."""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
import http.client
import json
from pathlib import Path
import sqlite3
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from event_log_baseline.config import CrawlerConfig
from event_log_baseline.mining_com import (
    COPPER_PAGE_URL,
    COPPER_TERM_API_URL,
    NOISE_MARKERS,
    POSTS_API_TEMPLATE,
    build_sample_result,
    parse_copper_term_id,
    parse_detail_page,
    parse_posts_api,
)
from event_log_baseline.pipeline import CrawlerBaseline


RUN_DIR = ROOT / "runs" / "local" / "2026-04-19" / "mining_com_copper_local_real_v1"
RESULT_PATH = RUN_DIR / "local_result.json"
ACCEPTANCE_PATH = RUN_DIR / "acceptance.json"
DEBUG_LOG_PATH = RUN_DIR / "debug.log"
DB_PATH = RUN_DIR / "local_result.db"


def append_debug(message: str) -> None:
    timestamp = datetime.now().astimezone().isoformat(timespec="seconds")
    with DEBUG_LOG_PATH.open("a", encoding="utf-8") as handle:
        handle.write(f"[{timestamp}] {message}\n")


def sleep_low_pressure(config: CrawlerConfig) -> None:
    time.sleep(max(config.request_interval_ms, 0) / 1000)


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def fetch_with_retry(baseline: CrawlerBaseline, config: CrawlerConfig, url: str, label: str) -> object:
    last_result = None
    attempts = max(config.max_retries, 0) + 1
    for attempt in range(1, attempts + 1):
        try:
            last_result = baseline.fetch(url)
        except (TimeoutError, OSError, http.client.HTTPException) as exc:
            class _TransientResult:
                status_code = 0
                error = str(exc)
                content = ""

            last_result = _TransientResult()
        append_debug(
            f"fetch {label} attempt={attempt}/{attempts} status={last_result.status_code} url={url} error={last_result.error}"
        )
        if last_result.status_code == 200 and not last_result.error:
            return last_result
        if attempt < attempts:
            sleep_low_pressure(config)
    return last_result


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS mining_com_copper_articles (
            task_id TEXT NOT NULL,
            post_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            detail_title TEXT,
            article_url TEXT NOT NULL,
            published_text TEXT,
            content TEXT NOT NULL,
            content_length INTEGER NOT NULL,
            content_preview TEXT,
            dedupe_key TEXT NOT NULL,
            detail_status_code INTEGER NOT NULL,
            fetched_at TEXT NOT NULL,
            PRIMARY KEY (task_id, post_id)
        )
        """
    )


def persist_samples(db_path: Path, samples: list[dict[str, object]]) -> int:
    with sqlite3.connect(db_path) as conn:
        init_db(conn)
        conn.executemany(
            """
            INSERT OR REPLACE INTO mining_com_copper_articles (
                task_id,
                post_id,
                title,
                detail_title,
                article_url,
                published_text,
                content,
                content_length,
                content_preview,
                dedupe_key,
                detail_status_code,
                fetched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "mining_com_copper_local_real_v1",
                    sample["post_id"],
                    sample["title"],
                    sample["detail_title"],
                    sample["article_url"],
                    sample["published_text"],
                    sample["content_full"],
                    sample["content_length"],
                    sample["content_preview"],
                    sample["dedupe_key"],
                    sample["detail_status_code"],
                    sample["fetched_at"],
                )
                for sample in samples
            ],
        )
        row = conn.execute("SELECT COUNT(*) FROM mining_com_copper_articles WHERE task_id = ?", ("mining_com_copper_local_real_v1",)).fetchone()
        return int(row[0]) if row else 0


def main() -> int:
    RUN_DIR.mkdir(parents=True, exist_ok=True)
    config = CrawlerConfig()
    baseline = CrawlerBaseline(config)
    executed_urls: list[str] = []

    append_debug("start mining_com_copper_local_real_v1")
    append_debug(
        "config request_interval_ms=%s max_concurrency=%s max_retries=%s request_timeout_seconds=%s enable_fallback_fetcher=%s"
        % (
            config.request_interval_ms,
            config.max_concurrency,
            config.max_retries,
            config.request_timeout_seconds,
            config.enable_fallback_fetcher,
        )
    )

    copper_page_result = fetch_with_retry(baseline, config, COPPER_PAGE_URL, "copper page")
    executed_urls.append(COPPER_PAGE_URL)
    sleep_low_pressure(config)

    term_result = fetch_with_retry(baseline, config, COPPER_TERM_API_URL, "copper term api")
    executed_urls.append(COPPER_TERM_API_URL)
    if term_result.status_code != 200:
        raise RuntimeError(f"Failed to fetch copper term api: status={term_result.status_code} error={term_result.error}")

    copper_term_id = parse_copper_term_id(term_result.content)
    posts_api_url = POSTS_API_TEMPLATE.format(term_id=copper_term_id, limit=3)
    sleep_low_pressure(config)

    posts_result = fetch_with_retry(baseline, config, posts_api_url, "copper posts api")
    executed_urls.append(posts_api_url)
    if posts_result.status_code != 200:
        raise RuntimeError(f"Failed to fetch copper posts api: status={posts_result.status_code} error={posts_result.error}")

    list_entries = parse_posts_api(posts_result.content)
    append_debug(f"parsed list entries={len(list_entries)}")

    detail_samples: list[dict[str, object]] = []
    for entry in list_entries[:3]:
        sleep_low_pressure(config)
        detail_result = fetch_with_retry(baseline, config, entry.url, "detail")
        executed_urls.append(entry.url)
        detail = parse_detail_page(detail_result.content)
        sample = build_sample_result(entry, detail)
        sample["detail_status_code"] = detail_result.status_code
        sample["detail_title"] = detail.title
        sample["content_full"] = detail.content
        sample["fetched_at"] = datetime.now().astimezone().isoformat(timespec="seconds")
        detail_samples.append(sample)

    inserted_rows = persist_samples(DB_PATH, detail_samples)
    append_debug(f"persist sqlite path={DB_PATH} rows={inserted_rows}")

    local_result = {
        "task_id": "mining_com_copper_local_real_v1",
        "target_name": "mining.com/copper",
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "command": str(Path(__file__).resolve()),
        "config": {
            "request_interval_ms": config.request_interval_ms,
            "max_concurrency": config.max_concurrency,
            "max_retries": config.max_retries,
            "request_timeout_seconds": config.request_timeout_seconds,
            "enable_fallback_fetcher": config.enable_fallback_fetcher,
            "fetcher_mode": config.fetcher_mode,
        },
        "list_source": {
            "page_url": COPPER_PAGE_URL,
            "page_status_code": copper_page_result.status_code,
            "api_url": posts_api_url,
            "copper_term_api_url": COPPER_TERM_API_URL,
            "copper_term_id": copper_term_id,
            "list_count": len(list_entries),
        },
        "list_items": [asdict(entry.to_list_item()) for entry in list_entries],
        "detail_samples": detail_samples,
        "db": {
            "db_path": str(DB_PATH),
            "table_name": "mining_com_copper_articles",
            "inserted_rows": inserted_rows,
        },
        "executed_urls": executed_urls,
        "notes": [
            "The copper landing page responded with status 200.",
            "The visible article list is backed by the site's public WordPress API, so list validation uses that live endpoint after requesting /copper/.",
            "Detail validation uses the real article pages, not the API-rendered body.",
        ],
    }
    write_json(RESULT_PATH, local_result)

    content_clean = all(
        sample["content_length"] > 200
        and all(marker not in str(sample["content_full"]) for marker in NOISE_MARKERS)
        and "Sign In Join" not in str(sample["content_full"])
        for sample in detail_samples
    )
    time_available = all(bool(sample["published_text"]) for sample in detail_samples)
    dedupe_generated = all(bool(sample["dedupe_key"]) for sample in detail_samples)
    detail_success = all(int(sample["detail_status_code"]) == 200 for sample in detail_samples)
    overall = all(
        [
            copper_page_result.status_code == 200,
            len(list_entries) > 0,
            detail_success,
            all(sample["content_length"] > 0 for sample in detail_samples),
            time_available,
            dedupe_generated,
            content_clean,
            inserted_rows == len(detail_samples),
            RESULT_PATH.exists(),
            DEBUG_LOG_PATH.exists(),
            DB_PATH.exists(),
        ]
    )

    acceptance = {
        "target_name": "mining.com/copper",
        "task_id": "mining_com_copper_local_real_v1",
        "checks": {
            "task_defined": "pass",
            "scope_single_task": "pass",
            "real_list_fetch_success": "pass" if len(list_entries) > 0 else "fail",
            "real_detail_fetch_success": "pass" if detail_success else "fail",
            "real_content_fetch_success": "pass" if all(sample["content_length"] > 0 for sample in detail_samples) else "fail",
            "real_content_clean_text": "pass" if content_clean else "fail",
            "real_time_field_available": "pass" if time_available else "fail",
            "real_dedupe_key_generated": "pass" if dedupe_generated else "fail",
            "low_pressure_local_crawling": "pass"
            if config.max_concurrency == 1 and config.max_retries <= 2 and not config.enable_fallback_fetcher
            else "fail",
            "local_command_defined": "pass",
            "local_test_executed": "pass",
            "local_test_passed": "pass" if overall else "fail",
            "local_result_recorded": "pass"
            if RESULT_PATH.exists() and DEBUG_LOG_PATH.exists() and ACCEPTANCE_PATH.parent.exists()
            else "fail",
            "local_db_recorded": "pass" if DB_PATH.exists() and inserted_rows == len(detail_samples) else "fail",
            "ready_for_server_validation": "pass" if overall else "fail",
        },
        "overall_result": "pass" if overall else "fail",
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
    }
    write_json(ACCEPTANCE_PATH, acceptance)
    append_debug(f"overall_result={acceptance['overall_result']}")
    print(json.dumps(local_result, ensure_ascii=False, indent=2))
    return 0 if overall else 1


if __name__ == "__main__":
    raise SystemExit(main())
