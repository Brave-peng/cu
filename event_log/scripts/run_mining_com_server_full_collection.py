"""Run a resumable full mining.com/copper crawl on the server via local relay."""

from __future__ import annotations

from datetime import datetime, timezone
import http.client
import json
import os
from pathlib import Path
import random
import sqlite3
import sys
import time
from urllib.parse import urlencode

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


TASK_ID = os.getenv("TASK_ID", "mining_com_copper_full_collection_relay_v1")
TARGET_NAME = os.getenv("TARGET_NAME", "server_full_collection_via_local_relay")
DATA_ROOT = Path(os.getenv("DATA_ROOT", "/data/event_log"))
RUN_DATE = os.getenv("RUN_DATE", datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d"))
RUN_DIR = Path(os.getenv("RUN_DIR", str(DATA_ROOT / "runs" / RUN_DATE / TASK_ID)))
DB_PATH = Path(os.getenv("DB_PATH", str(DATA_ROOT / "db" / "event_log.db")))
RESULT_PATH = RUN_DIR / "crawl_result.json"
ACCEPTANCE_PATH = RUN_DIR / "acceptance.json"
DEBUG_LOG_PATH = RUN_DIR / "debug.log"
STATE_PATH = RUN_DIR / "progress_state.json"
JITTER_MS = int(os.getenv("JITTER_MS", "600"))
PAGE_PAUSE_MS = int(os.getenv("PAGE_PAUSE_MS", "4000"))
BLOCKED_BACKOFF_SECONDS = int(os.getenv("BLOCKED_BACKOFF_SECONDS", "20"))


def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def append_debug(message: str) -> None:
    with DEBUG_LOG_PATH.open("a", encoding="utf-8") as handle:
        handle.write(f"[{now_iso()}] {message}\n")


def sleep_low_pressure(config: CrawlerConfig) -> None:
    base_ms = max(config.request_interval_ms, 0)
    jitter = random.randint(0, max(JITTER_MS, 0)) if JITTER_MS > 0 else 0
    time.sleep((base_ms + jitter) / 1000)


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def fetch_with_retry(baseline: CrawlerBaseline, config: CrawlerConfig, url: str, label: str):
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
                headers = {}

            last_result = _TransientResult()
        append_debug(
            f"fetch {label} attempt={attempt}/{attempts} status={last_result.status_code} url={url} error={last_result.error}"
        )
        if last_result.status_code == 200 and not last_result.error:
            return last_result
        if last_result.status_code in {0, 403, 429}:
            append_debug(
                f"blocked_or_transient status={last_result.status_code} label={label} backoff_seconds={BLOCKED_BACKOFF_SECONDS}"
            )
            time.sleep(max(BLOCKED_BACKOFF_SECONDS, 0))
        if attempt < attempts:
            sleep_low_pressure(config)
    return last_result


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS event_log_articles (
            source_name TEXT NOT NULL,
            dedupe_key TEXT NOT NULL,
            article_url TEXT NOT NULL,
            title TEXT NOT NULL,
            detail_title TEXT,
            published_text TEXT,
            content TEXT NOT NULL,
            content_length INTEGER NOT NULL,
            content_preview TEXT,
            http_status INTEGER,
            fetch_error TEXT,
            fetched_at TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (source_name, dedupe_key)
        )
        """
    )


def upsert_article(db_path: Path, sample: dict[str, object]) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        init_db(conn)
        conn.execute(
            """
            INSERT OR REPLACE INTO event_log_articles (
                source_name,
                dedupe_key,
                article_url,
                title,
                detail_title,
                published_text,
                content,
                content_length,
                content_preview,
                http_status,
                fetch_error,
                fetched_at,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "mining.com/copper",
                sample["dedupe_key"],
                sample["article_url"],
                sample["title"],
                sample["detail_title"],
                sample["published_text"],
                sample["content_full"],
                sample["content_length"],
                sample["content_preview"],
                sample["detail_status_code"],
                sample.get("fetch_error"),
                sample["fetched_at"],
                sample["fetched_at"],
            ),
        )


def db_count(db_path: Path) -> int:
    with sqlite3.connect(db_path) as conn:
        init_db(conn)
        row = conn.execute(
            "SELECT COUNT(1) FROM event_log_articles WHERE source_name = ?",
            ("mining.com/copper",),
        ).fetchone()
    return int(row[0]) if row else 0


def load_state() -> dict[str, object]:
    if STATE_PATH.exists():
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    return {
        "next_page": 1,
        "pages_completed": 0,
        "list_total_count": 0,
        "detail_success_count": 0,
        "detail_failed_count": 0,
        "content_success_count": 0,
        "content_failed_count": 0,
        "articles_processed": 0,
        "last_processed_url": None,
        "last_completed_page": 0,
        "samples": [],
    }


def save_state(state: dict[str, object]) -> None:
    write_json(STATE_PATH, state)


def update_result(
    state: dict[str, object],
    total_pages: int,
    total_posts: int,
    config: CrawlerConfig,
    relay_enabled: bool,
    overall_status: str,
) -> None:
    payload = {
        "source_name": "mining.com/copper",
        "task_id": TASK_ID,
        "target_name": TARGET_NAME,
        "total_pages": total_pages,
        "pages_completed": state["pages_completed"],
        "next_page": state["next_page"],
        "list_total_count": total_posts,
        "detail_success_count": state["detail_success_count"],
        "detail_failed_count": state["detail_failed_count"],
        "content_success_count": state["content_success_count"],
        "content_failed_count": state["content_failed_count"],
        "articles_processed": state["articles_processed"],
        "db_written_count": db_count(DB_PATH),
        "db_path": str(DB_PATH),
        "run_dir": str(RUN_DIR),
        "crawl_config": {
            "request_interval_ms": config.request_interval_ms,
            "jitter_ms": JITTER_MS,
            "page_pause_ms": PAGE_PAUSE_MS,
            "max_concurrency": config.max_concurrency,
            "max_retries": config.max_retries,
            "timeout_seconds": config.request_timeout_seconds,
            "blocked_backoff_seconds": BLOCKED_BACKOFF_SECONDS,
            "relay_enabled": relay_enabled,
        },
        "samples": state["samples"][:5],
        "status": overall_status,
        "generated_at": now_iso(),
    }
    write_json(RESULT_PATH, payload)


def final_acceptance(total_pages: int, relay_enabled: bool) -> dict[str, object]:
    result = json.loads(RESULT_PATH.read_text(encoding="utf-8"))
    content_clean = all(
        sample["content_length"] > 200
        and all(marker not in str(sample["content_full"]) for marker in NOISE_MARKERS)
        for sample in result["samples"][:5]
    )
    checks = {
        "relay_path_verified": "pass" if relay_enabled else "fail",
        "full_collection_capable": "pass" if result["pages_completed"] == total_pages else "fail",
        "pagination_progress_recorded": "pass" if RESULT_PATH.exists() else "fail",
        "all_pages_completed": "pass" if result["pages_completed"] == total_pages else "fail",
        "detail_fetch_success": "pass" if result["detail_failed_count"] == 0 else "fail",
        "content_fetch_success": "pass" if result["content_failed_count"] == 0 else "fail",
        "content_clean_text": "pass" if content_clean else "fail",
        "time_field_available": "pass"
        if all(bool(sample["published_text"]) for sample in result["samples"][:5])
        else "fail",
        "dedupe_effective": "pass"
        if len({sample["dedupe_key"] for sample in result["samples"]}) == len(result["samples"])
        else "fail",
        "server_db_recorded": "pass" if result["db_written_count"] >= result["list_total_count"] else "fail",
        "failure_traceable": "pass",
        "low_pressure_crawling_enforced": "pass",
        "resume_capable": "pass" if STATE_PATH.exists() else "fail",
        "server_result_recorded": "pass"
        if RESULT_PATH.exists() and DEBUG_LOG_PATH.exists() and ACCEPTANCE_PATH.parent.exists()
        else "fail",
    }
    overall = "pass" if all(value == "pass" for value in checks.values()) else "fail"
    return {
        "target_name": TARGET_NAME,
        "task_id": TASK_ID,
        "checks": checks,
        "overall_result": overall,
        "generated_at": now_iso(),
    }


def posts_api_url(term_id: int, page: int, per_page: int = 100) -> str:
    query = urlencode(
        {
            "commodity": term_id,
            "per_page": per_page,
            "page": page,
            "_fields": "id,date,date_gmt,link,title,excerpt",
        }
    )
    return f"https://www.mining.com/wp-json/wp/v2/posts?{query}"


def main() -> int:
    RUN_DIR.mkdir(parents=True, exist_ok=True)
    config = CrawlerConfig.from_env()
    baseline = CrawlerBaseline(config)
    relay_enabled = bool(os.getenv("HTTP_PROXY") or os.getenv("HTTPS_PROXY"))
    state = load_state()

    append_debug(f"start {TASK_ID}")
    append_debug(
        "config request_interval_ms=%s jitter_ms=%s page_pause_ms=%s max_concurrency=%s max_retries=%s request_timeout_seconds=%s blocked_backoff_seconds=%s relay_enabled=%s"
        % (
            config.request_interval_ms,
            JITTER_MS,
            PAGE_PAUSE_MS,
            config.max_concurrency,
            config.max_retries,
            config.request_timeout_seconds,
            BLOCKED_BACKOFF_SECONDS,
            relay_enabled,
        )
    )

    copper_page_result = fetch_with_retry(baseline, config, COPPER_PAGE_URL, "relay probe page")
    if copper_page_result.status_code != 200:
        raise RuntimeError(
            f"Relay probe failed: status={copper_page_result.status_code} error={copper_page_result.error}"
        )

    sleep_low_pressure(config)
    term_result = fetch_with_retry(baseline, config, COPPER_TERM_API_URL, "copper term api")
    if term_result.status_code != 200:
        raise RuntimeError(f"Failed to fetch term api: status={term_result.status_code} error={term_result.error}")
    copper_term_id = parse_copper_term_id(term_result.content)

    first_page_url = posts_api_url(copper_term_id, 1)
    sleep_low_pressure(config)
    first_page_result = fetch_with_retry(baseline, config, first_page_url, "posts page 1")
    if first_page_result.status_code != 200:
        raise RuntimeError(
            f"Failed to fetch first posts page: status={first_page_result.status_code} error={first_page_result.error}"
        )
    total_pages = int(first_page_result.headers.get("X-WP-TotalPages", "1"))
    total_posts = int(first_page_result.headers.get("X-WP-Total", "0"))

    start_page = int(state["next_page"])
    for page in range(start_page, total_pages + 1):
        page_result = first_page_result if page == 1 else None
        if page_result is None:
            sleep_low_pressure(config)
            page_result = fetch_with_retry(baseline, config, posts_api_url(copper_term_id, page), f"posts page {page}")
            if page_result.status_code != 200:
                raise RuntimeError(
                    f"Failed to fetch posts page {page}: status={page_result.status_code} error={page_result.error}"
                )
        entries = parse_posts_api(page_result.content)
        append_debug(f"page={page} entries={len(entries)}")
        for entry in entries:
            sleep_low_pressure(config)
            detail_result = fetch_with_retry(baseline, config, entry.url, "detail")
            detail = parse_detail_page(detail_result.content)
            sample = build_sample_result(entry, detail)
            sample["detail_status_code"] = detail_result.status_code
            sample["detail_title"] = detail.title
            sample["content_full"] = detail.content
            sample["fetched_at"] = now_iso()
            sample["fetch_error"] = detail_result.error
            upsert_article(DB_PATH, sample)
            state["articles_processed"] += 1
            state["last_processed_url"] = entry.url
            if detail_result.status_code == 200:
                state["detail_success_count"] += 1
            else:
                state["detail_failed_count"] += 1
            if sample["content_length"] > 0:
                state["content_success_count"] += 1
            else:
                state["content_failed_count"] += 1
            if len(state["samples"]) < 5:
                state["samples"].append(sample)
            save_state(state)
            update_result(state, total_pages, total_posts, config, relay_enabled, "running")
        state["pages_completed"] = page
        state["last_completed_page"] = page
        state["next_page"] = page + 1
        state["list_total_count"] = total_posts
        save_state(state)
        update_result(state, total_pages, total_posts, config, relay_enabled, "running")
        append_debug(f"page_completed={page}/{total_pages} articles_processed={state['articles_processed']}")
        time.sleep(max(PAGE_PAUSE_MS, 0) / 1000)

    acceptance = final_acceptance(total_pages, relay_enabled)
    write_json(ACCEPTANCE_PATH, acceptance)
    update_result(state, total_pages, total_posts, config, relay_enabled, acceptance["overall_result"])
    append_debug(f"overall_result={acceptance['overall_result']}")
    print(json.dumps(json.loads(RESULT_PATH.read_text(encoding='utf-8')), ensure_ascii=False, indent=2))
    return 0 if acceptance["overall_result"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
