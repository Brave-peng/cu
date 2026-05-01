"""Run a full Splash247 Ports and Logistics crawl via WordPress REST API."""

from __future__ import annotations

from datetime import datetime, timezone
from html import unescape
import http.client
import json
import os
from pathlib import Path
import random
import re
import sqlite3
import sys
import time
from typing import Iterable
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen

from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")


SOURCE_NAME = "splash247_ports_logistics"
DRY_CARGO_SOURCE_NAME = "splash247_dry_cargo"
CATEGORY_ID = int(os.getenv("CATEGORY_ID", "64"))
PAGE_LIMIT = int(os.getenv("PAGE_LIMIT", "100"))
TASK_ID = os.getenv("TASK_ID", "splash247_ports_logistics_full_stage_relay_v1")
TARGET_NAME = os.getenv("TARGET_NAME", "splash247_ports_logistics_full_stage_relay")
DATA_ROOT = Path(os.getenv("DATA_ROOT", "/data/event_log"))
RUN_DATE = os.getenv("RUN_DATE", datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d"))
RUN_DIR = Path(os.getenv("RUN_DIR", str(DATA_ROOT / "runs" / RUN_DATE / TASK_ID)))
STAGE_DB_PATH = Path(
    os.getenv("STAGE_DB_PATH", str(DATA_ROOT / "db" / "splash247_ports_logistics_stage.db"))
)
MAIN_DB_PATH = Path(os.getenv("MAIN_DB_PATH", str(DATA_ROOT / "db" / "event_log.db")))
DRY_CARGO_STAGE_DB_PATH = Path(
    os.getenv("DRY_CARGO_STAGE_DB_PATH", str(DATA_ROOT / "db" / "splash247_dry_cargo_stage.db"))
)
RESULT_PATH = RUN_DIR / "crawl_result.json"
OVERLAP_RESULT_PATH = RUN_DIR / "overlap_report.json"
MERGE_RESULT_PATH = RUN_DIR / "merge_result.json"
ACCEPTANCE_PATH = RUN_DIR / "acceptance.json"
DEBUG_LOG_PATH = RUN_DIR / "debug.log"
TASK_PATH = RUN_DIR / "task.json"
STATE_PATH = RUN_DIR / "progress_state.json"
THROUGHPUT_TARGET_SECONDS = float(os.getenv("THROUGHPUT_TARGET_SECONDS", "3.0"))
REQUEST_INTERVAL_MS = int(os.getenv("REQUEST_INTERVAL_MS", "120"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "2"))
REQUEST_TIMEOUT_SECONDS = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "8"))
JITTER_MS = int(os.getenv("JITTER_MS", "80"))
PAGE_PAUSE_MS = int(os.getenv("PAGE_PAUSE_MS", "150"))
BLOCKED_BACKOFF_SECONDS = int(os.getenv("BLOCKED_BACKOFF_SECONDS", "8"))
TRANSIENT_BACKOFF_SECONDS = float(os.getenv("TRANSIENT_BACKOFF_SECONDS", "0.5"))
BATCH_COMMIT_SIZE = int(os.getenv("BATCH_COMMIT_SIZE", "100"))
MAX_PAGES = int(os.getenv("MAX_PAGES", "0"))
WP_API_URL = os.getenv("WP_API_URL", "https://splash247.com/wp-json/wp/v2/posts")


class FetchResult:
    def __init__(self, status_code: int, content: bytes, headers: dict[str, str], error: str | None = None) -> None:
        self.status_code = status_code
        self.content = content
        self.headers = headers
        self.error = error


def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def append_debug(message: str) -> None:
    RUN_DIR.mkdir(parents=True, exist_ok=True)
    with DEBUG_LOG_PATH.open("a", encoding="utf-8") as handle:
        handle.write(f"[{now_iso()}] {message}\n")


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def sleep_low_pressure() -> None:
    jitter = random.randint(0, max(JITTER_MS, 0)) if JITTER_MS > 0 else 0
    time.sleep((max(REQUEST_INTERVAL_MS, 0) + jitter) / 1000)


def build_request(url: str) -> Request:
    return Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        },
    )


def fetch_with_retry(url: str, label: str) -> FetchResult:
    attempts = max(MAX_RETRIES, 0) + 1
    last_result = FetchResult(0, b"", {}, "not_started")
    for attempt in range(1, attempts + 1):
        try:
            with urlopen(build_request(url), timeout=REQUEST_TIMEOUT_SECONDS) as response:
                last_result = FetchResult(
                    response.status,
                    response.read(),
                    dict(response.headers.items()),
                )
        except (TimeoutError, OSError, http.client.HTTPException) as exc:
            last_result = FetchResult(0, b"", {}, str(exc))
        append_debug(
            f"fetch {label} attempt={attempt}/{attempts} status={last_result.status_code} url={url} error={last_result.error}"
        )
        if last_result.status_code == 200 and not last_result.error:
            return last_result
        if last_result.status_code in {403, 429}:
            time.sleep(max(BLOCKED_BACKOFF_SECONDS, 0))
        elif last_result.status_code == 0:
            time.sleep(max(TRANSIENT_BACKOFF_SECONDS, 0.0))
        if attempt < attempts:
            sleep_low_pressure()
    return last_result


def fetch_json(url: str, label: str) -> tuple[object | None, dict[str, str], int, str | None]:
    result = fetch_with_retry(url, label)
    if result.status_code != 200:
        return None, result.headers, result.status_code, result.error
    try:
        payload = json.loads(result.content.decode("utf-8", errors="replace"))
    except json.JSONDecodeError as exc:
        return None, result.headers, result.status_code, f"json_decode_error:{exc}"
    return payload, result.headers, result.status_code, None


def build_list_url(page: int) -> str:
    params = urlencode(
        {
            "categories": CATEGORY_ID,
            "per_page": PAGE_LIMIT,
            "page": page,
            "_fields": "id,date,link,title,content",
        }
    )
    return f"{WP_API_URL}?{params}"


def clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", unescape(value or "")).strip()


def html_to_text(html_text: str) -> str:
    soup = BeautifulSoup(html_text or "", "html.parser")
    for bad in soup.select("script, style, noscript, iframe, form, aside, nav"):
        bad.decompose()
    paragraphs = [clean_text(node.get_text(" ")) for node in soup.select("p")]
    paragraphs = [item for item in paragraphs if item]
    if paragraphs:
        return "\n\n".join(paragraphs)
    return clean_text(soup.get_text("\n"))


def normalize_dedupe_key(url: str) -> str:
    parsed = urlparse(url)
    return f"{parsed.netloc}{parsed.path}".rstrip("/").lower()


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA busy_timeout=60000")
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


def load_existing_dedupe_keys(db_path: Path, source_name: str = SOURCE_NAME) -> set[str]:
    if not db_path.exists():
        return set()
    with sqlite3.connect(db_path, timeout=60) as conn:
        init_db(conn)
        rows = conn.execute(
            """
            SELECT dedupe_key
            FROM event_log_articles
            WHERE source_name = ?
              AND content_length > 0
              AND (fetch_error IS NULL OR fetch_error = '')
            """,
            (source_name,),
        ).fetchall()
    return {row[0] for row in rows}


def db_count(db_path: Path, source_name: str = SOURCE_NAME) -> int:
    if not db_path.exists():
        return 0
    with sqlite3.connect(db_path, timeout=60) as conn:
        init_db(conn)
        row = conn.execute(
            "SELECT COUNT(1) FROM event_log_articles WHERE source_name = ?",
            (source_name,),
        ).fetchone()
    return int(row[0]) if row else 0


def upsert_articles(db_path: Path, samples: list[dict[str, object]]) -> None:
    if not samples:
        return
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path, timeout=60) as conn:
        init_db(conn)
        conn.executemany(
            """
            INSERT OR REPLACE INTO event_log_articles (
                source_name, dedupe_key, article_url, title, detail_title, published_text,
                content, content_length, content_preview, http_status, fetch_error, fetched_at, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    SOURCE_NAME,
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
                )
                for sample in samples
            ],
        )


def iter_stage_rows(db_path: Path) -> Iterable[tuple]:
    with sqlite3.connect(db_path, timeout=60) as conn:
        init_db(conn)
        rows = conn.execute(
            """
            SELECT source_name, dedupe_key, article_url, title, detail_title, published_text,
                   content, content_length, content_preview, http_status, fetch_error, fetched_at, created_at
            FROM event_log_articles
            WHERE source_name = ?
            ORDER BY published_text DESC, article_url DESC
            """,
            (SOURCE_NAME,),
        ).fetchall()
    return rows


def merge_stage_to_main(stage_db_path: Path, main_db_path: Path, max_attempts: int = 5) -> dict[str, object]:
    rows = list(iter_stage_rows(stage_db_path))
    if not rows:
        result = {"status": "empty_stage", "merged_count": 0, "generated_at": now_iso()}
        write_json(MERGE_RESULT_PATH, result)
        return result
    for attempt in range(1, max_attempts + 1):
        try:
            main_db_path.parent.mkdir(parents=True, exist_ok=True)
            with sqlite3.connect(main_db_path, timeout=60) as conn:
                init_db(conn)
                conn.executemany(
                    """
                    INSERT OR REPLACE INTO event_log_articles (
                        source_name, dedupe_key, article_url, title, detail_title, published_text,
                        content, content_length, content_preview, http_status, fetch_error, fetched_at, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    rows,
                )
            result = {
                "status": "merged",
                "merged_count": len(rows),
                "main_db_count": db_count(main_db_path),
                "generated_at": now_iso(),
            }
            write_json(MERGE_RESULT_PATH, result)
            append_debug(f"merge main_db status=merged rows={len(rows)}")
            return result
        except sqlite3.OperationalError as exc:
            append_debug(f"merge main_db attempt={attempt}/{max_attempts} error={exc}")
            time.sleep(5 * attempt)
    result = {
        "status": "merge_failed",
        "merged_count": 0,
        "main_db_count": db_count(main_db_path),
        "generated_at": now_iso(),
    }
    write_json(MERGE_RESULT_PATH, result)
    return result


def write_overlap_report(stage_db_path: Path, dry_cargo_db_path: Path) -> dict[str, object]:
    stage_keys = load_existing_dedupe_keys(stage_db_path, SOURCE_NAME)
    dry_keys = load_existing_dedupe_keys(dry_cargo_db_path, DRY_CARGO_SOURCE_NAME)
    overlap_keys = sorted(stage_keys & dry_keys)
    overlap_ratio = (len(overlap_keys) / len(stage_keys)) if stage_keys else 0.0
    samples: list[dict[str, object]] = []
    if overlap_keys:
        with sqlite3.connect(stage_db_path, timeout=60) as conn:
            init_db(conn)
            placeholders = ",".join("?" for _ in overlap_keys[:10])
            rows = conn.execute(
                f"""
                SELECT article_url, title, published_text
                FROM event_log_articles
                WHERE source_name = ? AND dedupe_key IN ({placeholders})
                ORDER BY published_text DESC, article_url DESC
                """,
                (SOURCE_NAME, *overlap_keys[:10]),
            ).fetchall()
        samples = [
            {"article_url": row[0], "title": row[1], "published_text": row[2]}
            for row in rows
        ]
    report = {
        "source_name": SOURCE_NAME,
        "comparison_source_name": DRY_CARGO_SOURCE_NAME,
        "stage_count": len(stage_keys),
        "comparison_count": len(dry_keys),
        "overlap_count": len(overlap_keys),
        "overlap_ratio": round(overlap_ratio, 4),
        "overlap_samples": samples,
        "generated_at": now_iso(),
    }
    write_json(OVERLAP_RESULT_PATH, report)
    append_debug(
        f"overlap computed stage_count={len(stage_keys)} comparison_count={len(dry_keys)} overlap_count={len(overlap_keys)} overlap_ratio={report['overlap_ratio']}"
    )
    return report


def ensure_task_file() -> None:
    payload = {
        "task_id": TASK_ID,
        "target_name": TARGET_NAME,
        "task_type": "crawl",
        "goal": "通过 WordPress REST API 完成 Splash247 Ports and Logistics 全量抓取，写入 stage DB，评估与 dry cargo 重合度，并合并到主库",
        "inputs": [
            f"{WP_API_URL}?categories={CATEGORY_ID}",
            str(DRY_CARGO_STAGE_DB_PATH),
            str(MAIN_DB_PATH),
        ],
        "done_when": [
            "acceptance overall_result = pass",
            "throughput_seconds_per_article <= 3.0",
            "merge_result.status = merged",
            "overlap_report.exists = true",
        ],
        "retry_limit": 3,
        "created_at": now_iso(),
    }
    write_json(TASK_PATH, payload)


def save_state(state: dict[str, object]) -> None:
    write_json(STATE_PATH, state)


def load_state(default_state: dict[str, object]) -> dict[str, object]:
    if not STATE_PATH.exists():
        return default_state
    try:
        stored = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default_state
    if not isinstance(stored, dict):
        return default_state
    merged = dict(default_state)
    merged.update(stored)
    if "next_page" not in stored:
        merged["next_page"] = int(merged.get("pages_completed", 0)) + 1
    return merged


def build_sample(post: dict[str, object]) -> dict[str, object]:
    title_data = post.get("title", {})
    content_data = post.get("content", {})
    title = clean_text(title_data.get("rendered", "") if isinstance(title_data, dict) else str(title_data))
    content_html = content_data.get("rendered", "") if isinstance(content_data, dict) else str(content_data)
    article_url = str(post.get("link") or "")
    content_full = html_to_text(content_html)
    published_text = str(post.get("date") or "")
    return {
        "title": title,
        "detail_title": title,
        "article_url": article_url,
        "published_text": published_text,
        "content_length": len(content_full),
        "content_preview": content_full[:280],
        "content_full": content_full,
        "dedupe_key": normalize_dedupe_key(article_url),
        "detail_status_code": 200,
        "fetched_at": now_iso(),
        "fetch_error": None,
    }


def update_result(
    state: dict[str, object],
    merge_result: dict[str, object],
    overlap_report: dict[str, object],
    status: str,
) -> None:
    throughput = (
        float(state["elapsed_seconds"]) / int(state["written_count"])
        if int(state["written_count"]) > 0
        else None
    )
    payload = {
        "source_name": SOURCE_NAME,
        "task_id": TASK_ID,
        "target_name": TARGET_NAME,
        "category_id": CATEGORY_ID,
        "page_limit": PAGE_LIMIT,
        "pages_completed": state["pages_completed"],
        "total_pages_discovered": state["total_pages_discovered"],
        "list_total_count": state["list_total_count"],
        "detail_attempted_count": state["detail_attempted_count"],
        "detail_success_count": state["detail_success_count"],
        "detail_failed_count": state["detail_failed_count"],
        "content_success_count": state["content_success_count"],
        "content_failed_count": state["content_failed_count"],
        "skipped_existing_count": state["skipped_existing_count"],
        "written_count": state["written_count"],
        "last_processed_url": state["last_processed_url"],
        "elapsed_seconds": round(float(state["elapsed_seconds"]), 3),
        "throughput_seconds_per_article": round(throughput, 3) if throughput is not None else None,
        "stage_db_path": str(STAGE_DB_PATH),
        "main_db_path": str(MAIN_DB_PATH),
        "dry_cargo_stage_db_path": str(DRY_CARGO_STAGE_DB_PATH),
        "run_dir": str(RUN_DIR),
        "samples": state["samples"][:5],
        "failed_samples": state["failed_samples"][:5],
        "overlap_report": overlap_report,
        "merge_result": merge_result,
        "status": status,
        "generated_at": now_iso(),
    }
    write_json(RESULT_PATH, payload)


def write_acceptance(
    state: dict[str, object],
    merge_result: dict[str, object],
    overlap_report: dict[str, object],
) -> None:
    stage_count = db_count(STAGE_DB_PATH, SOURCE_NAME)
    main_count = db_count(MAIN_DB_PATH, SOURCE_NAME)
    throughput = (
        float(state["elapsed_seconds"]) / int(state["written_count"])
        if int(state["written_count"]) > 0
        else None
    )
    checks = {
        "task_defined": "pass" if TASK_PATH.exists() else "fail",
        "list_fetch_success": "pass" if int(state["list_total_count"]) > 0 else "fail",
        "all_pages_completed": "pass"
        if int(state["pages_completed"]) == int(state["total_pages_discovered"]) and int(state["pages_completed"]) > 0
        else "fail",
        "detail_fetch_success": "pass"
        if int(state["detail_failed_count"]) == 0 and int(state["detail_success_count"]) > 0
        else "fail",
        "content_fetch_success": "pass"
        if int(state["content_failed_count"]) == 0 and int(state["content_success_count"]) > 0
        else "fail",
        "content_clean_text": "pass"
        if state["samples"] and all(int(sample["content_length"]) > 200 for sample in state["samples"][:5])
        else "fail",
        "time_field_available": "pass"
        if state["samples"] and all(bool(sample["published_text"]) for sample in state["samples"][:5])
        else "fail",
        "dedupe_key_generated": "pass"
        if state["samples"] and len({sample["dedupe_key"] for sample in state["samples"]}) == len(state["samples"])
        else "fail",
        "stage_db_recorded": "pass" if stage_count > 0 else "fail",
        "stage_db_count_consistent": "pass"
        if stage_count == int(state["written_count"]) and stage_count > 0
        else "fail",
        "throughput_target_met": "pass"
        if throughput is not None and throughput <= THROUGHPUT_TARGET_SECONDS
        else "fail",
        "cross_source_overlap_reported": "pass"
        if OVERLAP_RESULT_PATH.exists() and overlap_report.get("stage_count", 0) == stage_count
        else "fail",
        "main_db_merge_success": "pass" if merge_result.get("status") == "merged" else "fail",
        "main_db_count_consistent": "pass"
        if main_count == stage_count and main_count > 0
        else "fail",
        "failure_traceable": "pass"
        if DEBUG_LOG_PATH.exists()
        and (int(state["detail_failed_count"]) == 0 or len(state["failed_samples"]) == int(state["detail_failed_count"]))
        else "fail",
    }
    overall = "pass" if all(value == "pass" for value in checks.values()) else "fail"
    write_json(
        ACCEPTANCE_PATH,
        {
            "target_name": TARGET_NAME,
            "task_id": TASK_ID,
            "checks": checks,
            "overall_result": overall,
            "generated_at": now_iso(),
        },
    )


def main() -> int:
    RUN_DIR.mkdir(parents=True, exist_ok=True)
    ensure_task_file()
    start_time = time.perf_counter()
    existing_stage = load_existing_dedupe_keys(STAGE_DB_PATH, SOURCE_NAME)
    existing_main = load_existing_dedupe_keys(MAIN_DB_PATH, SOURCE_NAME)
    known_dedupe_keys = set(existing_stage) | set(existing_main)
    state = load_state({
        "pages_completed": 0,
        "total_pages_discovered": 1,
        "next_page": 1,
        "list_total_count": 0,
        "detail_attempted_count": 0,
        "detail_success_count": 0,
        "detail_failed_count": 0,
        "content_success_count": 0,
        "content_failed_count": 0,
        "skipped_existing_count": 0,
        "written_count": len(existing_stage),
        "elapsed_seconds": 0.0,
        "last_processed_url": None,
        "samples": [],
        "failed_samples": [],
    })
    pending_batch: list[dict[str, object]] = []
    overlap_report: dict[str, object] = {}
    merge_result: dict[str, object] = {"status": "skipped", "merged_count": 0, "generated_at": now_iso()}

    append_debug(
        "start task_id=%s category_id=%s page_limit=%s timeout=%s"
        % (TASK_ID, CATEGORY_ID, PAGE_LIMIT, REQUEST_TIMEOUT_SECONDS)
    )

    page = int(state.get("next_page", 1))
    while True:
        payload, headers, status_code, error = fetch_json(build_list_url(page), f"list page {page}")
        if payload is None or not isinstance(payload, list):
            append_debug(f"list page failed page={page} status={status_code} error={error}")
            state["elapsed_seconds"] = time.perf_counter() - start_time
            update_result(state, merge_result, overlap_report, "failed")
            write_acceptance(state, merge_result, overlap_report)
            return 1
        rows = payload
        total_pages = int(headers.get("X-WP-TotalPages", "1"))
        total_items = int(headers.get("X-WP-Total", "0"))
        state["total_pages_discovered"] = total_pages
        append_debug(f"page={page} rows={len(rows)} total_pages={total_pages} total_items={total_items}")
        if not rows:
            break
        state["pages_completed"] = int(state["pages_completed"]) + 1
        state["next_page"] = page + 1
        state["list_total_count"] = int(state["list_total_count"]) + len(rows)

        for post in rows:
            article_url = str(post.get("link") or "")
            if not article_url:
                continue
            dedupe_key = normalize_dedupe_key(article_url)
            if dedupe_key in known_dedupe_keys:
                state["skipped_existing_count"] = int(state["skipped_existing_count"]) + 1
                continue
            sample = build_sample(post)
            state["detail_attempted_count"] = int(state["detail_attempted_count"]) + 1
            if sample.get("fetch_error"):
                state["detail_failed_count"] = int(state["detail_failed_count"]) + 1
                state["content_failed_count"] = int(state["content_failed_count"]) + 1
                state["failed_samples"].append(
                    {
                        "article_url": sample["article_url"],
                        "title": sample["title"],
                        "fetch_error": sample["fetch_error"],
                    }
                )
            else:
                state["detail_success_count"] = int(state["detail_success_count"]) + 1
                if int(sample["content_length"]) > 0:
                    state["content_success_count"] = int(state["content_success_count"]) + 1
                else:
                    state["content_failed_count"] = int(state["content_failed_count"]) + 1
                    state["failed_samples"].append(
                        {
                            "article_url": sample["article_url"],
                            "title": sample["title"],
                            "fetch_error": "empty_content",
                        }
                    )
                if len(state["samples"]) < 5:
                    state["samples"].append(
                        {
                            "title": sample["title"],
                            "article_url": sample["article_url"],
                            "published_text": sample["published_text"],
                            "content_length": sample["content_length"],
                            "content_preview": sample["content_preview"],
                            "dedupe_key": sample["dedupe_key"],
                        }
                    )
            known_dedupe_keys.add(dedupe_key)
            state["last_processed_url"] = sample["article_url"]
            pending_batch.append(sample)
            if len(pending_batch) >= BATCH_COMMIT_SIZE:
                upsert_articles(STAGE_DB_PATH, pending_batch)
                state["written_count"] = db_count(STAGE_DB_PATH, SOURCE_NAME)
                save_state(state)
                pending_batch.clear()

        if pending_batch:
            upsert_articles(STAGE_DB_PATH, pending_batch)
            state["written_count"] = db_count(STAGE_DB_PATH, SOURCE_NAME)
            pending_batch.clear()
        save_state(state)

        if MAX_PAGES > 0 and page >= MAX_PAGES:
            append_debug(f"reached max_pages={MAX_PAGES} stop")
            break
        if page >= total_pages:
            break
        time.sleep(max(PAGE_PAUSE_MS, 0) / 1000)
        page += 1
        sleep_low_pressure()

    state["written_count"] = db_count(STAGE_DB_PATH, SOURCE_NAME)
    state["elapsed_seconds"] = time.perf_counter() - start_time
    save_state(state)
    overlap_report = write_overlap_report(STAGE_DB_PATH, DRY_CARGO_STAGE_DB_PATH)
    merge_result = merge_stage_to_main(STAGE_DB_PATH, MAIN_DB_PATH)
    update_result(state, merge_result, overlap_report, "completed")
    write_acceptance(state, merge_result, overlap_report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
