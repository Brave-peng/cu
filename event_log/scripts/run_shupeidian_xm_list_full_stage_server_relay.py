"""Run a full shupeidian.bjx.com.cn/xm/ list crawl and merge to main DB."""

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
from urllib.parse import urlparse
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")


SOURCE_NAME = "shupeidian.bjx.com.cn/xm"
CHANNEL_URL = "https://shupeidian.bjx.com.cn/xm/"
DETAIL_BLOCK_REASON = "detail_blocked_by_waf_list_only_source"
TASK_ID = os.getenv("TASK_ID", "shupeidian_xm_list_full_stage_relay_v1")
TARGET_NAME = os.getenv("TARGET_NAME", "shupeidian_xm_list_full_stage_relay")
DATA_ROOT = Path(os.getenv("DATA_ROOT", "/data/event_log"))
RUN_DATE = os.getenv("RUN_DATE", datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d"))
RUN_DIR = Path(os.getenv("RUN_DIR", str(DATA_ROOT / "runs" / RUN_DATE / TASK_ID)))
STAGE_DB_PATH = Path(os.getenv("STAGE_DB_PATH", str(DATA_ROOT / "db" / "shupeidian_xm_stage.db")))
MAIN_DB_PATH = Path(os.getenv("MAIN_DB_PATH", str(DATA_ROOT / "db" / "event_log.db")))
RESULT_PATH = RUN_DIR / "crawl_result.json"
MERGE_RESULT_PATH = RUN_DIR / "merge_result.json"
ACCEPTANCE_PATH = RUN_DIR / "acceptance.json"
DEBUG_LOG_PATH = RUN_DIR / "debug.log"
TASK_PATH = RUN_DIR / "task.json"
STATE_PATH = RUN_DIR / "progress_state.json"
REQUEST_INTERVAL_MS = int(os.getenv("REQUEST_INTERVAL_MS", "120"))
REQUEST_TIMEOUT_SECONDS = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "12"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "2"))
TRANSIENT_BACKOFF_SECONDS = float(os.getenv("TRANSIENT_BACKOFF_SECONDS", "0.5"))
BLOCKED_BACKOFF_SECONDS = int(os.getenv("BLOCKED_BACKOFF_SECONDS", "5"))
JITTER_MS = int(os.getenv("JITTER_MS", "80"))
PAGE_PAUSE_MS = int(os.getenv("PAGE_PAUSE_MS", "120"))
THROUGHPUT_TARGET_SECONDS = float(os.getenv("THROUGHPUT_TARGET_SECONDS", "1.0"))
MAX_PAGES = int(os.getenv("MAX_PAGES", "0"))


LIST_BLOCK_RE = re.compile(r'<div class="cc-list-content"><ul>(.*?)</ul>', re.S)
ITEM_RE = re.compile(
    r'<li><a href="(?P<url>[^"]+)"[^>]*title="(?P<title>[^"]+)">.*?</a><span>(?P<date>[^<]+)</span></li>',
    re.S,
)
TOTAL_PAGES_RE = re.compile(r'name="pageNumber"[^>]*max="(?P<pages>\d+)"')


class FetchResult:
    def __init__(self, status_code: int, content: str, headers: dict[str, str], error: str | None = None) -> None:
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


def clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", unescape(value or "")).strip()


def build_request(url: str) -> Request:
    return Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        },
    )


def fetch_with_retry(url: str, label: str) -> FetchResult:
    attempts = max(MAX_RETRIES, 0) + 1
    last_result = FetchResult(0, "", {}, "not_started")
    for attempt in range(1, attempts + 1):
        try:
            with urlopen(build_request(url), timeout=REQUEST_TIMEOUT_SECONDS) as response:
                last_result = FetchResult(
                    response.status,
                    response.read().decode("utf-8", errors="replace"),
                    dict(response.headers.items()),
                )
        except (TimeoutError, OSError, http.client.HTTPException) as exc:
            last_result = FetchResult(0, "", {}, str(exc))
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


def page_url(page: int) -> str:
    return CHANNEL_URL if page == 1 else f"{CHANNEL_URL}{page}/"


def parse_listing(html_text: str) -> tuple[list[dict[str, str]], int]:
    match = LIST_BLOCK_RE.search(html_text)
    block = match.group(1) if match else ""
    entries: list[dict[str, str]] = []
    for item in ITEM_RE.finditer(block):
        article_url = clean_text(item.group("url"))
        title = clean_text(item.group("title"))
        published_text = clean_text(item.group("date"))
        if not article_url or not title:
            continue
        entries.append(
            {
                "title": title,
                "article_url": article_url,
                "published_text": published_text,
            }
        )
    pages_match = TOTAL_PAGES_RE.search(html_text)
    total_pages = int(pages_match.group("pages")) if pages_match else 1
    return entries, total_pages


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
            "SELECT dedupe_key FROM event_log_articles WHERE source_name = ?",
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


def ensure_task_file() -> None:
    payload = {
        "task_id": TASK_ID,
        "target_name": TARGET_NAME,
        "task_type": "crawl",
        "goal": "完成 shupeidian.bjx.com.cn/xm/ 项目频道全量分页抓取，按列表源写入 stage DB，并合并到主库",
        "inputs": [
            CHANNEL_URL,
            str(STAGE_DB_PATH),
            str(MAIN_DB_PATH),
        ],
        "done_when": [
            "acceptance overall_result = pass",
            "all_pages_completed = pass",
            "merge_result.status = merged",
        ],
        "retry_limit": 3,
        "created_at": now_iso(),
    }
    write_json(TASK_PATH, payload)


def build_sample(item: dict[str, str]) -> dict[str, object]:
    return {
        "title": item["title"],
        "detail_title": item["title"],
        "article_url": item["article_url"],
        "published_text": item["published_text"],
        "content_length": 0,
        "content_preview": "",
        "content_full": "",
        "dedupe_key": normalize_dedupe_key(item["article_url"]),
        "detail_status_code": 0,
        "fetched_at": now_iso(),
        "fetch_error": DETAIL_BLOCK_REASON,
    }


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


def update_result(state: dict[str, object], merge_result: dict[str, object], status: str) -> None:
    throughput = (
        float(state["elapsed_seconds"]) / int(state["written_count"])
        if int(state["written_count"]) > 0
        else None
    )
    payload = {
        "source_name": SOURCE_NAME,
        "task_id": TASK_ID,
        "target_name": TARGET_NAME,
        "channel_url": CHANNEL_URL,
        "pages_completed": state["pages_completed"],
        "total_pages_discovered": state["total_pages_discovered"],
        "list_total_count": state["list_total_count"],
        "written_count": state["written_count"],
        "detail_url_count": state["detail_url_count"],
        "detail_blocked_count": state["detail_blocked_count"],
        "elapsed_seconds": round(float(state["elapsed_seconds"]), 3),
        "throughput_seconds_per_article": round(throughput, 3) if throughput is not None else None,
        "stage_db_path": str(STAGE_DB_PATH),
        "main_db_path": str(MAIN_DB_PATH),
        "run_dir": str(RUN_DIR),
        "last_processed_url": state["last_processed_url"],
        "samples": state["samples"][:5],
        "merge_result": merge_result,
        "status": status,
        "generated_at": now_iso(),
    }
    write_json(RESULT_PATH, payload)


def write_acceptance(state: dict[str, object], merge_result: dict[str, object]) -> None:
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
        "detail_url_collected": "pass"
        if int(state["detail_url_count"]) == int(state["written_count"]) and int(state["written_count"]) > 0
        else "fail",
        "list_only_strategy_applied": "pass"
        if int(state["detail_blocked_count"]) == int(state["written_count"]) and int(state["written_count"]) > 0
        else "fail",
        "stage_db_recorded": "pass" if stage_count > 0 else "fail",
        "stage_db_count_consistent": "pass"
        if stage_count == int(state["written_count"]) and stage_count > 0
        else "fail",
        "throughput_target_met": "pass"
        if throughput is not None and throughput <= THROUGHPUT_TARGET_SECONDS
        else "fail",
        "main_db_merge_success": "pass" if merge_result.get("status") == "merged" else "fail",
        "main_db_count_consistent": "pass"
        if main_count == stage_count and main_count > 0
        else "fail",
        "failure_traceable": "pass" if DEBUG_LOG_PATH.exists() else "fail",
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
    known_dedupe_keys = load_existing_dedupe_keys(STAGE_DB_PATH, SOURCE_NAME) | load_existing_dedupe_keys(MAIN_DB_PATH, SOURCE_NAME)
    state = load_state(
        {
            "pages_completed": 0,
            "total_pages_discovered": 1,
            "next_page": 1,
            "list_total_count": 0,
            "written_count": db_count(STAGE_DB_PATH, SOURCE_NAME),
            "detail_url_count": db_count(STAGE_DB_PATH, SOURCE_NAME),
            "detail_blocked_count": db_count(STAGE_DB_PATH, SOURCE_NAME),
            "elapsed_seconds": 0.0,
            "last_processed_url": None,
            "samples": [],
        }
    )
    pending_batch: list[dict[str, object]] = []
    merge_result: dict[str, object] = {"status": "skipped", "merged_count": 0, "generated_at": now_iso()}

    append_debug(f"start task_id={TASK_ID} channel_url={CHANNEL_URL} timeout={REQUEST_TIMEOUT_SECONDS}")

    page = int(state.get("next_page", 1))
    while True:
        listing = fetch_with_retry(page_url(page), f"list page {page}")
        if listing.status_code != 200:
            append_debug(f"list page failed page={page} status={listing.status_code} error={listing.error}")
            state["elapsed_seconds"] = time.perf_counter() - start_time
            save_state(state)
            update_result(state, merge_result, "failed")
            write_acceptance(state, merge_result)
            return 1
        entries, total_pages = parse_listing(listing.content)
        state["total_pages_discovered"] = total_pages
        append_debug(f"page={page} entries={len(entries)} total_pages={total_pages}")
        if not entries:
            break
        state["pages_completed"] = int(state["pages_completed"]) + 1
        state["next_page"] = page + 1
        state["list_total_count"] = int(state["list_total_count"]) + len(entries)
        for item in entries:
            dedupe_key = normalize_dedupe_key(item["article_url"])
            if dedupe_key in known_dedupe_keys:
                continue
            sample = build_sample(item)
            known_dedupe_keys.add(dedupe_key)
            pending_batch.append(sample)
            state["detail_url_count"] = int(state["detail_url_count"]) + 1
            state["detail_blocked_count"] = int(state["detail_blocked_count"]) + 1
            state["last_processed_url"] = item["article_url"]
            if len(state["samples"]) < 5:
                state["samples"].append(
                    {
                        "title": sample["title"],
                        "article_url": sample["article_url"],
                        "published_text": sample["published_text"],
                        "dedupe_key": sample["dedupe_key"],
                        "fetch_error": sample["fetch_error"],
                    }
                )
        if pending_batch:
            upsert_articles(STAGE_DB_PATH, pending_batch)
            pending_batch.clear()
        state["written_count"] = db_count(STAGE_DB_PATH, SOURCE_NAME)
        save_state(state)
        if MAX_PAGES > 0 and page >= MAX_PAGES:
            append_debug(f"reached max_pages={MAX_PAGES} stop")
            break
        if page >= total_pages:
            break
        time.sleep(max(PAGE_PAUSE_MS, 0) / 1000)
        sleep_low_pressure()
        page += 1

    state["written_count"] = db_count(STAGE_DB_PATH, SOURCE_NAME)
    state["elapsed_seconds"] = time.perf_counter() - start_time
    save_state(state)
    merge_result = merge_stage_to_main(STAGE_DB_PATH, MAIN_DB_PATH)
    update_result(state, merge_result, "completed")
    write_acceptance(state, merge_result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
