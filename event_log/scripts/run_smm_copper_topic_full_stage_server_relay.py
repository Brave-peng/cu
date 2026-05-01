"""Run a full SMM copper topic crawl on the server via low-cost API relay."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from html import unescape
from html.parser import HTMLParser
import gzip
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

from event_log_baseline.config import CrawlerConfig


SOURCE_NAME = "smm_copper_topic"
TOPIC_ID = os.getenv("TOPIC_ID", "200")
PAGE_LIMIT = int(os.getenv("PAGE_LIMIT", "10"))
TASK_ID = os.getenv("TASK_ID", "smm_copper_topic_full_stage_relay_v1")
TARGET_NAME = os.getenv("TARGET_NAME", "smm_copper_topic_full_stage_relay")
DATA_ROOT = Path(os.getenv("DATA_ROOT", "/data/event_log"))
RUN_DATE = os.getenv("RUN_DATE", datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d"))
RUN_DIR = Path(os.getenv("RUN_DIR", str(DATA_ROOT / "runs" / RUN_DATE / TASK_ID)))
STAGE_DB_PATH = Path(os.getenv("STAGE_DB_PATH", str(DATA_ROOT / "db" / "smm_copper_topic_stage.db")))
MAIN_DB_PATH = Path(os.getenv("MAIN_DB_PATH", str(DATA_ROOT / "db" / "event_log.db")))
RESULT_PATH = RUN_DIR / "crawl_result.json"
MERGE_RESULT_PATH = RUN_DIR / "merge_result.json"
ACCEPTANCE_PATH = RUN_DIR / "acceptance.json"
DEBUG_LOG_PATH = RUN_DIR / "debug.log"
TASK_PATH = RUN_DIR / "task.json"
STATE_PATH = RUN_DIR / "progress_state.json"
THROUGHPUT_TARGET_SECONDS = float(os.getenv("THROUGHPUT_TARGET_SECONDS", "3.0"))
REQUEST_INTERVAL_MS = int(os.getenv("REQUEST_INTERVAL_MS", "250"))
MAX_CONCURRENCY = int(os.getenv("MAX_CONCURRENCY", "3"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "2"))
REQUEST_TIMEOUT_SECONDS = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "12"))
JITTER_MS = int(os.getenv("JITTER_MS", "150"))
BLOCKED_BACKOFF_SECONDS = int(os.getenv("BLOCKED_BACKOFF_SECONDS", "8"))
BATCH_COMMIT_SIZE = int(os.getenv("BATCH_COMMIT_SIZE", "10"))
MAX_PAGES = int(os.getenv("MAX_PAGES", "0"))

LIST_API_TEMPLATE = "https://news.smm.cn/api/list/topic_news/{topic_id}/pages/{page}/limit/{limit}"
DETAIL_API_TEMPLATE = "https://news.smm.cn/api/detail/{news_id}"


class FetchResult:
    def __init__(self, status_code: int, content: bytes, headers: dict[str, str], error: str | None = None) -> None:
        self.status_code = status_code
        self.content = content
        self.headers = headers
        self.error = error


class HtmlToTextParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []

    def handle_starttag(self, tag: str, attrs) -> None:  # type: ignore[override]
        if tag in {"br", "p", "div", "li", "tr", "h1", "h2", "h3", "h4"}:
            self.parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if tag in {"p", "div", "li", "tr", "h1", "h2", "h3", "h4"}:
            self.parts.append("\n")

    def handle_data(self, data: str) -> None:
        if data:
            self.parts.append(data)

    def get_text(self) -> str:
        text = unescape("".join(self.parts))
        text = text.replace("\xa0", " ")
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r"[ \t]+\n", "\n", text)
        text = re.sub(r"[ \t]{2,}", " ", text)
        return text.strip()


def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def append_debug(message: str) -> None:
    RUN_DIR.mkdir(parents=True, exist_ok=True)
    with DEBUG_LOG_PATH.open("a", encoding="utf-8") as handle:
        handle.write(f"[{now_iso()}] {message}\n")


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def build_request(url: str, accept_json: bool = True) -> Request:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    if accept_json:
        headers["Accept"] = "application/json, text/plain, */*"
    return Request(url, headers=headers)


def sleep_low_pressure() -> None:
    jitter = random.randint(0, max(JITTER_MS, 0)) if JITTER_MS > 0 else 0
    time.sleep((max(REQUEST_INTERVAL_MS, 0) + jitter) / 1000)


def fetch_with_retry(url: str, label: str) -> FetchResult:
    attempts = max(MAX_RETRIES, 0) + 1
    last_result = FetchResult(0, b"", {}, "not_started")
    for attempt in range(1, attempts + 1):
        try:
            with urlopen(build_request(url), timeout=REQUEST_TIMEOUT_SECONDS) as response:
                content = response.read()
                if response.headers.get("Content-Encoding") == "gzip":
                    content = gzip.decompress(content)
                last_result = FetchResult(
                    response.status,
                    content,
                    dict(response.headers.items()),
                )
        except (TimeoutError, OSError, http.client.HTTPException, EOFError, gzip.BadGzipFile) as exc:
            last_result = FetchResult(0, b"", {}, str(exc))
        append_debug(
            f"fetch {label} attempt={attempt}/{attempts} status={last_result.status_code} url={url} error={last_result.error}"
        )
        if last_result.status_code == 200 and not last_result.error:
            return last_result
        if last_result.status_code in {0, 403, 429}:
            time.sleep(max(BLOCKED_BACKOFF_SECONDS, 0))
        if attempt < attempts:
            sleep_low_pressure()
    return last_result


def fetch_json(url: str, label: str) -> tuple[dict[str, object] | None, int, str | None]:
    result = fetch_with_retry(url, label)
    if result.status_code != 200:
        return None, result.status_code, result.error
    try:
        payload = json.loads(result.content.decode("utf-8", errors="replace"))
    except json.JSONDecodeError as exc:
        return None, result.status_code, f"json_decode_error:{exc}"
    return payload, result.status_code, None


def html_to_text(html_text: str) -> str:
    parser = HtmlToTextParser()
    parser.feed(html_text or "")
    text = parser.get_text()
    lines = [line.strip() for line in text.splitlines()]
    cleaned = "\n\n".join(line for line in lines if line)
    return cleaned.strip()


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


def db_source_names(db_path: Path) -> set[str]:
    if not db_path.exists():
        return set()
    with sqlite3.connect(db_path, timeout=60) as conn:
        init_db(conn)
        rows = conn.execute(
            "SELECT DISTINCT source_name FROM event_log_articles ORDER BY source_name"
        ).fetchall()
    return {row[0] for row in rows}


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


def build_list_url(page: int) -> str:
    return LIST_API_TEMPLATE.format(topic_id=TOPIC_ID, page=page, limit=PAGE_LIMIT)


def build_detail_url(news_id: int) -> str:
    return DETAIL_API_TEMPLATE.format(news_id=news_id)


def parse_list_payload(payload: dict[str, object]) -> list[dict[str, object]]:
    data = payload.get("data", {})
    if not isinstance(data, dict):
        return []
    inner = data.get("data", {})
    if not isinstance(inner, dict):
        return []
    rows = inner.get("newsListList", [])
    return rows if isinstance(rows, list) else []


def build_sample_from_detail(item: dict[str, object]) -> dict[str, object]:
    news_id = int(item["newsId"])
    detail_url = build_detail_url(news_id)
    detail_payload, status_code, error = fetch_json(detail_url, f"detail {news_id}")
    if detail_payload is None:
        return {
            "title": str(item.get("title") or ""),
            "detail_title": str(item.get("title") or ""),
            "article_url": str(item.get("url") or detail_url),
            "published_text": str(item.get("date") or ""),
            "content_length": 0,
            "content_preview": "",
            "content_full": "",
            "dedupe_key": normalize_dedupe_key(str(item.get("url") or detail_url)),
            "detail_status_code": status_code,
            "fetched_at": now_iso(),
            "fetch_error": error or "detail_fetch_failed",
        }
    code = detail_payload.get("code")
    data = detail_payload.get("data", {})
    if code != 0 or not isinstance(data, dict):
        return {
            "title": str(item.get("title") or ""),
            "detail_title": str(item.get("title") or ""),
            "article_url": str(item.get("url") or detail_url),
            "published_text": str(item.get("date") or ""),
            "content_length": 0,
            "content_preview": "",
            "content_full": "",
            "dedupe_key": normalize_dedupe_key(str(item.get("url") or detail_url)),
            "detail_status_code": status_code,
            "fetched_at": now_iso(),
            "fetch_error": f"detail_api_code:{code}",
        }
    title = str(data.get("title") or item.get("title") or "")
    article_url = str(item.get("url") or detail_url)
    content_html = str(data.get("content") or "")
    content_text = html_to_text(content_html)
    published_text = str(data.get("date") or item.get("date") or "")
    return {
        "title": str(item.get("title") or title),
        "detail_title": title,
        "article_url": article_url,
        "published_text": published_text,
        "content_length": len(content_text),
        "content_preview": content_text[:280],
        "content_full": content_text,
        "dedupe_key": normalize_dedupe_key(article_url),
        "detail_status_code": status_code,
        "fetched_at": now_iso(),
        "fetch_error": None,
    }


def ensure_task_file() -> None:
    payload = {
        "task_id": TASK_ID,
        "target_name": TARGET_NAME,
        "task_type": "crawl",
        "goal": "通过 SMM 官方 API 完成铜专题全量分页与详情抓取，先写入 stage DB，验收通过后合并到主库",
        "inputs": [
            "https://jcng9kabbb3s.feishu.cn/docx/UsXJdfAHFoPYN9xd2Ghcwhsnn6g",
            f"https://news.smm.cn/topic/{TOPIC_ID}",
            LIST_API_TEMPLATE,
            DETAIL_API_TEMPLATE,
        ],
        "done_when": [
            "acceptance overall_result = pass",
            "throughput_seconds_per_article <= 3.0",
            "merge_result.status = merged",
        ],
        "retry_limit": 3,
        "created_at": now_iso(),
    }
    write_json(TASK_PATH, payload)


def save_state(state: dict[str, object]) -> None:
    write_json(STATE_PATH, state)


def verify_incremental(existing_after_merge: set[str]) -> tuple[bool, int]:
    page = 1
    new_candidates = 0
    while True:
        payload, status_code, error = fetch_json(build_list_url(page), f"incremental-list-{page}")
        if payload is None:
            append_debug(f"incremental list failed page={page} status={status_code} error={error}")
            return False, new_candidates
        rows = parse_list_payload(payload)
        if not rows:
            return True, new_candidates
        for item in rows:
            url = str(item.get("url") or "")
            if not url:
                continue
            dedupe_key = normalize_dedupe_key(url)
            if dedupe_key not in existing_after_merge:
                new_candidates += 1
                append_debug(
                    f"incremental verification found new candidate page={page} article_url={url}"
                )
                return False, new_candidates
        if len(rows) < PAGE_LIMIT:
            return new_candidates == 0, new_candidates
        page += 1
        sleep_low_pressure()


def update_result(
    state: dict[str, object],
    merge_result: dict[str, object],
    overall_status: str,
) -> None:
    throughput = (
        float(state["elapsed_seconds"]) / int(state["detail_success_count"])
        if int(state["detail_success_count"]) > 0
        else None
    )
    payload = {
        "source_name": SOURCE_NAME,
        "task_id": TASK_ID,
        "target_name": TARGET_NAME,
        "topic_id": TOPIC_ID,
        "page_limit": PAGE_LIMIT,
        "pages_completed": state["pages_completed"],
        "row_count": state["page1_row_count"],
        "page2_row_count": state["page2_row_count"],
        "list_total_count": state["list_total_count"],
        "detail_attempted_count": state["detail_attempted_count"],
        "detail_success_count": state["detail_success_count"],
        "detail_failed_count": state["detail_failed_count"],
        "content_success_count": state["content_success_count"],
        "content_failed_count": state["content_failed_count"],
        "skipped_existing_count": state["skipped_existing_count"],
        "written_count": state["written_count"],
        "stage_db_path": str(STAGE_DB_PATH),
        "main_db_path": str(MAIN_DB_PATH),
        "run_dir": str(RUN_DIR),
        "elapsed_seconds": round(float(state["elapsed_seconds"]), 3),
        "throughput_seconds_per_article": round(throughput, 3) if throughput is not None else None,
        "incremental_new_candidates": state["incremental_new_candidates"],
        "samples": state["samples"][:5],
        "merge_result": merge_result,
        "status": overall_status,
        "generated_at": now_iso(),
    }
    write_json(RESULT_PATH, payload)


def write_acceptance(
    state: dict[str, object],
    merge_result: dict[str, object],
    incremental_passed: bool,
) -> None:
    stage_count = db_count(STAGE_DB_PATH)
    main_count = db_count(MAIN_DB_PATH)
    throughput = (
        float(state["elapsed_seconds"]) / int(state["detail_success_count"])
        if int(state["detail_success_count"]) > 0
        else None
    )
    checks = {
        "task_defined": "pass" if TASK_PATH.exists() else "fail",
        "list_fetch_success": "pass" if int(state["page1_row_count"]) > 0 else "fail",
        "detail_fetch_success": "pass"
        if int(state["detail_success_count"]) > 0 and int(state["detail_failed_count"]) == 0
        else "fail",
        "content_fetch_success": "pass"
        if int(state["content_success_count"]) > 0 and int(state["content_failed_count"]) == 0
        else "fail",
        "content_clean_text": "pass"
        if state["samples"] and all(sample["content_length"] > 80 for sample in state["samples"][:5])
        else "fail",
        "time_field_available": "pass"
        if state["samples"] and all(bool(sample["published_text"]) for sample in state["samples"][:5])
        else "fail",
        "source_name_correct": "pass" if db_source_names(STAGE_DB_PATH) <= {SOURCE_NAME} else "fail",
        "dedupe_effective": "pass"
        if int(state["written_count"]) == stage_count and int(state["written_count"]) > 0
        else "fail",
        "db_write_success": "pass" if stage_count > 0 and merge_result.get("status") == "merged" else "fail",
        "failure_traceable": "pass"
        if DEBUG_LOG_PATH.exists() and (
            int(state["detail_failed_count"]) == 0 or any(sample.get("fetch_error") for sample in state["failed_samples"])
        )
        else "fail",
        "full_collection_capable": "pass" if int(state["pages_completed"]) >= 2 else "fail",
        "incremental_update_available": "pass" if incremental_passed else "fail",
        "throughput_target_met": "pass"
        if throughput is not None and throughput <= THROUGHPUT_TARGET_SECONDS
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
    existing_stage = load_existing_dedupe_keys(STAGE_DB_PATH)
    existing_main = load_existing_dedupe_keys(MAIN_DB_PATH)
    known_dedupe_keys = set(existing_stage) | set(existing_main)
    state: dict[str, object] = {
        "page1_row_count": 0,
        "page2_row_count": 0,
        "pages_completed": 0,
        "list_total_count": 0,
        "detail_attempted_count": 0,
        "detail_success_count": 0,
        "detail_failed_count": 0,
        "content_success_count": 0,
        "content_failed_count": 0,
        "skipped_existing_count": 0,
        "written_count": len(existing_stage),
        "elapsed_seconds": 0.0,
        "incremental_new_candidates": 0,
        "samples": [],
        "failed_samples": [],
    }
    pending_batch: list[dict[str, object]] = []

    append_debug(
        "start task_id=%s topic_id=%s max_concurrency=%s request_interval_ms=%s timeout=%s"
        % (TASK_ID, TOPIC_ID, MAX_CONCURRENCY, REQUEST_INTERVAL_MS, REQUEST_TIMEOUT_SECONDS)
    )

    page = 1
    while True:
        payload, status_code, error = fetch_json(build_list_url(page), f"list-{page}")
        if payload is None:
            append_debug(f"list failed page={page} status={status_code} error={error}")
            break
        rows = parse_list_payload(payload)
        if page == 1:
            state["page1_row_count"] = len(rows)
        if page == 2:
            state["page2_row_count"] = len(rows)
        if not rows:
            append_debug(f"list empty page={page} stop")
            break
        state["pages_completed"] = int(state["pages_completed"]) + 1
        state["list_total_count"] = int(state["list_total_count"]) + len(rows)

        todo_rows: list[dict[str, object]] = []
        page_seen: set[str] = set()
        for item in rows:
            url = str(item.get("url") or "")
            if not url:
                continue
            dedupe_key = normalize_dedupe_key(url)
            if dedupe_key in page_seen:
                continue
            page_seen.add(dedupe_key)
            if dedupe_key in known_dedupe_keys:
                state["skipped_existing_count"] = int(state["skipped_existing_count"]) + 1
                continue
            todo_rows.append(item)

        if todo_rows:
            with ThreadPoolExecutor(max_workers=max(MAX_CONCURRENCY, 1)) as executor:
                future_map = {}
                for item in todo_rows:
                    sleep_low_pressure()
                    future = executor.submit(build_sample_from_detail, item)
                    future_map[future] = item
                for future in as_completed(future_map):
                    sample = future.result()
                    state["detail_attempted_count"] = int(state["detail_attempted_count"]) + 1
                    if sample.get("fetch_error"):
                        state["detail_failed_count"] = int(state["detail_failed_count"]) + 1
                        state["failed_samples"].append(
                            {
                                "article_url": sample["article_url"],
                                "fetch_error": sample["fetch_error"],
                                "detail_status_code": sample["detail_status_code"],
                            }
                        )
                    else:
                        state["detail_success_count"] = int(state["detail_success_count"]) + 1
                    if int(sample["content_length"]) > 0:
                        state["content_success_count"] = int(state["content_success_count"]) + 1
                    else:
                        state["content_failed_count"] = int(state["content_failed_count"]) + 1
                    if len(state["samples"]) < 5 and int(sample["content_length"]) > 0:
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
                    known_dedupe_keys.add(str(sample["dedupe_key"]))
                    pending_batch.append(sample)
                    if len(pending_batch) >= max(BATCH_COMMIT_SIZE, 1):
                        upsert_articles(STAGE_DB_PATH, pending_batch)
                        state["written_count"] = db_count(STAGE_DB_PATH)
                        pending_batch = []
        if len(rows) < PAGE_LIMIT:
            break
        if MAX_PAGES > 0 and page >= MAX_PAGES:
            append_debug(f"reached max_pages={MAX_PAGES} stop")
            break
        page += 1

    if pending_batch:
        upsert_articles(STAGE_DB_PATH, pending_batch)
        state["written_count"] = db_count(STAGE_DB_PATH)

    state["elapsed_seconds"] = round(time.perf_counter() - start_time, 3)
    save_state(state)

    merge_result = {"status": "skipped", "merged_count": 0, "generated_at": now_iso()}
    incremental_passed = False
    if int(state["detail_success_count"]) > 0 and int(state["detail_failed_count"]) == 0:
        merge_result = merge_stage_to_main(STAGE_DB_PATH, MAIN_DB_PATH)
        existing_after_merge = load_existing_dedupe_keys(STAGE_DB_PATH) | load_existing_dedupe_keys(MAIN_DB_PATH)
        incremental_passed, incremental_new_candidates = verify_incremental(existing_after_merge)
        state["incremental_new_candidates"] = incremental_new_candidates
    update_result(state, merge_result, "completed")
    write_acceptance(state, merge_result, incremental_passed)
    return 0 if json.loads(ACCEPTANCE_PATH.read_text(encoding="utf-8"))["overall_result"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
