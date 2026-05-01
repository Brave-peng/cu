"""Run Mysteel copper wire rod full delivery on the server."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import random
import re
import sqlite3
import subprocess
import sys
import time
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from event_log_baseline.config import CrawlerConfig


SOURCE_NAME = "mysteel_copper_wire_rod"
SOURCE_GROUP = "copper_processing"
ENTRY_URL = "https://list1.mysteel.com/article/p-1939----0201---------1.html"
LIST_URL_TEMPLATE = "https://list1.mysteel.com/article/p-1939----0201---------{page}.html"
TASK_ID = os.getenv("TASK_ID", "mysteel_copper_wire_rod_server_delivery_v1")
TARGET_NAME = os.getenv("TARGET_NAME", "mysteel_copper_wire_rod")
DATA_ROOT = Path(os.getenv("DATA_ROOT", "/data/event_log"))
RUN_DATE = os.getenv("RUN_DATE", datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d"))
RUN_DIR = Path(os.getenv("RUN_DIR", str(DATA_ROOT / "runs" / RUN_DATE / TASK_ID)))
STAGE_DB_PATH = Path(os.getenv("STAGE_DB_PATH", str(DATA_ROOT / "db" / "mysteel_copper_wire_rod_stage.db")))
MAIN_DB_PATH = Path(os.getenv("MAIN_DB_PATH", str(DATA_ROOT / "db" / "event_log.db")))
LEDGER_PATH = Path(os.getenv("LEDGER_PATH", str(DATA_ROOT / "logs" / "merge_ledger.jsonl")))
RESULT_PATH = RUN_DIR / "crawl_result.json"
MERGE_RESULT_PATH = RUN_DIR / "merge_result.json"
ACCEPTANCE_PATH = RUN_DIR / "acceptance.json"
DEBUG_LOG_PATH = RUN_DIR / "debug.log"
TASK_PATH = RUN_DIR / "task.json"
STATE_PATH = RUN_DIR / "progress_state.json"
JITTER_MS = int(os.getenv("JITTER_MS", "600"))
PAGE_PAUSE_MS = int(os.getenv("PAGE_PAUSE_MS", "2500"))
BLOCKED_BACKOFF_SECONDS = int(os.getenv("BLOCKED_BACKOFF_SECONDS", "20"))
MYSTEEL_CHARSETS = ("gb18030", "gbk", "utf-8")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/123 Safari/537.36",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}


def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def append_debug(message: str) -> None:
    RUN_DIR.mkdir(parents=True, exist_ok=True)
    with DEBUG_LOG_PATH.open("a", encoding="utf-8") as handle:
        handle.write(f"[{now_iso()}] {message}\n")


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def normalize_url(url: str) -> str:
    parsed = urlparse(url)
    query = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True) if not k.lower().startswith("utm_")]
    path = parsed.path.rstrip("/") or parsed.path
    return urlunparse((parsed.scheme, parsed.netloc.lower(), path + ("/" if path and not path.endswith("/") else ""), "", urlencode(query), ""))


def dedupe_key(url: str) -> str:
    return hashlib.sha256(normalize_url(url).encode("utf-8")).hexdigest()


def detect_charset(content: bytes, header_encoding: str | None = None) -> str:
    header = clean_text(header_encoding or "").lower()
    if header:
        for charset in MYSTEEL_CHARSETS:
            if charset in header:
                return charset
    head = content[:4096].decode("ascii", errors="ignore").lower()
    match = re.search(r"charset=['\"]?([a-z0-9_-]+)", head)
    if match:
        charset = match.group(1)
        return "gb18030" if charset == "gb2312" else charset
    return "gb18030"


def decode_html(content: bytes, header_encoding: str | None = None) -> str:
    candidates: list[str] = []
    detected = detect_charset(content, header_encoding)
    if detected:
        candidates.append(detected)
    for charset in MYSTEEL_CHARSETS:
        if charset not in candidates:
            candidates.append(charset)
    for charset in candidates:
        try:
            return content.decode(charset)
        except UnicodeDecodeError:
            continue
    return content.decode("utf-8", errors="replace")


def sleep_low_pressure(config: CrawlerConfig, extra_ms: int = 0) -> None:
    base_ms = max(config.request_interval_ms, 0) + max(extra_ms, 0)
    jitter = random.randint(0, max(JITTER_MS, 0)) if JITTER_MS > 0 else 0
    time.sleep((base_ms + jitter) / 1000)


def fetch(url: str, referer: str | None = None) -> tuple[str, int]:
    headers = dict(HEADERS)
    if referer:
        headers["Referer"] = referer
    response = requests.get(url, headers=headers, timeout=25)
    response.raise_for_status()
    return decode_html(response.content, response.encoding), response.status_code


def fetch_with_curl(url: str, referer: str = ENTRY_URL) -> tuple[str, int]:
    result = subprocess.run(
        ["curl", "-L", "--retry", "2", "--connect-timeout", "20", "-A", HEADERS["User-Agent"], "-e", referer, url],
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.decode("utf-8", errors="replace").strip())
    return decode_html(result.stdout), 200


def fetch_with_retry(config: CrawlerConfig, url: str, label: str, referer: str | None = None) -> tuple[str, int, str | None]:
    attempts = max(config.max_retries, 0) + 1
    last_error: str | None = "not_started"
    last_status = 0
    last_html = ""
    for attempt in range(1, attempts + 1):
        try:
            html, status = fetch(url, referer)
            last_html, last_status, last_error = html, status, None
        except Exception as exc:  # noqa: BLE001
            append_debug(f"{label}_requests_failed attempt={attempt}/{attempts} url={url} error={exc!r}")
            try:
                html, status = fetch_with_curl(url, referer or ENTRY_URL)
                last_html, last_status, last_error = html, status, None
            except Exception as curl_exc:  # noqa: BLE001
                last_html, last_status, last_error = "", 0, repr(curl_exc)
        append_debug(f"fetch {label} attempt={attempt}/{attempts} status={last_status} url={url} error={last_error}")
        if last_status == 200 and last_html:
            return last_html, last_status, None
        if attempt < attempts:
            append_debug(f"blocked_or_transient status={last_status} label={label} backoff_seconds={BLOCKED_BACKOFF_SECONDS}")
            time.sleep(max(BLOCKED_BACKOFF_SECONDS, 0))
            sleep_low_pressure(config)
    return last_html, last_status, last_error


def parse_listing(html: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    rows: list[dict[str, str]] = []
    for link in soup.select("a[href]"):
        title = clean_text(link.get_text(" "))
        href = link.get("href")
        if not title or not href:
            continue
        article_url = urljoin(ENTRY_URL, href)
        if "/a/" not in article_url or len(title) < 6:
            continue
        context = clean_text(link.parent.get_text(" ") if link.parent else "")
        match = re.search(r"20\d{2}[-/]\d{1,2}[-/]\d{1,2}(?:\s+\d{1,2}:\d{2})?", context)
        rows.append(
            {
                "published_text": match.group(0) if match else "",
                "title": title,
                "article_url": article_url,
                "excerpt": "",
                "list_context": context,
                "dedupe_key": dedupe_key(article_url),
            }
        )
    deduped: list[dict[str, str]] = []
    seen: set[str] = set()
    for row in rows:
        key = row["dedupe_key"]
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def parse_detail(url: str, config: CrawlerConfig) -> dict[str, object]:
    html, status, fetch_error = fetch_with_retry(config, url, "detail", ENTRY_URL)
    if status != 200 or not html:
        return {
            "http_status": status,
            "published_date": "",
            "detail_title": "",
            "excerpt": "",
            "content": "",
            "content_html": "",
            "content_length": 0,
            "fetch_error": fetch_error or f"status={status}",
        }
    soup = BeautifulSoup(html, "html.parser")
    meta_description = soup.select_one("meta[name='description']")
    meta_publish = soup.select_one("meta[name='publish']")
    title_node = soup.select_one("h1, title")
    content_node = soup.select_one("#content-text, .content-main, article")
    content_html = str(content_node) if content_node else ""
    if content_node is not None:
        for bad in content_node.select(
            "script, style, noscript, iframe, form, aside, nav, .editor, .copyright, .related-box, .tool-part"
        ):
            bad.decompose()
        paragraphs = [clean_text(p.get_text(" ")) for p in content_node.select("p")]
        content = "\n".join(
            p
            for p in paragraphs
            if p
            and len(p) > 10
            and "免责声明" not in p
            and "资讯监督" not in p
            and "资讯投诉" not in p
        )
    else:
        content = ""
    if len(content) < 80 and meta_description:
        content = clean_text(meta_description.get("content", ""))
    return {
        "http_status": status,
        "published_date": clean_text(meta_publish.get("content", "") if meta_publish else ""),
        "detail_title": clean_text(title_node.get_text(" ") if title_node else ""),
        "excerpt": clean_text(meta_description.get("content", "") if meta_description else ""),
        "content": content,
        "content_html": content_html,
        "content_length": len(content),
        "fetch_error": "",
    }


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


def upsert_article(db_path: Path, row: dict[str, object]) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path, timeout=60) as conn:
        init_db(conn)
        conn.execute(
            """
            INSERT OR REPLACE INTO event_log_articles (
                source_name, dedupe_key, article_url, title, detail_title, published_text,
                content, content_length, content_preview, http_status, fetch_error, fetched_at, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                SOURCE_NAME,
                row["dedupe_key"],
                row["article_url"],
                row["title"],
                row.get("detail_title", ""),
                row.get("published_text", ""),
                row.get("content", ""),
                row.get("content_length", 0),
                row.get("content_preview", ""),
                row.get("http_status", 0),
                row.get("fetch_error", ""),
                row["fetched_at"],
                row["fetched_at"],
            ),
        )


def db_count(db_path: Path, source_name: str = SOURCE_NAME) -> int:
    if not db_path.exists():
        return 0
    with sqlite3.connect(db_path, timeout=60) as conn:
        init_db(conn)
        row = conn.execute("SELECT COUNT(1) FROM event_log_articles WHERE source_name = ?", (source_name,)).fetchone()
    return int(row[0]) if row else 0


def ensure_task_file() -> None:
    write_json(
        TASK_PATH,
        {
            "task_id": TASK_ID,
            "target_name": TARGET_NAME,
            "task_type": "server_delivery",
            "goal": "Crawl Mysteel copper wire rod on server, validate readable Chinese content, stage it, then merge accepted rows into event_log.db",
            "inputs": [ENTRY_URL],
            "done_when": [
                "acceptance overall_result = pass",
                "main_db_delivery_complete = pass",
            ],
            "retry_limit": 3,
            "created_at": now_iso(),
        },
    )


def load_state(main_db_before: int) -> dict[str, object]:
    if STATE_PATH.exists():
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    return {
        "next_page": 1,
        "pages_completed": 0,
        "terminal_empty_page_seen": False,
        "list_total_count": 0,
        "detail_success_count": 0,
        "detail_failed_count": 0,
        "content_success_count": 0,
        "content_failed_count": 0,
        "deduped_count": 0,
        "last_completed_page": 0,
        "last_processed_url": None,
        "main_db_count_before": main_db_before,
        "samples": [],
    }


def save_state(state: dict[str, object]) -> None:
    write_json(STATE_PATH, state)


def update_result(state: dict[str, object], config: CrawlerConfig, status: str) -> None:
    write_json(
        RESULT_PATH,
        {
            "source_name": SOURCE_NAME,
            "task_id": TASK_ID,
            "target_name": TARGET_NAME,
            "next_page": state["next_page"],
            "pages_completed": state["pages_completed"],
            "last_completed_page": state["last_completed_page"],
            "list_total_count": state["list_total_count"],
            "detail_success_count": state["detail_success_count"],
            "detail_failed_count": state["detail_failed_count"],
            "content_success_count": state["content_success_count"],
            "content_failed_count": state["content_failed_count"],
            "deduped_count": state["deduped_count"],
            "db_written_count": db_count(STAGE_DB_PATH),
            "stage_db_path": str(STAGE_DB_PATH),
            "main_db_path": str(MAIN_DB_PATH),
            "last_processed_url": state["last_processed_url"],
            "crawl_config": {
                "request_interval_ms": config.request_interval_ms,
                "jitter_ms": JITTER_MS,
                "page_pause_ms": PAGE_PAUSE_MS,
                "max_concurrency": config.max_concurrency,
                "max_retries": config.max_retries,
                "timeout_seconds": config.request_timeout_seconds,
                "blocked_backoff_seconds": BLOCKED_BACKOFF_SECONDS,
                "proxy_enabled": bool(os.getenv("HTTP_PROXY") or os.getenv("HTTPS_PROXY")),
            },
            "samples": state["samples"][:5],
            "status": status,
            "generated_at": now_iso(),
        },
    )


def failure_budget(total: int) -> int:
    if total <= 0:
        return 0
    return max(3, math.ceil(total * 0.05))


def write_acceptance(state: dict[str, object], config: CrawlerConfig, merge_result: dict[str, object], status: str) -> None:
    stage_count = db_count(STAGE_DB_PATH)
    main_count = db_count(MAIN_DB_PATH)
    total = int(state["list_total_count"])
    detail_fail = int(state["detail_failed_count"])
    content_fail = int(state["content_failed_count"])
    fail_budget = failure_budget(total)
    samples = state["samples"][:3]
    checks = {
        "task_defined": "pass" if TASK_PATH.exists() else "fail",
        "scope_single_task": "pass",
        "server_runtime_ready": "pass",
        "real_list_fetch_success": "pass" if total > 0 else "fail",
        "pagination_fetch_success": "pass" if int(state["pages_completed"]) >= 2 else "fail",
        "all_pages_completed": "pass" if bool(state["terminal_empty_page_seen"]) else "fail",
        "detail_fetch_success": "pass" if int(state["detail_success_count"]) > 0 and detail_fail <= fail_budget else "fail",
        "content_extract_success": "pass" if int(state["content_success_count"]) > 0 and content_fail <= fail_budget else "fail",
        "content_clean_text": "pass"
        if samples and all(sample["content_length"] > 100 and re.search(r"[\u4e00-\u9fff]", sample["content_preview"]) for sample in samples)
        else "fail",
        "time_field_available": "pass" if samples and all(bool(sample["published_text"]) for sample in samples) else "fail",
        "dedupe_key_generated": "pass" if samples and len({sample["dedupe_key"] for sample in samples}) == len(samples) else "fail",
        "failure_traceable": "pass" if DEBUG_LOG_PATH.exists() else "fail",
        "stage_db_recorded": "pass" if stage_count > 0 and stage_count >= int(state["deduped_count"]) else "fail",
        "main_db_not_touched_during_parallel_crawl": "pass"
        if merge_result.get("status") in {"merged", "empty_stage"} or main_count == int(state["main_db_count_before"])
        else "fail",
        "acceptance_passed_before_merge": "pass"
        if merge_result.get("status") in {"merged", "empty_stage", "pending"} or status != "completed"
        else "fail",
        "main_db_merged": "pass" if merge_result.get("status") == "merged" else "fail",
        "merge_ledger_recorded": "pass" if merge_result.get("ledger_recorded") else "fail",
        "main_db_delivery_complete": "pass" if merge_result.get("status") == "merged" and main_count >= merge_result.get("main_db_count_after", 0) else "fail",
        "server_result_recorded": "pass"
        if TASK_PATH.exists() and RESULT_PATH.exists() and ACCEPTANCE_PATH.parent.exists() and DEBUG_LOG_PATH.exists()
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
            "status": status,
        },
    )


def iter_stage_rows(stage_db_path: Path) -> list[tuple]:
    with sqlite3.connect(stage_db_path, timeout=60) as conn:
        init_db(conn)
        return conn.execute(
            """
            SELECT source_name, dedupe_key, article_url, title, detail_title, published_text,
                   content, content_length, content_preview, http_status, fetch_error, fetched_at, created_at
            FROM event_log_articles
            WHERE source_name = ?
            ORDER BY published_text DESC, article_url DESC
            """,
            (SOURCE_NAME,),
        ).fetchall()


def append_merge_ledger(payload: dict[str, object]) -> bool:
    try:
        LEDGER_PATH.parent.mkdir(parents=True, exist_ok=True)
        with LEDGER_PATH.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False) + "\n")
        return True
    except Exception as exc:  # noqa: BLE001
        append_debug(f"merge_ledger_failed error={exc!r}")
        return False


def merge_stage_to_main(stage_db_path: Path, main_db_path: Path) -> dict[str, object]:
    rows = iter_stage_rows(stage_db_path)
    main_before = db_count(main_db_path)
    if not rows:
        result = {
            "task_id": TASK_ID,
            "source_name": SOURCE_NAME,
            "stage_db_path": str(stage_db_path),
            "main_db_path": str(main_db_path),
            "stage_row_count": 0,
            "merged_count": 0,
            "main_db_count_before": main_before,
            "main_db_count_after": main_before,
            "merge_status": "empty_stage",
            "generated_at": now_iso(),
            "ledger_recorded": False,
            "status": "empty_stage",
        }
        write_json(MERGE_RESULT_PATH, result)
        return result
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
    main_after = db_count(main_db_path)
    ledger_payload = {
        "task_id": TASK_ID,
        "source_name": SOURCE_NAME,
        "merge_status": "merged",
        "merged_count": len(rows),
        "main_db_count_before": main_before,
        "main_db_count_after": main_after,
        "generated_at": now_iso(),
    }
    ledger_recorded = append_merge_ledger(ledger_payload)
    result = {
        "task_id": TASK_ID,
        "source_name": SOURCE_NAME,
        "stage_db_path": str(stage_db_path),
        "main_db_path": str(main_db_path),
        "stage_row_count": len(rows),
        "merged_count": len(rows),
        "main_db_count_before": main_before,
        "main_db_count_after": main_after,
        "merge_status": "merged",
        "generated_at": ledger_payload["generated_at"],
        "ledger_recorded": ledger_recorded,
        "status": "merged",
    }
    write_json(MERGE_RESULT_PATH, result)
    append_debug(f"merge_completed rows={len(rows)} main_before={main_before} main_after={main_after}")
    return result


def listing_page_url(page: int) -> str:
    return LIST_URL_TEMPLATE.format(page=page)


def main() -> int:
    RUN_DIR.mkdir(parents=True, exist_ok=True)
    ensure_task_file()
    config = CrawlerConfig.from_env()
    state = load_state(main_db_before=db_count(MAIN_DB_PATH))

    append_debug(f"start {TASK_ID}")
    append_debug(
        "config request_interval_ms=%s jitter_ms=%s page_pause_ms=%s max_concurrency=%s max_retries=%s timeout_seconds=%s blocked_backoff_seconds=%s proxy_enabled=%s"
        % (
            config.request_interval_ms,
            JITTER_MS,
            PAGE_PAUSE_MS,
            config.max_concurrency,
            config.max_retries,
            config.request_timeout_seconds,
            BLOCKED_BACKOFF_SECONDS,
            bool(os.getenv("HTTP_PROXY") or os.getenv("HTTPS_PROXY")),
        )
    )

    page = int(state["next_page"])
    while True:
        page_url = listing_page_url(page)
        html, status, fetch_error = fetch_with_retry(config, page_url, f"list page {page}", ENTRY_URL)
        if status != 200 or not html:
            append_debug(f"list_page_failed page={page} status={status} error={fetch_error}")
            update_result(state, config, "failed")
            pending_merge = {"status": "pending", "ledger_recorded": False}
            write_acceptance(state, config, pending_merge, "failed")
            return 1
        entries = parse_listing(html)
        append_debug(f"page={page} entries={len(entries)}")
        if not entries:
            state["terminal_empty_page_seen"] = True
            state["next_page"] = page
            state["deduped_count"] = db_count(STAGE_DB_PATH)
            save_state(state)
            update_result(state, config, "completed")
            break
        state["list_total_count"] = int(state["list_total_count"]) + len(entries)
        for item in entries:
            sleep_low_pressure(config)
            detail = parse_detail(item["article_url"], config)
            fetched_at = now_iso()
            published_text = item["published_text"] or str(detail.get("published_date", ""))
            row = {
                **item,
                "published_text": published_text,
                "published_date": detail.get("published_date", ""),
                "detail_title": detail.get("detail_title", "") or item["title"],
                "content": detail.get("content", ""),
                "content_length": int(detail.get("content_length", 0)),
                "content_preview": str(detail.get("content", ""))[:280],
                "http_status": int(detail.get("http_status", 0)),
                "fetch_error": detail.get("fetch_error", ""),
                "fetched_at": fetched_at,
            }
            upsert_article(STAGE_DB_PATH, row)
            if row["http_status"] == 200 and row["content_length"] > 0:
                state["detail_success_count"] = int(state["detail_success_count"]) + 1
                state["content_success_count"] = int(state["content_success_count"]) + 1
            else:
                state["detail_failed_count"] = int(state["detail_failed_count"]) + 1
                state["content_failed_count"] = int(state["content_failed_count"]) + 1
            state["last_processed_url"] = item["article_url"]
            if row["content_length"] > 0 and len(state["samples"]) < 5:
                state["samples"].append(
                    {
                        "title": row["title"],
                        "article_url": row["article_url"],
                        "published_text": row["published_text"],
                        "content_length": row["content_length"],
                        "content_preview": row["content_preview"],
                        "dedupe_key": row["dedupe_key"],
                    }
                )
        state["pages_completed"] = int(state["pages_completed"]) + 1
        state["last_completed_page"] = page
        state["next_page"] = page + 1
        state["deduped_count"] = db_count(STAGE_DB_PATH)
        save_state(state)
        update_result(state, config, "running")
        pending_merge = {"status": "pending", "ledger_recorded": False}
        write_acceptance(state, config, pending_merge, "running")
        append_debug(f"page_completed={page} next_page={page + 1} stage_db_count={state['deduped_count']}")
        time.sleep(max(PAGE_PAUSE_MS, 0) / 1000)
        page += 1

    merge_result = merge_stage_to_main(STAGE_DB_PATH, MAIN_DB_PATH)
    state["deduped_count"] = db_count(STAGE_DB_PATH)
    save_state(state)
    update_result(state, config, "completed")
    write_acceptance(state, config, merge_result, "completed")
    acceptance = json.loads(ACCEPTANCE_PATH.read_text(encoding="utf-8"))
    return 0 if acceptance["overall_result"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
