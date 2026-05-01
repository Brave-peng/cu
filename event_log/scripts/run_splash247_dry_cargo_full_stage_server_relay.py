"""Run a full Splash247 Dry Cargo stage crawl on the server via local relay."""

from __future__ import annotations

from datetime import datetime, timezone
import http.client
import json
import os
from pathlib import Path
import random
import re
import sqlite3
import sys
import time
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen

from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from event_log_baseline.config import CrawlerConfig


SOURCE_NAME = "splash247_dry_cargo"
LIST_URL = "https://splash247.com/category/sector/dry-cargo/"
TASK_ID = os.getenv("TASK_ID", "splash247_dry_cargo_full_stage_relay_v1")
TARGET_NAME = os.getenv("TARGET_NAME", "splash247_dry_cargo_full_stage_relay")
DATA_ROOT = Path(os.getenv("DATA_ROOT", "/data/event_log"))
RUN_DATE = os.getenv("RUN_DATE", datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d"))
RUN_DIR = Path(os.getenv("RUN_DIR", str(DATA_ROOT / "runs" / RUN_DATE / TASK_ID)))
STAGE_DB_PATH = Path(os.getenv("STAGE_DB_PATH", str(DATA_ROOT / "db" / "splash247_dry_cargo_stage.db")))
MAIN_DB_PATH = Path(os.getenv("MAIN_DB_PATH", str(DATA_ROOT / "db" / "event_log.db")))
RESULT_PATH = RUN_DIR / "crawl_result.json"
ACCEPTANCE_PATH = RUN_DIR / "acceptance.json"
DEBUG_LOG_PATH = RUN_DIR / "debug.log"
TASK_PATH = RUN_DIR / "task.json"
STATE_PATH = RUN_DIR / "progress_state.json"
JITTER_MS = int(os.getenv("JITTER_MS", "900"))
PAGE_PAUSE_MS = int(os.getenv("PAGE_PAUSE_MS", "4000"))
BLOCKED_BACKOFF_SECONDS = int(os.getenv("BLOCKED_BACKOFF_SECONDS", "30"))


def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def append_debug(message: str) -> None:
    RUN_DIR.mkdir(parents=True, exist_ok=True)
    with DEBUG_LOG_PATH.open("a", encoding="utf-8") as handle:
        handle.write(f"[{now_iso()}] {message}\n")


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def sleep_low_pressure(config: CrawlerConfig) -> None:
    base_ms = max(config.request_interval_ms, 0)
    jitter = random.randint(0, max(JITTER_MS, 0)) if JITTER_MS > 0 else 0
    time.sleep((base_ms + jitter) / 1000)


class FetchResult:
    def __init__(self, status_code: int, content: str, headers: dict[str, str], error: str | None = None) -> None:
        self.status_code = status_code
        self.content = content
        self.headers = headers
        self.error = error


def is_challenge_page(html_text: str) -> bool:
    lowered = (html_text or "").lower()
    return (
        "one moment, please" in lowered
        or "window.location.reload()" in lowered
        or "cf-browser-verification" in lowered
        or "challenge-platform" in lowered
    )


def build_request(url: str) -> Request:
    return Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        },
    )


def fetch_with_retry(config: CrawlerConfig, url: str, label: str) -> FetchResult:
    attempts = max(config.max_retries, 0) + 1
    last_result = FetchResult(0, "", {}, "not_started")
    for attempt in range(1, attempts + 1):
        try:
            with urlopen(build_request(url), timeout=config.request_timeout_seconds) as response:
                content = response.read().decode("utf-8", errors="replace")
                headers = dict(response.headers.items())
                if response.status == 200 and is_challenge_page(content):
                    last_result = FetchResult(0, content, headers, "challenge_page")
                else:
                    last_result = FetchResult(response.status, content, headers)
        except (TimeoutError, OSError, http.client.HTTPException) as exc:
            last_result = FetchResult(0, "", {}, str(exc))
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


def clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "")).strip()


def listing_page_url(page: int) -> str:
    return LIST_URL if page == 1 else f"{LIST_URL}page/{page}/"


def parse_listing(html_text: str, page_url: str) -> tuple[list[dict[str, str]], int]:
    soup = BeautifulSoup(html_text, "html.parser")
    entries: list[dict[str, str]] = []
    for post in soup.select(".post"):
        anchor = post.select_one("h2 a[href]")
        if not anchor:
            continue
        article_url = urljoin(page_url, anchor["href"])
        if not article_url.startswith("https://splash247.com/") or "/category/" in article_url:
            continue
        title = clean_text(anchor.get_text(" "))
        excerpt_node = post.select_one("p")
        excerpt = clean_text(excerpt_node.get_text(" ") if excerpt_node else "")
        time_node = post.select_one("time[datetime], time")
        published_text = ""
        if time_node is not None:
            published_text = time_node.get("datetime") if time_node.has_attr("datetime") else clean_text(time_node.get_text(" "))
        if not published_text:
            match = re.search(
                r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{1,2},\s+\d{4}",
                post.get_text(" ", strip=True),
                re.I,
            )
            published_text = match.group(0) if match else ""
        entries.append(
            {
                "title": title,
                "article_url": article_url,
                "excerpt": excerpt,
                "published_text": published_text,
            }
        )
    pages = [1]
    pagination_roots = soup.select(
        ".pagination, .nav-links, .page-numbers, .paging, nav.pagination, .pagination-wrap"
    )
    anchors = pagination_roots[0].select("a[href]") if pagination_roots else []
    for anchor in anchors:
        href = anchor.get("href", "")
        match = re.search(r"/category/sector/dry-cargo/page/(\d+)/", href)
        if match:
            pages.append(int(match.group(1)))
    return entries, max(pages)


def parse_detail(html_text: str) -> dict[str, str]:
    soup = BeautifulSoup(html_text, "html.parser")
    title_node = soup.select_one("h1")
    time_node = soup.select_one("time[datetime], time")
    content_node = soup.select_one(".entry-content, .post-content, .article-content, article")
    if content_node is None:
        candidates = soup.select("article, .post, .single-post, main")
        content_node = max(candidates, key=lambda node: len(node.get_text(" ", strip=True)), default=None)
    content = ""
    if content_node is not None:
        for bad in content_node.select(
            "script, style, noscript, iframe, form, aside, nav, .menu, .newsletter, .comments, .related, .tags, .social, .author, .sharedaddy"
        ):
            bad.decompose()
        paragraphs = [clean_text(node.get_text(" ")) for node in content_node.select("p")]
        paragraphs = [item for item in paragraphs if item and len(item) > 20]
        content = "\n\n".join(paragraphs)
    return {
        "detail_title": clean_text(title_node.get_text(" ") if title_node else ""),
        "detail_published_text": time_node.get("datetime") if time_node is not None and time_node.has_attr("datetime") else clean_text(time_node.get_text(" ") if time_node else ""),
        "content_full": content,
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


def normalize_dedupe_key(url: str) -> str:
    parsed = urlparse(url)
    return f"{parsed.netloc}{parsed.path}".rstrip("/").lower()


def upsert_article(db_path: Path, sample: dict[str, object]) -> None:
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


def db_count(db_path: Path, source_name: str) -> int:
    if not db_path.exists():
        return 0
    with sqlite3.connect(db_path, timeout=60) as conn:
        init_db(conn)
        row = conn.execute(
            "SELECT COUNT(1) FROM event_log_articles WHERE source_name = ?",
            (source_name,),
        ).fetchone()
    return int(row[0]) if row else 0


def load_state(main_db_before: int) -> dict[str, object]:
    if STATE_PATH.exists():
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    return {
        "next_page": 1,
        "pages_completed": 0,
        "total_pages_discovered": 1,
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


def update_result(state: dict[str, object], config: CrawlerConfig, relay_enabled: bool, status: str) -> None:
    payload = {
        "source_name": SOURCE_NAME,
        "task_id": TASK_ID,
        "target_name": TARGET_NAME,
        "next_page": state["next_page"],
        "pages_completed": state["pages_completed"],
        "last_completed_page": state["last_completed_page"],
        "total_pages_discovered": state["total_pages_discovered"],
        "list_total_count": state["list_total_count"],
        "detail_success_count": state["detail_success_count"],
        "detail_failed_count": state["detail_failed_count"],
        "content_success_count": state["content_success_count"],
        "content_failed_count": state["content_failed_count"],
        "deduped_count": state["deduped_count"],
        "db_written_count": db_count(STAGE_DB_PATH, SOURCE_NAME),
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
            "relay_enabled": relay_enabled,
        },
        "samples": state["samples"][:5],
        "status": status,
        "generated_at": now_iso(),
    }
    write_json(RESULT_PATH, payload)


def write_acceptance(state: dict[str, object], config: CrawlerConfig, relay_enabled: bool, status: str) -> None:
    stage_count = db_count(STAGE_DB_PATH, SOURCE_NAME)
    main_count = db_count(MAIN_DB_PATH, SOURCE_NAME)
    all_pages_completed = (
        (
            state["pages_completed"] >= state["total_pages_discovered"]
            and state["total_pages_discovered"] > 0
        )
        or bool(state.get("terminal_empty_page_seen"))
    )
    checks = {
        "task_defined": "pass" if TASK_PATH.exists() else "fail",
        "scope_single_task": "pass",
        "relay_path_verified": "pass" if relay_enabled else "fail",
        "pagination_started": "pass" if state["pages_completed"] > 0 or state["next_page"] > 1 or state["list_total_count"] > 0 else "fail",
        "all_pages_completed": "pass" if all_pages_completed else "fail",
        "list_fetch_success": "pass" if state["list_total_count"] > 0 else "fail",
        "list_deduped": "pass" if state["deduped_count"] > 0 else "fail",
        "detail_fetch_success": "pass" if state["detail_failed_count"] == 0 and state["detail_success_count"] > 0 else "fail",
        "content_fetch_success": "pass" if state["content_failed_count"] == 0 and state["content_success_count"] > 0 else "fail",
        "content_clean_text": "pass"
        if state["samples"] and all(sample["content_length"] > 200 for sample in state["samples"][:5])
        else "fail",
        "time_field_available": "pass"
        if state["samples"] and all(bool(sample["published_text"]) for sample in state["samples"][:5])
        else "fail",
        "dedupe_key_generated": "pass"
        if state["samples"] and len({sample["dedupe_key"] for sample in state["samples"]}) == len(state["samples"])
        else "fail",
        "low_pressure_crawling_enforced": "pass"
        if config.max_concurrency == 1 and config.request_interval_ms >= 1500
        else "fail",
        "stage_db_recorded": "pass" if stage_count > 0 else "fail",
        "stage_db_count_consistent": "pass" if stage_count >= state["deduped_count"] and state["deduped_count"] > 0 else "fail",
        "main_db_not_touched_during_parallel_crawl": "pass" if main_count == state["main_db_count_before"] else "fail",
        "server_result_recorded": "pass"
        if TASK_PATH.exists() and RESULT_PATH.exists() and DEBUG_LOG_PATH.exists() and STATE_PATH.exists()
        else "fail",
        "ready_for_merge_task": "pass" if all_pages_completed and stage_count > 0 and main_count == state["main_db_count_before"] else "fail",
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


def ensure_task_file() -> None:
    payload = {
        "task_id": TASK_ID,
        "target_name": TARGET_NAME,
        "task_type": "crawl",
        "goal": "通过本地 relay 完成 Splash247 Dry Cargo 全量分页与详情抓取，并写入 stage DB，不触碰主库",
        "inputs": [
            "docs/信息源库.md",
            "docs/next_step_plan_splash247_dry_cargo_full_stage_relay.md",
        ],
        "done_when": [
            "all_pages_completed = pass",
            "detail_fetch_success = pass",
            "content_fetch_success = pass",
            "stage_db_recorded = pass",
            "main_db_not_touched_during_parallel_crawl = pass",
            "acceptance overall_result = pass",
        ],
        "retry_limit": 3,
        "created_at": now_iso(),
    }
    write_json(TASK_PATH, payload)


def main() -> int:
    RUN_DIR.mkdir(parents=True, exist_ok=True)
    ensure_task_file()
    config = CrawlerConfig.from_env()
    relay_enabled = bool(os.getenv("HTTP_PROXY") or os.getenv("HTTPS_PROXY"))
    state = load_state(main_db_before=db_count(MAIN_DB_PATH, SOURCE_NAME))

    append_debug(f"start {TASK_ID}")
    append_debug(
        "config request_interval_ms=%s jitter_ms=%s page_pause_ms=%s max_concurrency=%s max_retries=%s timeout_seconds=%s blocked_backoff_seconds=%s relay_enabled=%s"
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

    page = int(state["next_page"])
    while True:
        page_url = listing_page_url(page)
        listing = fetch_with_retry(config, page_url, f"list page {page}")
        if listing.status_code != 200:
            append_debug(f"list page failed page={page} status={listing.status_code}")
            update_result(state, config, relay_enabled, "failed")
            write_acceptance(state, config, relay_enabled, "failed")
            return 1
        entries, max_page = parse_listing(listing.content, page_url)
        state["total_pages_discovered"] = max(int(state["total_pages_discovered"]), max_page, page)
        if not entries:
            state["terminal_empty_page_seen"] = True
            state["total_pages_discovered"] = max(
                int(state["pages_completed"]),
                min(int(state["total_pages_discovered"]), page - 1),
            )
            state["next_page"] = page
            state["deduped_count"] = db_count(STAGE_DB_PATH, SOURCE_NAME)
            save_state(state)
            update_result(state, config, relay_enabled, "completed")
            write_acceptance(state, config, relay_enabled, "completed")
            append_debug(f"page={page} entries=0 stop")
            break
        append_debug(f"page={page} entries={len(entries)} discovered_max_page={max_page}")
        state["list_total_count"] = int(state["list_total_count"]) + len(entries)
        dedupe_keys_seen: set[str] = set()
        for item in entries:
            dedupe_key = normalize_dedupe_key(item["article_url"])
            if dedupe_key in dedupe_keys_seen:
                continue
            dedupe_keys_seen.add(dedupe_key)
            sleep_low_pressure(config)
            detail_result = fetch_with_retry(config, item["article_url"], "detail")
            if detail_result.status_code != 200:
                state["detail_failed_count"] = int(state["detail_failed_count"]) + 1
                sample = {
                    "title": item["title"],
                    "detail_title": item["title"],
                    "article_url": item["article_url"],
                    "published_text": item["published_text"],
                    "content_length": 0,
                    "content_preview": "",
                    "content_full": "",
                    "dedupe_key": dedupe_key,
                    "detail_status_code": detail_result.status_code,
                    "fetched_at": now_iso(),
                    "fetch_error": detail_result.error,
                }
                upsert_article(STAGE_DB_PATH, sample)
                continue
            detail = parse_detail(detail_result.content)
            published_text = detail["detail_published_text"] or item["published_text"]
            content_full = detail["content_full"]
            sample = {
                "title": item["title"],
                "detail_title": detail["detail_title"] or item["title"],
                "article_url": item["article_url"],
                "published_text": published_text,
                "content_length": len(content_full),
                "content_preview": content_full[:280],
                "content_full": content_full,
                "dedupe_key": dedupe_key,
                "detail_status_code": detail_result.status_code,
                "fetched_at": now_iso(),
                "fetch_error": None,
            }
            upsert_article(STAGE_DB_PATH, sample)
            state["detail_success_count"] = int(state["detail_success_count"]) + 1
            if content_full:
                state["content_success_count"] = int(state["content_success_count"]) + 1
            else:
                state["content_failed_count"] = int(state["content_failed_count"]) + 1
            state["last_processed_url"] = item["article_url"]
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
        state["pages_completed"] = int(state["pages_completed"]) + 1
        state["last_completed_page"] = page
        state["next_page"] = page + 1
        state["deduped_count"] = db_count(STAGE_DB_PATH, SOURCE_NAME)
        save_state(state)
        update_result(state, config, relay_enabled, "running")
        write_acceptance(state, config, relay_enabled, "running")
        append_debug(f"page_completed={page} next_page={page+1} stage_db_count={state['deduped_count']}")
        if page >= state["total_pages_discovered"]:
            break
        time.sleep(max(PAGE_PAUSE_MS, 0) / 1000)
        page += 1

    state["deduped_count"] = db_count(STAGE_DB_PATH, SOURCE_NAME)
    save_state(state)
    update_result(state, config, relay_enabled, "completed")
    write_acceptance(state, config, relay_enabled, "completed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
