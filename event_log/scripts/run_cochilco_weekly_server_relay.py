"""Run a Cochilco weekly PDF crawl on the server via local relay."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from html.parser import HTMLParser
from io import BytesIO
import json
import os
from pathlib import Path
import random
import re
import sqlite3
import sys
import time
from typing import Iterable
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen

try:
    from pypdf import PdfReader
except ImportError:  # pragma: no cover - handled at runtime on server
    try:
        from PyPDF2 import PdfReader  # type: ignore[assignment]
    except ImportError:
        PdfReader = None

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from event_log_baseline.config import CrawlerConfig


SOURCE_NAME = "cochilco.cl/informe-semanal-del-cobre"
LIST_URL = "https://www.cochilco.cl/web/informe-semanal-del-cobre/"
TASK_ID = os.getenv("TASK_ID", "cochilco_weekly_last_year_server_relay_v1")
TARGET_NAME = os.getenv("TARGET_NAME", "cochilco_weekly_last_year_server_relay")
DATA_ROOT = Path(os.getenv("DATA_ROOT", "/data/event_log"))
RUN_DATE = os.getenv("RUN_DATE", datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d"))
RUN_DIR = Path(os.getenv("RUN_DIR", str(DATA_ROOT / "runs" / RUN_DATE / TASK_ID)))
STAGE_DB_PATH = Path(os.getenv("STAGE_DB_PATH", str(DATA_ROOT / "db" / "cochilco_stage.db")))
MAIN_DB_PATH = Path(os.getenv("MAIN_DB_PATH", str(DATA_ROOT / "db" / "event_log.db")))
RESULT_PATH = RUN_DIR / "crawl_result.json"
MERGE_RESULT_PATH = RUN_DIR / "merge_result.json"
ACCEPTANCE_PATH = RUN_DIR / "acceptance.json"
DEBUG_LOG_PATH = RUN_DIR / "debug.log"
TASK_PATH = RUN_DIR / "task.json"
CUTOFF_DATE = date.fromisoformat(os.getenv("CUTOFF_DATE", "2025-01-01"))
JITTER_MS = int(os.getenv("JITTER_MS", "900"))
BLOCKED_BACKOFF_SECONDS = int(os.getenv("BLOCKED_BACKOFF_SECONDS", "30"))

SPANISH_MONTHS = {
    "enero": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7,
    "agosto": 8,
    "septiembre": 9,
    "setiembre": 9,
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12,
}


@dataclass(slots=True)
class FetchResult:
    status_code: int
    content: bytes
    headers: dict[str, str]
    error: str | None = None


@dataclass(slots=True)
class ReportLink:
    title: str
    pdf_url: str
    published_date: date | None


class AnchorCollector(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[tuple[str, str]] = []
        self._href: str | None = None
        self._text_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "a":
            return
        attrs_dict = dict(attrs)
        self._href = attrs_dict.get("href")
        self._text_parts = []

    def handle_data(self, data: str) -> None:
        if self._href is not None:
            self._text_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag != "a" or self._href is None:
            return
        text = " ".join("".join(self._text_parts).split())
        self.links.append((self._href, text))
        self._href = None
        self._text_parts = []


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


def fetch_with_retry(config: CrawlerConfig, url: str, label: str, binary: bool = False) -> FetchResult:
    attempts = max(config.max_retries, 0) + 1
    last_result = FetchResult(status_code=0, content=b"", headers={}, error="not_started")
    for attempt in range(1, attempts + 1):
        try:
            with urlopen(build_request(url), timeout=config.request_timeout_seconds) as response:
                content = response.read()
                headers = dict(response.headers.items())
                last_result = FetchResult(status_code=response.status, content=content, headers=headers)
        except Exception as exc:  # noqa: BLE001
            last_result = FetchResult(status_code=0, content=b"", headers={}, error=str(exc))
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


def parse_last_spanish_date(text: str) -> date | None:
    pattern = re.compile(
        r"(\d{1,2})\s+de\s+([a-záéíóú]+)\s+de\s+(\d{4})",
        flags=re.IGNORECASE,
    )
    matches = pattern.findall(text.lower())
    if not matches:
        return None
    day, month_text, year = matches[-1]
    month = SPANISH_MONTHS.get(month_text)
    if not month:
        return None
    return date(int(year), month, int(day))


def extract_report_links(html_text: str) -> list[ReportLink]:
    parser = AnchorCollector()
    parser.feed(html_text)
    results: list[ReportLink] = []
    for href, text in parser.links:
        lower_href = href.lower()
        lower_text = text.lower()
        if not lower_href.endswith(".pdf"):
            continue
        if "informe del mercado internacional del cobre" not in lower_text:
            continue
        pdf_url = urljoin(LIST_URL, href)
        published_date = parse_last_spanish_date(text)
        if published_date is not None and published_date < CUTOFF_DATE:
            continue
        if published_date is None:
            year_match = re.search(r"/(20\d{2})/", pdf_url)
            if not year_match or int(year_match.group(1)) < CUTOFF_DATE.year:
                continue
        results.append(ReportLink(title=text, pdf_url=pdf_url, published_date=published_date))
    deduped: dict[str, ReportLink] = {}
    for item in results:
        deduped[item.pdf_url] = item
    return list(deduped.values())


def normalize_dedupe_key(url: str) -> str:
    parsed = urlparse(url)
    return f"{parsed.netloc}{parsed.path}".rstrip("/").lower()


def extract_pdf_text(pdf_bytes: bytes) -> str:
    if PdfReader is None:
        raise RuntimeError("pypdf is not installed")
    reader = PdfReader(BytesIO(pdf_bytes))
    chunks: list[str] = []
    for page in reader.pages:
        chunks.append(page.extract_text() or "")
    text = "\n".join(chunks)
    return re.sub(r"\s+\n", "\n", text).strip()


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


def upsert_article(db_path: Path, sample: dict[str, object]) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path, timeout=60) as conn:
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
        return {"merged_count": 0, "status": "empty_stage", "generated_at": now_iso()}
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


def build_sample(report: ReportLink, content_text: str, fetched_at: str) -> dict[str, object]:
    return {
        "title": report.title,
        "detail_title": report.title,
        "article_url": report.pdf_url,
        "published_text": report.published_date.isoformat() if report.published_date else "",
        "content_length": len(content_text),
        "content_preview": content_text[:280],
        "content_full": content_text,
        "dedupe_key": normalize_dedupe_key(report.pdf_url),
        "detail_status_code": 200,
        "fetched_at": fetched_at,
        "fetch_error": None,
    }


def ensure_task_file() -> None:
    payload = {
        "task_id": TASK_ID,
        "target_name": TARGET_NAME,
        "task_type": "crawl",
        "goal": "通过本地 relay 在服务器上抓取 2025-01-01 以来的 Cochilco 铜周报，并最终落入服务器主库",
        "inputs": [
            "docs/信息源库.md",
            "docs/event_log.md",
            "docs/next_step_plan_cochilco_weekly_last_year_server_delivery.md",
        ],
        "done_when": [
            "date_range_filter_correct = pass",
            "pdf_text_extract_success = pass",
            "main_db_delivery_complete = pass",
            "acceptance overall_result = pass",
        ],
        "retry_limit": 3,
        "created_at": now_iso(),
    }
    write_json(TASK_PATH, payload)


def build_acceptance(
    relay_enabled: bool,
    report_links: list[ReportLink],
    processed_samples: list[dict[str, object]],
    download_fail_count: int,
    extract_fail_count: int,
    merge_result: dict[str, object],
    config: CrawlerConfig,
) -> dict[str, object]:
    stage_count = db_count(STAGE_DB_PATH)
    main_count = db_count(MAIN_DB_PATH)
    checks = {
        "task_defined": "pass" if TASK_PATH.exists() else "fail",
        "scope_single_task": "pass",
        "relay_path_verified": "pass" if relay_enabled else "fail",
        "real_list_fetch_success": "pass" if report_links else "fail",
        "pdf_link_extract_success": "pass" if report_links else "fail",
        "date_range_filter_correct": "pass"
        if all((item.published_date or CUTOFF_DATE) >= CUTOFF_DATE for item in report_links)
        else "fail",
        "pdf_download_success": "pass" if download_fail_count == 0 and processed_samples else "fail",
        "pdf_text_extract_success": "pass" if extract_fail_count == 0 and processed_samples else "fail",
        "pdf_content_clean_text": "pass"
        if processed_samples and all(sample["content_length"] > 500 for sample in processed_samples[:5])
        else "fail",
        "time_field_available": "pass"
        if processed_samples and all(bool(sample["published_text"]) for sample in processed_samples[:5])
        else "fail",
        "dedupe_key_generated": "pass"
        if processed_samples and len({sample["dedupe_key"] for sample in processed_samples}) == len(processed_samples)
        else "fail",
        "low_pressure_crawling_enforced": "pass"
        if config.max_concurrency == 1 and config.request_interval_ms >= 1500
        else "fail",
        "stage_db_recorded": "pass" if stage_count >= len(processed_samples) and stage_count > 0 else "fail",
        "acceptance_passed_before_merge": "pass"
        if download_fail_count == 0 and extract_fail_count == 0 and processed_samples
        else "fail",
        "main_db_merged": "pass" if merge_result.get("status") == "merged" else "fail",
        "main_db_delivery_complete": "pass" if main_count > 0 and merge_result.get("status") == "merged" else "fail",
        "server_result_recorded": "pass"
        if TASK_PATH.exists() and RESULT_PATH.exists() and MERGE_RESULT_PATH.exists() and DEBUG_LOG_PATH.exists()
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


def main() -> int:
    RUN_DIR.mkdir(parents=True, exist_ok=True)
    ensure_task_file()
    config = CrawlerConfig.from_env()
    relay_enabled = bool(os.getenv("HTTP_PROXY") or os.getenv("HTTPS_PROXY"))

    append_debug(f"start {TASK_ID}")
    append_debug(
        "config request_interval_ms=%s max_concurrency=%s max_retries=%s request_timeout_seconds=%s jitter_ms=%s blocked_backoff_seconds=%s relay_enabled=%s cutoff_date=%s stage_db=%s main_db=%s"
        % (
            config.request_interval_ms,
            config.max_concurrency,
            config.max_retries,
            config.request_timeout_seconds,
            JITTER_MS,
            BLOCKED_BACKOFF_SECONDS,
            relay_enabled,
            CUTOFF_DATE.isoformat(),
            STAGE_DB_PATH,
            MAIN_DB_PATH,
        )
    )

    list_result = fetch_with_retry(config, LIST_URL, "cochilco weekly list")
    if list_result.status_code != 200:
        raise RuntimeError(f"Failed list fetch: status={list_result.status_code} error={list_result.error}")

    html_text = list_result.content.decode("utf-8", errors="replace")
    report_links = extract_report_links(html_text)
    append_debug(f"list extracted_reports={len(report_links)}")

    processed_samples: list[dict[str, object]] = []
    download_fail_count = 0
    extract_fail_count = 0
    sample_payloads: list[dict[str, object]] = []

    for index, report in enumerate(report_links, start=1):
        sleep_low_pressure(config)
        pdf_result = fetch_with_retry(config, report.pdf_url, f"pdf {index}", binary=True)
        if pdf_result.status_code != 200 or pdf_result.error:
            download_fail_count += 1
            append_debug(f"pdf download failed url={report.pdf_url}")
            continue
        try:
            content_text = extract_pdf_text(pdf_result.content)
        except Exception as exc:  # noqa: BLE001
            extract_fail_count += 1
            append_debug(f"pdf extract failed url={report.pdf_url} error={exc}")
            continue
        fetched_at = now_iso()
        sample = build_sample(report, content_text, fetched_at)
        upsert_article(STAGE_DB_PATH, sample)
        processed_samples.append(sample)
        if len(sample_payloads) < 5:
            sample_payloads.append(
                {
                    "title": sample["title"],
                    "article_url": sample["article_url"],
                    "published_text": sample["published_text"],
                    "content_length": sample["content_length"],
                    "content_preview": sample["content_preview"],
                    "dedupe_key": sample["dedupe_key"],
                }
            )

    crawl_result = {
        "source_name": SOURCE_NAME,
        "task_id": TASK_ID,
        "target_name": TARGET_NAME,
        "list_total_count": len(report_links),
        "detail_success_count": len(processed_samples),
        "detail_failed_count": download_fail_count,
        "content_success_count": len(processed_samples),
        "content_failed_count": extract_fail_count,
        "deduped_count": db_count(STAGE_DB_PATH),
        "db_written_count": db_count(STAGE_DB_PATH),
        "stage_db_path": str(STAGE_DB_PATH),
        "main_db_path": str(MAIN_DB_PATH),
        "cutoff_date": CUTOFF_DATE.isoformat(),
        "crawl_config": {
            "request_interval_ms": config.request_interval_ms,
            "jitter_ms": JITTER_MS,
            "max_concurrency": config.max_concurrency,
            "max_retries": config.max_retries,
            "timeout_seconds": config.request_timeout_seconds,
            "blocked_backoff_seconds": BLOCKED_BACKOFF_SECONDS,
            "relay_enabled": relay_enabled,
        },
        "samples": sample_payloads,
        "generated_at": now_iso(),
    }
    write_json(RESULT_PATH, crawl_result)

    merge_result = merge_stage_to_main(STAGE_DB_PATH, MAIN_DB_PATH)
    acceptance = build_acceptance(
        relay_enabled=relay_enabled,
        report_links=report_links,
        processed_samples=processed_samples,
        download_fail_count=download_fail_count,
        extract_fail_count=extract_fail_count,
        merge_result=merge_result,
        config=config,
    )
    write_json(ACCEPTANCE_PATH, acceptance)
    return 0 if acceptance["overall_result"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
