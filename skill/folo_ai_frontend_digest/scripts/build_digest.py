#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
import textwrap
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


FOLO_CATEGORY_DEFAULT = "AI FrontEnd"
PAPER_FEED_TITLE = "cs.AI updates on arXiv.org"
ALLOWED_ARXIV_CATEGORIES = {"cs.LG", "cs.CL", "cs.SE"}
BLACKLIST_TERMS = [
    "clinical",
    "psychiatric",
    "lung cancer",
    "biomechanical",
    "traffic",
    "driving",
    "emboli",
    "field medicine",
    "legal",
    "graph",
]
NEW_HINTS = [
    "gui",
    "interface",
    "browser",
    "multimodal",
    "olfactory",
    "robotic",
    "edge",
    "ondevice",
    "deployment",
    "governance",
    "human-ai",
]
CLASSIC_HINTS = [
    "agent",
    "multi-agent",
    "reasoning",
    "retriev",
    "rag",
    "memory",
    "code",
    "coding",
    "safety",
    "alignment",
    "benchmark",
    "test generation",
    "inference",
    "distill",
    "spars",
]


@dataclass
class TimelineEntry:
    title: str
    url: str
    published_at: str
    description: str
    summary: str
    categories: List[str]
    feed_title: str
    feed_url: str
    feed_site_url: str
    raw: Dict[str, Any]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a Folo AI FrontEnd digest dataset.")
    parser.add_argument("--category", default=FOLO_CATEGORY_DEFAULT)
    parser.add_argument("--hours", type=int, default=24)
    parser.add_argument("--output-dir", default="output")
    parser.add_argument("--limit", type=int, default=100)
    return parser.parse_args()


def run_json_command(args: List[str]) -> Dict[str, Any]:
    last_error = ""
    for attempt in range(4):
        proc = subprocess.run(args, capture_output=True, text=True, encoding="utf-8")
        if proc.returncode == 0:
            try:
                return json.loads(proc.stdout)
            except json.JSONDecodeError as exc:
                raise RuntimeError(f"Failed to parse JSON from command: {' '.join(args)}\n{proc.stdout}") from exc
        last_error = proc.stderr or proc.stdout
        if "aborted" not in last_error.lower() or attempt == 3:
            break
        time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"Command failed: {' '.join(args)}\n{last_error}")


def resolve_executable(name: str) -> str:
    candidates = [name]
    if sys.platform.startswith("win"):
        candidates = [f"{name}.cmd", f"{name}.exe", name]
    for candidate in candidates:
        path = shutil.which(candidate)
        if path:
            return path
    raise RuntimeError(f"Could not find executable for {name}")


def parse_dt(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)


def fetch_timeline(category: str, hours: int, limit: int) -> List[TimelineEntry]:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    cursor: Optional[str] = None
    collected: List[TimelineEntry] = []

    while True:
        cmd = [
            resolve_executable("npx"),
            "--yes",
            "folocli@latest",
            "timeline",
            "--category",
            category,
            "--limit",
            str(limit),
            "--format",
            "json",
        ]
        if cursor:
            cmd.extend(["--cursor", cursor])

        payload = run_json_command(cmd)
        data = payload.get("data") or {}
        entries = data.get("entries") or []
        if not entries:
            break

        stop = False
        for item in entries:
            entry = item.get("entries") or {}
            published_at = entry.get("publishedAt")
            if not published_at:
                continue
            published_dt = parse_dt(published_at)
            if published_dt < cutoff:
                stop = True
                continue
            collected.append(
                TimelineEntry(
                    title=entry.get("title") or "",
                    url=entry.get("url") or "",
                    published_at=published_at,
                    description=entry.get("description") or "",
                    summary=entry.get("summary") or "",
                    categories=list(entry.get("categories") or []),
                    feed_title=((item.get("feeds") or {}).get("title") or ""),
                    feed_url=((item.get("feeds") or {}).get("url") or ""),
                    feed_site_url=((item.get("feeds") or {}).get("siteUrl") or ""),
                    raw=item,
                )
            )

        cursor = data.get("nextCursor")
        if stop or not data.get("hasNext") or not cursor:
            break
        time.sleep(0.2)

    return collected


def is_blacklisted(text: str) -> bool:
    lowered = text.lower()
    return any(term in lowered for term in BLACKLIST_TERMS)


def extract_arxiv_id(url: str) -> Optional[str]:
    match = re.search(r"arxiv\.org/abs/([^/?#]+)", url)
    return match.group(1) if match else None


def fetch_arxiv_abstracts(arxiv_ids: Iterable[str]) -> Dict[str, Dict[str, Any]]:
    ids = [x for x in arxiv_ids if x]
    if not ids:
        return {}

    output: Dict[str, Dict[str, Any]] = {}
    chunk_size = 10
    namespace = {"atom": "http://www.w3.org/2005/Atom"}

    for i in range(0, len(ids), chunk_size):
        chunk = ids[i : i + chunk_size]
        query = urllib.parse.urlencode({"id_list": ",".join(chunk)})
        url = f"https://export.arxiv.org/api/query?{query}"
        xml_text = ""
        chunk_loaded = False
        for attempt in range(5):
            try:
                with urllib.request.urlopen(url, timeout=30) as response:
                    xml_text = response.read().decode("utf-8")
                chunk_loaded = True
                break
            except urllib.error.HTTPError as exc:
                if exc.code != 429 or attempt == 4:
                    break
                # arXiv rate-limits bursts; back off so recurring jobs can recover.
                time.sleep(2 * (attempt + 1))
            except (TimeoutError, urllib.error.URLError):
                if attempt == 4:
                    break
                time.sleep(2 * (attempt + 1))
        if not chunk_loaded:
            continue
        root = ET.fromstring(xml_text)
        for entry in root.findall("atom:entry", namespace):
            entry_id = entry.findtext("atom:id", default="", namespaces=namespace)
            arxiv_id = entry_id.rsplit("/", 1)[-1]
            summary = entry.findtext("atom:summary", default="", namespaces=namespace).strip()
            title = entry.findtext("atom:title", default="", namespaces=namespace).strip()
            categories = [node.attrib.get("term", "") for node in entry.findall("atom:category", namespace)]
            output[arxiv_id] = {
                "arxiv_id": arxiv_id,
                "title": title,
                "abstract": " ".join(summary.split()),
                "categories": categories,
                "source": entry_id,
            }
        time.sleep(0.2)

    return output


def hint_bucket(text: str) -> str:
    lowered = text.lower()
    new_score = sum(1 for hint in NEW_HINTS if hint in lowered)
    classic_score = sum(1 for hint in CLASSIC_HINTS if hint in lowered)
    if new_score > classic_score:
        return "new"
    return "classic"


def build_digest_dataset(entries: List[TimelineEntry]) -> Dict[str, Any]:
    news: List[Dict[str, Any]] = []
    papers_raw: List[Dict[str, Any]] = []
    papers_kept: List[Dict[str, Any]] = []
    dropped: List[Dict[str, Any]] = []

    for item in entries:
        row = {
            "title": item.title,
            "url": item.url,
            "published_at": item.published_at,
            "description": item.description,
            "summary": item.summary,
            "categories": item.categories,
            "feed_title": item.feed_title,
            "feed_url": item.feed_url,
            "feed_site_url": item.feed_site_url,
        }
        if item.feed_title != PAPER_FEED_TITLE:
            row["source_link"] = item.feed_site_url or item.feed_url
            news.append(row)
            continue

        papers_raw.append(row)
        text_blob = " ".join([item.title, item.description, item.summary])
        if is_blacklisted(text_blob):
            row["drop_reason"] = "blacklist"
            dropped.append(row)
            continue
        if not set(item.categories).intersection(ALLOWED_ARXIV_CATEGORIES):
            row["drop_reason"] = "category"
            dropped.append(row)
            continue
        row["arxiv_id"] = extract_arxiv_id(item.url)
        papers_kept.append(row)

    abstracts = fetch_arxiv_abstracts([row.get("arxiv_id") for row in papers_kept if row.get("arxiv_id")])
    for row in papers_kept:
        arxiv_id = row.get("arxiv_id")
        abstract_data = abstracts.get(arxiv_id or "", {})
        row["abstract"] = abstract_data.get("abstract") or row.get("summary") or row.get("description") or ""
        row["abstract_title"] = abstract_data.get("title") or row["title"]
        row["api_categories"] = abstract_data.get("categories") or row["categories"]
        row["bucket_hint"] = hint_bucket(" ".join([row["title"], row["abstract"]]))

    papers_kept.sort(key=lambda x: x["published_at"], reverse=True)
    news.sort(key=lambda x: x["published_at"], reverse=True)
    dropped.sort(key=lambda x: x["published_at"], reverse=True)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "stats": {
            "total_entries": len(entries),
            "news_count": len(news),
            "paper_raw_count": len(papers_raw),
            "paper_candidate_count": len(papers_kept),
            "paper_dropped_count": len(dropped),
        },
        "rules": {
            "paper_feed_title": PAPER_FEED_TITLE,
            "allowed_categories": sorted(ALLOWED_ARXIV_CATEGORIES),
            "blacklist_terms": BLACKLIST_TERMS,
        },
        "news": news,
        "paper_candidates": papers_kept,
        "paper_dropped": dropped,
    }


def build_scaffold_markdown(data: Dict[str, Any], category: str, hours: int) -> str:
    stats = data["stats"]
    lines = [
        f"# {category} Digest Scaffold",
        "",
        f"- Window: past {hours} hours",
        f"- Total entries: {stats['total_entries']}",
        f"- News items: {stats['news_count']}",
        f"- Raw arXiv papers: {stats['paper_raw_count']}",
        f"- Filtered paper candidates: {stats['paper_candidate_count']}",
        "",
        "## News",
        "",
    ]
    if not data["news"]:
        lines.append("_No news items in this window._")
        lines.append("")
    else:
        for item in data["news"]:
            lines.extend(
                [
                    f"### {item['title']}",
                    f"- Original: {item['url']}",
                    f"- Source: {item['source_link']}",
                    "",
                ]
            )

    lines.extend(["## Paper Candidates", ""])
    for idx, item in enumerate(data["paper_candidates"], start=1):
        abstract = item.get("abstract", "")
        short = textwrap.shorten(abstract, width=240, placeholder="...")
        lines.extend(
            [
                f"### {idx}. {item['title']}",
                f"- Bucket hint: {item.get('bucket_hint', 'classic')}",
                f"- Categories: {', '.join(item.get('api_categories') or item.get('categories') or [])}",
                f"- Original: {item['url']}",
                f"- Abstract: {short}",
                "",
            ]
        )

    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    entries = fetch_timeline(category=args.category, hours=args.hours, limit=args.limit)
    dataset = build_digest_dataset(entries)
    scaffold = build_scaffold_markdown(dataset, category=args.category, hours=args.hours)

    json_path = output_dir / "digest-data.json"
    md_path = output_dir / "digest-scaffold.md"
    json_path.write_text(json.dumps(dataset, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(scaffold, encoding="utf-8")

    print(json.dumps({"json": str(json_path), "markdown": str(md_path), "stats": dataset["stats"]}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
