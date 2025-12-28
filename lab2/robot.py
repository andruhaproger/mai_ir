import hashlib
import re
import sqlite3
import sys
import time
from dataclasses import dataclass
from typing import List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

import requests
import yaml
from bs4 import BeautifulSoup
from tqdm import tqdm


def normalize_url(url: str) -> str:
    url = (url or "").strip()
    parts = urlsplit(url)

    scheme = (parts.scheme or "http").lower()
    host = (parts.hostname or "").lower()
    port = parts.port

    if not host:
        return ""

    netloc = host
    if port and not ((scheme == "http" and port == 80) or (scheme == "https" and port == 443)):
        netloc = f"{host}:{port}"

    path = parts.path or "/"
    path = re.sub(r"/{2,}", "/", path)
    if path != "/" and path.endswith("/"):
        path = path[:-1]

    query_pairs = parse_qsl(parts.query, keep_blank_values=True)
    query_pairs.sort(key=lambda x: (x[0], x[1]))
    query = urlencode(query_pairs, doseq=True)

    return urlunsplit((scheme, netloc, path, query, ""))


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


SCHEMA = """
CREATE TABLE IF NOT EXISTS docs (
  url TEXT PRIMARY KEY,
  source TEXT NOT NULL,
  html TEXT NOT NULL,
  fetched_at INTEGER NOT NULL,
  last_checked_at INTEGER NOT NULL,
  etag TEXT,
  last_modified TEXT,
  content_hash TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS frontier (
  url TEXT PRIMARY KEY,
  source TEXT NOT NULL,
  next_fetch_at INTEGER NOT NULL,
  depth INTEGER NOT NULL,
  discovered_at INTEGER NOT NULL,
  last_error TEXT
);

CREATE INDEX IF NOT EXISTS idx_frontier_next_fetch ON frontier(next_fetch_at);
"""


class SQLiteStore:
    def __init__(self, path: str, commit_every: int = 100):
        self.path = path
        self.conn = sqlite3.connect(self.path)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.executescript(SCHEMA)
        self.conn.commit()
        self._ops = 0
        self.commit_every = max(1, int(commit_every))
        self.frontier_size = int(self.conn.execute("SELECT COUNT(*) FROM frontier").fetchone()[0])

    def close(self):
        self.conn.commit()
        self.conn.close()

    def _maybe_commit(self):
        self._ops += 1
        if self._ops >= self.commit_every:
            self.conn.commit()
            self._ops = 0

    def _frontier_exists(self, url: str) -> bool:
        row = self.conn.execute("SELECT 1 FROM frontier WHERE url=? LIMIT 1", (url,)).fetchone()
        return row is not None

    def upsert_frontier(self, url: str, source: str, next_fetch_at: int, depth: int, max_frontier: int):
        is_new = not self._frontier_exists(url)
        if is_new and max_frontier > 0 and self.frontier_size >= max_frontier:
            return

        now = int(time.time())
        self.conn.execute(
            """
            INSERT INTO frontier(url, source, next_fetch_at, depth, discovered_at, last_error)
            VALUES(?,?,?,?,?,NULL)
            ON CONFLICT(url) DO UPDATE SET
              next_fetch_at = MIN(frontier.next_fetch_at, excluded.next_fetch_at),
              source = excluded.source
            """,
            (url, source, int(next_fetch_at), int(depth), now),
        )
        if is_new:
            self.frontier_size += 1
        self._maybe_commit()

    def pop_next_url(self) -> Optional[Tuple[str, str, int, int]]:
        row = self.conn.execute(
            """
            SELECT url, source, depth, next_fetch_at
            FROM frontier
            ORDER BY next_fetch_at ASC
            LIMIT 1
            """
        ).fetchone()
        if not row:
            return None
        url, source, depth, next_fetch_at = row
        self.conn.execute("DELETE FROM frontier WHERE url=?", (url,))
        self.frontier_size = max(0, self.frontier_size - 1)
        self._maybe_commit()
        return url, source, int(depth), int(next_fetch_at)

    def set_frontier_error(self, url: str, source: str, depth: int, err: str, retry_in_seconds: int = 3600):
        is_new = not self._frontier_exists(url)

        next_fetch = int(time.time()) + int(retry_in_seconds)
        now = int(time.time())
        self.conn.execute(
            """
            INSERT INTO frontier(url, source, next_fetch_at, depth, discovered_at, last_error)
            VALUES(?,?,?,?,?,?)
            ON CONFLICT(url) DO UPDATE SET
              next_fetch_at = excluded.next_fetch_at,
              last_error = excluded.last_error
            """,
            (url, source, next_fetch, int(depth), now, (err or "")[:500]),
        )
        if is_new:
            self.frontier_size += 1
        self._maybe_commit()

    def get_doc_meta(self, url: str) -> Optional[Tuple[Optional[str], Optional[str], str]]:
        row = self.conn.execute(
            "SELECT etag, last_modified, content_hash FROM docs WHERE url=?",
            (url,),
        ).fetchone()
        return row if row else None

    def upsert_doc(
        self,
        url: str,
        source: str,
        html: str,
        fetched_at: int,
        last_checked_at: int,
        etag: Optional[str],
        last_modified: Optional[str],
        content_hash: str,
    ):
        self.conn.execute(
            """
            INSERT INTO docs(url, source, html, fetched_at, last_checked_at, etag, last_modified, content_hash)
            VALUES(?,?,?,?,?,?,?,?)
            ON CONFLICT(url) DO UPDATE SET
              source=excluded.source,
              html=excluded.html,
              fetched_at=excluded.fetched_at,
              last_checked_at=excluded.last_checked_at,
              etag=excluded.etag,
              last_modified=excluded.last_modified,
              content_hash=excluded.content_hash
            """,
            (url, source, html, int(fetched_at), int(last_checked_at), etag, last_modified, content_hash),
        )
        self._maybe_commit()

    def mark_checked_nochange(self, url: str):
        now = int(time.time())
        self.conn.execute("UPDATE docs SET last_checked_at=? WHERE url=?", (now, url))
        self._maybe_commit()

    def docs_count(self) -> int:
        return int(self.conn.execute("SELECT COUNT(*) FROM docs").fetchone()[0])


@dataclass
class SourceCfg:
    name: str
    seeds: List[str]
    allowed_domains: List[str]
    allow_prefixes: List[str]


def allowed(url: str, scfg: SourceCfg) -> bool:
    if not url:
        return False
    p = urlsplit(url)
    host = (p.hostname or "").lower()
    if p.scheme not in ("http", "https"):
        return False
    if host not in [d.lower() for d in scfg.allowed_domains]:
        return False
    if scfg.allow_prefixes and not any(url.startswith(pref) for pref in scfg.allow_prefixes):
        return False
    if host.endswith("wikipedia.org") and "/wiki/" in url:
        tail = url.split("/wiki/", 1)[1]
        if ":" in tail:
            return False
    return True


def extract_links(base_url: str, html: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    links = []
    for a in soup.select("a[href]"):
        href = a.get("href")
        if not href:
            continue
        if href.startswith("mailto:") or href.startswith("javascript:"):
            continue
        links.append(urljoin(base_url, href))
    return links


def fetch_url(session: requests.Session, url: str, prev_meta: Optional[Tuple[Optional[str], Optional[str], str]]):
    headers = {}
    if prev_meta:
        etag, last_mod, _ = prev_meta
        if etag:
            headers["If-None-Match"] = etag
        if last_mod:
            headers["If-Modified-Since"] = last_mod

    r = session.get(url, headers=headers, timeout=30)
    status = r.status_code

    if status == 304:
        return "", None, None, None, status, False

    r.raise_for_status()
    html_bytes = r.content
    html_text = r.text

    etag = r.headers.get("ETag")
    last_modified = r.headers.get("Last-Modified")

    new_hash = sha256_bytes(html_bytes)
    if prev_meta:
        _, _, old_hash = prev_meta
        if old_hash == new_hash:
            return "", etag, last_modified, new_hash, status, False

    return html_text, etag, last_modified, new_hash, status, True


def run(config_path: str):
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    db_cfg = cfg["db"]
    logic = cfg["logic"]

    store = SQLiteStore(db_cfg.get("path", "robot.db"), commit_every=int(logic.get("commit_every", 100)))

    delay = float(logic.get("delay_seconds", 0.25))
    max_pages = int(logic.get("max_pages", 1000))
    recrawl_after = int(logic.get("recrawl_after_seconds", 7 * 24 * 3600))
    ua = logic.get("user_agent", "MAI-IR-Lab02-Robot/1.0")
    max_links_per_page = int(logic.get("max_links_per_page", 200))
    max_frontier = int(logic.get("max_frontier", 250000))

    sources: List[SourceCfg] = []
    for s in logic.get("sources", []):
        sources.append(
            SourceCfg(
                name=s["name"],
                seeds=[normalize_url(u) for u in s.get("seeds", [])],
                allowed_domains=s.get("allowed_domains", []),
                allow_prefixes=s.get("allow_prefixes", []),
            )
        )

    now = int(time.time())
    for scfg in sources:
        for u in scfg.seeds:
            if allowed(u, scfg):
                store.upsert_frontier(u, scfg.name, next_fetch_at=now, depth=0, max_frontier=max_frontier)

    session = requests.Session()
    session.headers.update({"User-Agent": ua})

    source_map = {s.name: s for s in sources}

    processed = 0
    pbar = tqdm(total=max_pages, desc="robot: pages processed")

    docs_in_db = 0
    try:
        while processed < max_pages:
            item = store.pop_next_url()
            if not item:
                break

            url, source_name, depth, next_fetch_at = item
            scfg = source_map.get(source_name)
            if not scfg:
                continue

            now = int(time.time())
            if next_fetch_at > now:
                time.sleep(min(next_fetch_at - now, 1))
                store.upsert_frontier(url, source_name, next_fetch_at, depth, max_frontier=max_frontier)
                continue

            url = normalize_url(url)
            if not allowed(url, scfg):
                continue

            prev_meta = store.get_doc_meta(url)

            try:
                html, etag, last_mod, content_hash, _, changed = fetch_url(session, url, prev_meta)
                ts = int(time.time())

                if changed:
                    store.upsert_doc(
                        url=url,
                        source=source_name,
                        html=html,
                        fetched_at=ts,
                        last_checked_at=ts,
                        etag=etag,
                        last_modified=last_mod,
                        content_hash=content_hash or "",
                    )

                    links = extract_links(url, html)
                    added = 0
                    for link in links:
                        if added >= max_links_per_page:
                            break
                        n = normalize_url(link)
                        if allowed(n, scfg):
                            store.upsert_frontier(
                                n,
                                source_name,
                                next_fetch_at=ts,
                                depth=depth + 1,
                                max_frontier=max_frontier,
                            )
                            added += 1
                else:
                    store.mark_checked_nochange(url)

                store.upsert_frontier(url, source_name, next_fetch_at=ts + recrawl_after, depth=0, max_frontier=max_frontier)

                processed += 1
                pbar.update(1)
                time.sleep(delay)

            except Exception as e:
                store.set_frontier_error(url, source_name, depth, err=str(e), retry_in_seconds=3600)
                time.sleep(delay)

    finally:
        pbar.close()
        docs_in_db = store.docs_count()
        store.close()

    print("Done. Docs in DB:", docs_in_db)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python robot.py path/to/config.yaml")
        sys.exit(2)
    run(sys.argv[1])
