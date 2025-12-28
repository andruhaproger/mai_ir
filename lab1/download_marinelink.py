import argparse
import os
import re
import time
from typing import Dict, List, Set

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

BASE = "https://www.marinelink.com"
LISTING = "https://www.marinelink.com/maritime-news"


def safe_slug(s: str, max_len: int = 120) -> str:
    s = (s or "").strip().replace(" ", "_")
    s = re.sub(r"[^0-9A-Za-z._-]+", "", s)
    s = s.strip("._-")
    return (s[:max_len] if s else "doc")


def write_json(path: str, obj: Dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    import json
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False)


def get_html(session: requests.Session, url: str, timeout: int, retries: int, backoff: float) -> str:
    last_err = None
    for i in range(retries + 1):
        try:
            r = session.get(url, timeout=timeout)
            r.raise_for_status()
            return r.text
        except Exception as e:
            last_err = e
            time.sleep(backoff * (2 ** i))
    raise last_err  # type: ignore


def extract_news_links(list_html: str) -> List[str]:
    soup = BeautifulSoup(list_html, "lxml")
    links = []
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if href.startswith("/news/") and re.search(r"-\d{4,}$", href):
            links.append(BASE + href)
    return links


def parse_article_min(html: str) -> Dict[str, str]:
    soup = BeautifulSoup(html, "lxml")
    h1 = soup.find("h1")
    title = h1.get_text(" ", strip=True) if h1 else ""

    parts = []
    for p in soup.select("p"):
        t = p.get_text(" ", strip=True)
        if t:
            parts.append(t)
    body = "\n".join(parts)

    date_text = ""
    for node in soup.find_all(string=re.compile(r"\b\w+\s+\d{1,2},\s+\d{4}\b")):
        cand = node.strip()
        if cand and len(cand) <= 30:
            date_text = cand
            break

    return {"title": title, "body": body, "date": date_text}


def existing_ids(out_dir: str) -> Set[str]:
    ids: Set[str] = set()
    if not os.path.isdir(out_dir):
        return ids
    for name in os.listdir(out_dir):
        if not (name.startswith("ml_") and name.endswith(".json")):
            continue
        parts = name.split("_", 2)
        if len(parts) >= 2:
            ids.add(parts[1])
    return ids


def main():
    ap = argparse.ArgumentParser(description="MarineLink crawler (Maritime News).")
    ap.add_argument("--out_dir", default=os.path.join("data_raw", "marinelink"))
    ap.add_argument("--target_kept", type=int, default=7013)
    ap.add_argument("--max_fetch", type=int, default=8839, help="Макс. успешных скачиваний (учёт отбраковки).")

    ap.add_argument("--max_pages", type=int, default=720, help="Глубина пагинации listing.")
    ap.add_argument("--sleep", type=float, default=0.7)
    ap.add_argument("--min_body_chars", type=int, default=1100)

    ap.add_argument("--timeout", type=int, default=30)
    ap.add_argument("--retries", type=int, default=2)
    ap.add_argument("--backoff", type=float, default=0.8)

    ap.add_argument("--ua", default="MAI-IR-Lab01-SeaCorpus/3.1 (educational; polite crawler)")
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    os.makedirs("out", exist_ok=True)

    s = requests.Session()
    s.headers.update({"User-Agent": args.ua})

    saved_ids = existing_ids(args.out_dir)
    kept = len(saved_ids)
    kept_start = kept  # ✅ для корректного keep_rate при resume

    fetched_ok = 0
    rejected_short = 0
    rejected_dup = 0
    errors = 0

    # 1) собрать ссылки из listing (может быть много дублей)
    all_links: List[str] = []
    for page in range(1, args.max_pages + 1):
        url = LISTING if page == 1 else f"{LISTING}?page={page}"
        try:
            html = get_html(s, url, timeout=args.timeout, retries=args.retries, backoff=args.backoff)
        except Exception:
            errors += 1
            continue

        all_links.extend(extract_news_links(html))
        time.sleep(args.sleep)

    # 2) unique + считаем дубли реалистично ✅
    uniq_links: List[str] = []
    seen_links: Set[str] = set()
    for u in all_links:
        if u in seen_links:
            rejected_dup += 1
            continue
        uniq_links.append(u)
        seen_links.add(u)

    pbar = tqdm(total=args.target_kept, initial=min(kept, args.target_kept), desc="MarineLink kept")

    # 3) качать статьи, пока не наберём target_kept или max_fetch
    for link in uniq_links:
        if kept >= args.target_kept or fetched_ok >= args.max_fetch:
            break

        m = re.search(r"-(\d{4,})$", link)
        doc_id = m.group(1) if m else safe_slug(link)
        if doc_id in saved_ids:
            continue

        try:
            html = get_html(s, link, timeout=args.timeout, retries=args.retries, backoff=args.backoff)
            fetched_ok += 1
        except Exception:
            errors += 1
            time.sleep(args.sleep)
            continue

        meta = parse_article_min(html)
        if len(meta.get("body", "")) < args.min_body_chars:
            rejected_short += 1
            time.sleep(args.sleep)
            continue

        doc = {
            "source": "marinelink",
            "url": link,
            "title": meta.get("title", ""),
            "date": meta.get("date", ""),
            "raw_html": html,
            "fetched_at": int(time.time()),
        }
        fname = f"ml_{doc_id}_{safe_slug(doc['title'])}.json"
        write_json(os.path.join(args.out_dir, fname), doc)

        saved_ids.add(doc_id)
        kept += 1
        pbar.update(1)
        time.sleep(args.sleep)

    pbar.close()

    kept_new = kept - kept_start
    keep_rate = (kept_new / fetched_ok) if fetched_ok else 0.0

    report = {
        "source": "marinelink",
        "target_kept": args.target_kept,
        "max_fetch": args.max_fetch,

        "kept_total": kept,
        "kept_new": kept_new,

        "fetched_ok": fetched_ok,
        "rejected_short": rejected_short,
        "rejected_dup": rejected_dup,
        "errors": errors,
        "keep_rate": keep_rate,

        "ts": int(time.time()),
        "out_dir": args.out_dir,
        "min_body_chars": args.min_body_chars,
        "max_pages": args.max_pages,
    }
    write_json(os.path.join("out", "marinelink_report.json"), report)

    print("MarineLink done.")
    print(report)
    if kept < args.target_kept:
        print("Note: if not enough kept docs, try increasing --max_pages or lowering --min_body_chars.")


if __name__ == "__main__":
    main()
