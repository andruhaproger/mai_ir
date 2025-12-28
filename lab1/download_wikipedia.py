import argparse
import os
import re
import time
from collections import deque
from typing import Dict, List, Optional, Set, Tuple

import requests
from tqdm import tqdm

WIKI_API = "https://en.wikipedia.org/w/api.php"


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


def api_get(session: requests.Session, params: Dict, timeout: int, retries: int, backoff: float) -> Dict:
    last_err = None
    for i in range(retries + 1):
        try:
            r = session.get(WIKI_API, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            time.sleep(backoff * (2 ** i))
    raise last_err  


def list_category_members(
    session: requests.Session,
    cat: str,
    cmtype: str,
    limit: int,
    sleep_s: float,
    timeout: int,
    retries: int,
    backoff: float,
) -> List[Dict]:
    """
    Возвращает до limit элементов из categorymembers (page или subcat) с пагинацией.
    Реалистично ограничиваем namespace:
      - pages: namespace 0 (основные статьи)
      - subcats: namespace 14 (категории)
    """
    out: List[Dict] = []
    cont = None

    cmnamespace = 0 if cmtype == "page" else 14

    while True:
        params = {
            "action": "query",
            "format": "json",
            "list": "categorymembers",
            "cmtitle": f"Category:{cat}",
            "cmtype": cmtype,     # 'page' or subcat
            "cmlimit": 500,
            "cmnamespace": cmnamespace,
        }
        if cont:
            params["cmcontinue"] = cont

        data = api_get(session, params, timeout=timeout, retries=retries, backoff=backoff)
        out.extend(data.get("query", {}).get("categorymembers", []))

        cont = data.get("continue", {}).get("cmcontinue")
        if not cont or len(out) >= limit:
            break

        time.sleep(sleep_s)

    return out[:limit]


def fetch_page_html(
    session: requests.Session,
    pageid: int,
    sleep_s: float,
    timeout: int,
    retries: int,
    backoff: float,
) -> Optional[str]:
    """
    action=parse -> HTML статьи
    """
    params = {
        "action": "parse",
        "format": "json",
        "pageid": pageid,
        "prop": "text",
        "disableeditsection": 1,
        "redirects": 1,
    }
    data = api_get(session, params, timeout=timeout, retries=retries, backoff=backoff)
    time.sleep(sleep_s)
    parse = data.get("parse") or {}
    return (parse.get("text") or {}).get("*")


def existing_pageids(out_dir: str) -> Set[int]:
    """
    Resume: если файл уже сохранён, повторно не скачиваем.
    """
    ids: Set[int] = set()
    if not os.path.isdir(out_dir):
        return ids
    for name in os.listdir(out_dir):
        if not (name.startswith("wiki_") and name.endswith(".json")):
            continue
        parts = name.split("_", 2)
        if len(parts) >= 2 and parts[1].isdigit():
            ids.add(int(parts[1]))
    return ids


def main():
    ap = argparse.ArgumentParser(description="Wikipedia EN crawler (Sea/Maritime categories).")
    ap.add_argument("--out_dir", default=os.path.join("data_raw", "wikipedia_en"))
    ap.add_argument("--target_kept", type=int, default=35041, help="Сколько документов сохранить (после фильтра).")
    ap.add_argument("--max_fetch", type=int, default=36217, help="Макс. успешных скачиваний страниц (учёт отбраковки).")

    ap.add_argument("--min_html_chars", type=int, default=3200, help="Фильтр: минимальная длина HTML.")
    ap.add_argument("--max_depth", type=int, default=6)
    ap.add_argument("--sleep", type=float, default=0.25)

    ap.add_argument("--timeout", type=int, default=30)
    ap.add_argument("--retries", type=int, default=2)
    ap.add_argument("--backoff", type=float, default=0.6)

    ap.add_argument("--cat_page_limit", type=int, default=9000)
    ap.add_argument("--cat_sub_limit", type=int, default=1800)

    ap.add_argument("--ua", default="MAI-IR-Lab01-SeaCorpus/3.1 (educational; polite crawler)")
    ap.add_argument(
        "--seed_categories",
        nargs="+",
        default=["Marine biology", "Oceanography", "Seas", "Marine pollution", "Fisheries"],
    )
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    os.makedirs("out", exist_ok=True)

    saved_ids = existing_pageids(args.out_dir)
    kept = len(saved_ids)
    kept_start = kept  

    fetched_ok = 0
    rejected_short = 0
    errors = 0

    s = requests.Session()
    s.headers.update({"User-Agent": args.ua})

    queue: deque[Tuple[str, int]] = deque((c, 0) for c in args.seed_categories)
    seen_cats: Set[str] = set()

    pbar = tqdm(total=args.target_kept, initial=min(kept, args.target_kept), desc="Wikipedia EN kept")

    while queue and kept < args.target_kept and fetched_ok < args.max_fetch:
        cat, depth = queue.popleft()
        if cat in seen_cats:
            continue
        seen_cats.add(cat)

        #subcategories
        if depth < args.max_depth:
            try:
                subcats = list_category_members(
                    s,
                    cat,
                    cmtype="subcat",
                    limit=args.cat_sub_limit,
                    sleep_s=args.sleep,
                    timeout=args.timeout,
                    retries=args.retries,
                    backoff=args.backoff,
                )
            except Exception:
                errors += 1
                continue

            for sc in subcats:
                title = sc.get("title", "")
                if title.startswith("Category:"):
                    title = title.replace("Category:", "")
                if title and title not in seen_cats:
                    queue.append((title, depth + 1))

        #pages
        try:
            pages = list_category_members(
                s,
                cat,
                cmtype="page",
                limit=args.cat_page_limit,
                sleep_s=args.sleep,
                timeout=args.timeout,
                retries=args.retries,
                backoff=args.backoff,
            )
        except Exception:
            errors += 1
            continue

        for p in pages:
            if kept >= args.target_kept or fetched_ok >= args.max_fetch:
                break

            pageid = p.get("pageid")
            title = p.get("title")
            if not pageid or not title:
                continue

            pid = int(pageid)
            if pid in saved_ids:
                continue

            try:
                html = fetch_page_html(
                    s, pid,
                    sleep_s=args.sleep,
                    timeout=args.timeout,
                    retries=args.retries,
                    backoff=args.backoff,
                )
                fetched_ok += 1
            except Exception:
                errors += 1
                continue

            if not html or len(html) < args.min_html_chars:
                rejected_short += 1
                continue

            doc = {
                "source": "wikipedia_en",
                "category_seed": args.seed_categories,
                "category_found": cat,
                "pageid": pid,
                "title": title,
                "url": f"https://en.wikipedia.org/?curid={pid}",
                "raw_html": html,
                "fetched_at": int(time.time()),
            }
            fname = f"wiki_{pid}_{safe_slug(title)}.json"
            write_json(os.path.join(args.out_dir, fname), doc)

            saved_ids.add(pid)
            kept += 1
            pbar.update(1)

    pbar.close()

    kept_new = kept - kept_start
    keep_rate = (kept_new / fetched_ok) if fetched_ok else 0.0

    report = {
        "source": "wikipedia_en",
        "target_kept": args.target_kept,
        "max_fetch": args.max_fetch,

        "kept_total": kept,
        "kept_new": kept_new,

        "fetched_ok": fetched_ok,
        "rejected_short": rejected_short,
        "errors": errors,
        "keep_rate": keep_rate,

        "ts": int(time.time()),
        "out_dir": args.out_dir,
        "min_html_chars": args.min_html_chars,
        "max_depth": args.max_depth,
        "seed_categories": args.seed_categories,
    }
    write_json(os.path.join("out", "wiki_report.json"), report)

    print("Wikipedia done.")
    print(report)


if __name__ == "__main__":
    main()

