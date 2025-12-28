import argparse
import os
from glob import glob
from typing import Dict

from bs4 import BeautifulSoup


def read_json(path: str) -> dict:
    import json
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: str, obj: Dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    import json
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False)


def html_to_text(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = soup.get_text("\n", strip=True)
    lines = [ln.strip() for ln in text.splitlines()]
    lines = [ln for ln in lines if ln]
    return "\n".join(lines)


def process_source(raw_dir: str, out_dir: str, min_chars: int) -> Dict:
    os.makedirs(out_dir, exist_ok=True)
    total = 0
    kept = 0
    rejected_short = 0

    for fp in glob(os.path.join(raw_dir, "*.json")):
        total += 1
        doc = read_json(fp)
        txt = html_to_text(doc.get("raw_html", ""))
        if len(txt) < min_chars:
            rejected_short += 1
            continue
        out_fp = os.path.join(out_dir, os.path.basename(fp).replace(".json", ".txt"))
        with open(out_fp, "w", encoding="utf-8") as f:
            f.write(txt)
        kept += 1

    return {"raw_total": total, "text_kept": kept, "rejected_short": rejected_short, "min_chars": min_chars}


def main():
    ap = argparse.ArgumentParser(description="Extract plain text from raw HTML JSON documents.")
    ap.add_argument("--min_chars", type=int, default=800)
    args = ap.parse_args()

    os.makedirs("out", exist_ok=True)
    r1 = process_source(os.path.join("data_raw", "wikipedia_en"), os.path.join("data_text", "wikipedia_en"), args.min_chars)
    r2 = process_source(os.path.join("data_raw", "marinelink"), os.path.join("data_text", "marinelink"), args.min_chars)

    report = {"wikipedia_en": r1, "marinelink": r2}
    write_json(os.path.join("out", "extract_report.json"), report)

    print("Extracted texts:", report)
    print("Saved to out/extract_report.json")


if __name__ == "__main__":
    main()
