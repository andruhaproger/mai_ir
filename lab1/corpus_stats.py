import json
import os
from glob import glob
from typing import Dict, List


def files_in(dir_path: str, pattern: str) -> List[str]:
    return [p for p in glob(os.path.join(dir_path, pattern)) if os.path.isfile(p)]


def size_bytes(paths: List[str]) -> int:
    return sum(os.path.getsize(p) for p in paths)


def avg_size_bytes(paths: List[str]) -> float:
    return (size_bytes(paths) / len(paths)) if paths else 0.0


def avg_text_chars(paths: List[str]) -> float:
    if not paths:
        return 0.0
    total = 0
    for p in paths:
        with open(p, "r", encoding="utf-8") as f:
            total += len(f.read())
    return total / len(paths)


def mb(x: int) -> float:
    return x / (1024 * 1024)


def kb(x: float) -> float:
    return x / 1024


def stats_block(name: str, raw_dir: str, txt_dir: str) -> Dict:
    raw_files = files_in(raw_dir, "*.json")
    txt_files = files_in(txt_dir, "*.txt")
    return {
        "source": name,
        "docs_raw": len(raw_files),
        "docs_text": len(txt_files),
        "raw_total_bytes": size_bytes(raw_files),
        "text_total_bytes": size_bytes(txt_files),
        "avg_raw_bytes": avg_size_bytes(raw_files),
        "avg_text_bytes": avg_size_bytes(txt_files),
        "avg_text_chars": avg_text_chars(txt_files),
    }


def main():
    os.makedirs("out", exist_ok=True)

    s1 = stats_block("wikipedia_en", os.path.join("data_raw", "wikipedia_en"), os.path.join("data_text", "wikipedia_en"))
    s2 = stats_block("marinelink", os.path.join("data_raw", "marinelink"), os.path.join("data_text", "marinelink"))

    total_docs = s1["docs_text"] + s2["docs_text"]
    total_raw_bytes = s1["raw_total_bytes"] + s2["raw_total_bytes"]
    total_txt_bytes = s1["text_total_bytes"] + s2["text_total_bytes"]

    lines = []
    lines.append("=== Lab01 corpus statistics (Sea / Maritime) ===")
    for s in [s1, s2]:
        lines.append(f"\nSource: {s['source']}")
        lines.append(f"  docs (raw/text): {s['docs_raw']} / {s['docs_text']}")
        lines.append(f"  raw total:  {mb(s['raw_total_bytes']):.2f} MB")
        lines.append(f"  text total: {mb(s['text_total_bytes']):.2f} MB")
        lines.append(f"  avg raw doc:  {kb(s['avg_raw_bytes']):.2f} KB")
        lines.append(f"  avg text doc: {kb(s['avg_text_bytes']):.2f} KB")
        lines.append(f"  avg text chars: {s['avg_text_chars']:.0f}")

    lines.append("\n=== Total ===")
    lines.append(f"docs(text): {total_docs}")
    lines.append(f"raw total:  {mb(total_raw_bytes):.2f} MB")
    lines.append(f"text total: {mb(total_txt_bytes):.2f} MB")

    report_txt = "\n".join(lines)
    with open(os.path.join("out", "stats.txt"), "w", encoding="utf-8") as f:
        f.write(report_txt)

    payload = {"sources": [s1, s2], "total_docs_text": total_docs}
    with open(os.path.join("out", "stats.json"), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(report_txt)
    print("\nSaved: out/stats.txt, out/stats.json")


if __name__ == "__main__":
    main()
