import argparse
import glob
import os
import re
import time
from collections import Counter
from typing import Iterable, Tuple


TOKEN_RE = re.compile(r"[a-z0-9]+(?:['-][a-z0-9]+)*", re.IGNORECASE)


def iter_text_files(input_dir: str) -> Iterable[Tuple[str, str]]:
    for src in ("wikipedia_en", "marinelink"):
        d = os.path.join(input_dir, src)
        for fp in glob.glob(os.path.join(d, "*.txt")):
            yield src, fp


def tokenize(text: str):
    return [m.group(0).lower() for m in TOKEN_RE.finditer(text)]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input_dir", default="data_text")
    ap.add_argument("--out_dir", default=os.path.join("out", "lab_03"))
    ap.add_argument("--limit_files", type=int, default=0, help="0 = all files")
    ap.add_argument("--min_token_len", type=int, default=1)
    ap.add_argument("--max_tokens", type=int, default=0, help="0 = no limit (for quick debug)")
    ap.add_argument("--save_counts_tsv", default=os.path.join("out", "lab_03", "term_freq.tsv"))
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    files = list(iter_text_files(args.input_dir))
    if args.limit_files and args.limit_files > 0:
        files = files[: args.limit_files]

    term_freq = Counter()
    total_tokens = 0
    total_token_chars = 0
    total_bytes = 0

    t0 = time.perf_counter()
    for _, fp in files:
        with open(fp, "r", encoding="utf-8", errors="ignore") as f:
            text = f.read()
        total_bytes += len(text.encode("utf-8", errors="ignore"))

        toks = tokenize(text)
        if args.min_token_len > 1:
            toks = [t for t in toks if len(t) >= args.min_token_len]

        if args.max_tokens and total_tokens >= args.max_tokens:
            break

        total_tokens += len(toks)
        total_token_chars += sum(len(t) for t in toks)
        term_freq.update(toks)

    t1 = time.perf_counter()
    elapsed = t1 - t0
    kb = total_bytes / 1024.0
    avg_len = (total_token_chars / total_tokens) if total_tokens else 0.0
    speed = (kb / elapsed) if elapsed > 0 else 0.0

    stats_txt = []
    stats_txt.append("=== Lab03 Tokenization statistics ===")
    stats_txt.append(f"files: {len(files)}")
    stats_txt.append(f"input_kb: {kb:.2f}")
    stats_txt.append(f"total_tokens: {total_tokens}")
    stats_txt.append(f"avg_token_len: {avg_len:.4f}")
    stats_txt.append(f"vocab_size: {len(term_freq)}")
    stats_txt.append(f"time_s: {elapsed:.4f}")
    stats_txt.append(f"speed_kb_s: {speed:.2f}")
    stats_txt = "\n".join(stats_txt) + "\n"

    with open(os.path.join(args.out_dir, "stats.txt"), "w", encoding="utf-8") as f:
        f.write(stats_txt)

    if args.save_counts_tsv:
        with open(args.save_counts_tsv, "w", encoding="utf-8") as f:
            for term, cnt in term_freq.most_common():
                f.write(f"{term}\t{cnt}\n")

    print(stats_txt.strip())
    print("Saved:", os.path.join(args.out_dir, "stats.txt"))
    if args.save_counts_tsv:
        print("Saved:", args.save_counts_tsv)


if __name__ == "__main__":
    main()
