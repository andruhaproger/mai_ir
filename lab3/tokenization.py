import os, sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import argparse
import time
from collections import Counter

from tokenizer import tokenize
from corpus import iter_text_files


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input_dir", default="data_text")
    ap.add_argument("--out_dir", default=os.path.join("out", "lab3"))
    ap.add_argument("--limit_files", type=int, default=0, help="0 = all files")
    ap.add_argument("--min_token_len", type=int, default=1)
    ap.add_argument("--save_counts_tsv", default="")
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    term_freq = Counter()
    total_tokens = 0
    total_token_chars = 0
    total_bytes = 0
    files_used = 0

    t0 = time.perf_counter()
    for _, fp in iter_text_files(args.input_dir):
        files_used += 1
        if args.limit_files and files_used > args.limit_files:
            break

        with open(fp, "r", encoding="utf-8", errors="ignore") as f:
            text = f.read()

        total_bytes += len(text.encode("utf-8", errors="ignore"))

        toks = tokenize(text)
        if args.min_token_len > 1:
            toks = [t for t in toks if len(t) >= args.min_token_len]

        total_tokens += len(toks)
        total_token_chars += sum(len(t) for t in toks)
        term_freq.update(toks)

    t1 = time.perf_counter()
    elapsed = t1 - t0
    kb = total_bytes / 1024.0
    avg_len = (total_token_chars / total_tokens) if total_tokens else 0.0
    speed = (kb / elapsed) if elapsed > 0 else 0.0

    stats = []
    stats.append("=== Lab03 Tokenization statistics ===")
    stats.append(f"files: {files_used}")
    stats.append(f"input_kb: {kb:.2f}")
    stats.append(f"total_tokens: {total_tokens}")
    stats.append(f"avg_token_len: {avg_len:.4f}")
    stats.append(f"vocab_size: {len(term_freq)}")
    stats.append(f"time_s: {elapsed:.4f}")
    stats.append(f"speed_kb_s: {speed:.2f}")
    stats_txt = "\n".join(stats) + "\n"

    out_stats = os.path.join(args.out_dir, "stats.txt")
    with open(out_stats, "w", encoding="utf-8") as f:
        f.write(stats_txt)

    if args.save_counts_tsv:
        with open(args.save_counts_tsv, "w", encoding="utf-8") as f:
            for term, cnt in term_freq.most_common():
                f.write(f"{term}\t{cnt}\n")

    print(stats_txt.strip())
    print("Saved:", out_stats)
    if args.save_counts_tsv:
        print("Saved:", args.save_counts_tsv)


if __name__ == "__main__":
    main()
