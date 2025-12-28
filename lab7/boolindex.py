import argparse
import glob
import os
import struct
import subprocess
import sys
import time

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from tokenizer import tokenize, simple_stem_en


def list_docs(input_dir: str):
    docs = []
    for src in ("wikipedia_en", "marinelink"):
        d = os.path.join(input_dir, src)
        for fp in glob.glob(os.path.join(d, "*.txt")):
            docs.append((src, fp))
    docs.sort(key=lambda x: x[1])
    return docs


def build_term_doc_file(docs, term_doc_path: str, use_stemming: bool, min_token_len: int, max_docs: int):
    t0 = time.perf_counter()
    total_pairs = 0
    total_bytes = 0
    used = 0

    with open(term_doc_path, "w", encoding="utf-8") as out:
        for doc_id, (src, fp) in enumerate(docs):
            if max_docs and doc_id >= max_docs:
                break
            used += 1
            with open(fp, "r", encoding="utf-8", errors="ignore") as f:
                text = f.read()
            total_bytes += len(text.encode("utf-8", errors="ignore"))

            toks = tokenize(text)
            if min_token_len > 1:
                toks = [t for t in toks if len(t) >= min_token_len]
            if use_stemming:
                toks = [simple_stem_en(t) for t in toks]

            uniq = set(toks)
            for term in uniq:
                out.write(f"{term}\t{doc_id}\n")
                total_pairs += 1

    t1 = time.perf_counter()
    kb = total_bytes / 1024.0
    return used, total_pairs, kb, (t1 - t0)


def write_docs_table(docs, docs_tsv: str, max_docs: int):
    with open(docs_tsv, "w", encoding="utf-8") as f:
        for doc_id, (src, fp) in enumerate(docs):
            if max_docs and doc_id >= max_docs:
                break
            f.write(f"{doc_id}\t{src}\t{fp}\n")


def build_inverted_from_sorted(sorted_path: str, dict_tsv: str, postings_bin: str):
    t0 = time.perf_counter()

    with open(sorted_path, "r", encoding="utf-8", errors="ignore") as inp, \
         open(postings_bin, "wb") as post, \
         open(dict_tsv, "w", encoding="utf-8") as dic:

        cur_term = None
        last_doc = None
        postings = []
        offset = 0

        def flush(term, postings_list):
            nonlocal offset
            if term is None:
                return
            postings_list.sort()
            data = b"".join(struct.pack("<I", d) for d in postings_list)
            post.write(data)
            nbytes = len(data)
            dic.write(f"{term}\t{len(postings_list)}\t{offset}\t{nbytes}\n")
            offset += nbytes

        for line in inp:
            line = line.rstrip("\n")
            if not line:
                continue
            term, doc_s = line.split("\t")
            doc_id = int(doc_s)

            if cur_term != term:
                flush(cur_term, postings)
                cur_term = term
                postings = []
                last_doc = None

            if last_doc != doc_id:
                postings.append(doc_id)
                last_doc = doc_id

        flush(cur_term, postings)

    t1 = time.perf_counter()
    return t1 - t0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input_dir", default="data_text")
    ap.add_argument("--out_dir", default="out_bool")
    ap.add_argument("--max_docs", type=int, default=0, help="0 = all")
    ap.add_argument("--min_token_len", type=int, default=2)
    ap.add_argument("--no_stemming", action="store_true")
    ap.add_argument("--tmp_dir", default=None)
    ap.add_argument("--sort_mem", default="40%")
    args = ap.parse_args()

    out_dir = args.out_dir
    tmp_dir = args.tmp_dir or os.path.join(out_dir, "tmp")
    idx_dir = os.path.join(out_dir, "index")
    os.makedirs(tmp_dir, exist_ok=True)
    os.makedirs(idx_dir, exist_ok=True)

    term_doc_path = os.path.join(tmp_dir, "term_doc.tsv")
    sorted_path = os.path.join(tmp_dir, "term_doc_sorted.tsv")

    docs_tsv = os.path.join(idx_dir, "docs.tsv")
    dict_tsv = os.path.join(idx_dir, "dict.tsv")
    postings_bin = os.path.join(idx_dir, "postings.bin")

    docs = list_docs(args.input_dir)
    if not docs:
        raise SystemExit("No documents found in input_dir")

    use_stemming = not args.no_stemming

    used, pairs, kb, t_build = build_term_doc_file(
        docs, term_doc_path, use_stemming=use_stemming, min_token_len=args.min_token_len, max_docs=args.max_docs
    )
    print(f"term-doc built: docs_used={used} pairs={pairs} input_kb={kb:.1f} time={t_build:.2f}s speed={kb/t_build:.1f} KB/s")

    subprocess.run(
        ["bash", "-lc", f"sort -T {tmp_dir} -S {args.sort_mem} {term_doc_path} > {sorted_path}"],
        check=True,
    )
    print("sorted:", sorted_path, "size(bytes)=", os.path.getsize(sorted_path))

    write_docs_table(docs, docs_tsv, args.max_docs)

    t_inv = build_inverted_from_sorted(sorted_path, dict_tsv, postings_bin)
    dict_lines = sum(1 for _ in open(dict_tsv, "r", encoding="utf-8"))
    post_size = os.path.getsize(postings_bin)

    print("index built:", idx_dir)
    print(f"inverted build time: {t_inv:.2f}s")
    print(f"dict terms: {dict_lines}")
    print(f"postings.bin size: {post_size}")


if __name__ == "__main__":
    main()
