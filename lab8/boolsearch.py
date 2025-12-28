import argparse
import os
import re
import struct
import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from tokenizer import simple_stem_en


Q_TOKEN_RE = re.compile(r"\(|\)|AND|OR|NOT|[A-Za-z0-9]+(?:['-][A-Za-z0-9]+)*", re.IGNORECASE)
PREC = {"NOT": 3, "AND": 2, "OR": 1}
RIGHT_ASSOC = {"NOT"}


def load_dict(dict_path: str):
    d = {}
    with open(dict_path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            term, df, off, nb = line.rstrip("\n").split("\t")
            d[term] = (int(df), int(off), int(nb))
    return d


def load_docs(docs_tsv: str):
    docs = []
    with open(docs_tsv, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            doc_id, src, path = line.rstrip("\n").split("\t", 2)
            docs.append((src, path))
    return docs


def read_postings(term: str, term_dict, postings_bin: str):
    meta = term_dict.get(term)
    if not meta:
        return []
    _, off, nb = meta
    with open(postings_bin, "rb") as f:
        f.seek(off)
        data = f.read(nb)
    out = []
    for i in range(0, len(data), 4):
        out.append(struct.unpack("<I", data[i:i+4])[0])
    return out


def op_and(a, b):
    i = j = 0
    out = []
    while i < len(a) and j < len(b):
        if a[i] == b[j]:
            out.append(a[i]); i += 1; j += 1
        elif a[i] < b[j]:
            i += 1
        else:
            j += 1
    return out


def op_or(a, b):
    i = j = 0
    out = []
    while i < len(a) and j < len(b):
        if a[i] == b[j]:
            out.append(a[i]); i += 1; j += 1
        elif a[i] < b[j]:
            out.append(a[i]); i += 1
        else:
            out.append(b[j]); j += 1
    out.extend(a[i:])
    out.extend(b[j:])
    return out


def op_not(a, n_docs: int):
    out = []
    i = 0
    for d in range(n_docs):
        if i < len(a) and a[i] == d:
            i += 1
        else:
            out.append(d)
    return out


def q_tokenize(q: str):
    toks = [m.group(0) for m in Q_TOKEN_RE.finditer(q)]
    out = []
    for t in toks:
        u = t.upper()
        if u in ("AND", "OR", "NOT", "(", ")"):
            out.append(u)
        else:
            out.append(simple_stem_en(t.lower()))
    return out


def to_rpn(tokens):
    out = []
    stack = []
    for t in tokens:
        if t == "(":
            stack.append(t)
        elif t == ")":
            while stack and stack[-1] != "(":
                out.append(stack.pop())
            if not stack:
                raise ValueError("Mismatched parentheses")
            stack.pop()
        elif t in PREC:
            while stack and stack[-1] in PREC:
                top = stack[-1]
                if (PREC[top] > PREC[t]) or (PREC[top] == PREC[t] and t not in RIGHT_ASSOC):
                    out.append(stack.pop())
                else:
                    break
            stack.append(t)
        else:
            out.append(t)
    while stack:
        if stack[-1] in ("(", ")"):
            raise ValueError("Mismatched parentheses")
        out.append(stack.pop())
    return out


def eval_rpn(rpn, term_dict, postings_bin: str, n_docs: int):
    st = []
    for t in rpn:
        if t == "NOT":
            if not st:
                raise ValueError("NOT missing operand")
            a = st.pop()
            st.append(op_not(a, n_docs))
        elif t == "AND":
            if len(st) < 2:
                raise ValueError("AND missing operand")
            b = st.pop(); a = st.pop()
            st.append(op_and(a, b))
        elif t == "OR":
            if len(st) < 2:
                raise ValueError("OR missing operand")
            b = st.pop(); a = st.pop()
            st.append(op_or(a, b))
        else:
            st.append(read_postings(t, term_dict, postings_bin))
    if len(st) != 1:
        raise ValueError("Bad query")
    return st[0]


def search(query: str, term_dict, postings_bin: str, n_docs: int, topk: int):
    toks = q_tokenize(query)
    rpn = to_rpn(toks)
    ids = eval_rpn(rpn, term_dict, postings_bin, n_docs)
    return ids[:topk], len(ids)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--index_dir", default=os.path.join("out_bool", "index"))
    ap.add_argument("--topk", type=int, default=10)
    ap.add_argument("--query", default=None)
    args = ap.parse_args()

    dict_path = os.path.join(args.index_dir, "dict.tsv")
    docs_path = os.path.join(args.index_dir, "docs.tsv")
    post_path = os.path.join(args.index_dir, "postings.bin")

    term_dict = load_dict(dict_path)
    docs = load_docs(docs_path)
    n_docs = len(docs)

    if args.query is not None:
        ids, total = search(args.query, term_dict, post_path, n_docs, args.topk)
        print(f"hits: {total}")
        for d in ids:
            src, path = docs[d]
            print(f"{d}\t{src}\t{path}")
        return

    while True:
        q = input("query> ").strip()
        if not q:
            break
        try:
            ids, total = search(q, term_dict, post_path, n_docs, args.topk)
            print(f"hits: {total}")
            for d in ids:
                src, path = docs[d]
                print(f"{d}\t{src}\t{path}")
        except Exception as e:
            print("ERROR:", e)


if __name__ == "__main__":
    main()
