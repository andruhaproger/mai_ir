import os
import re
import argparse
from html import escape
from flask import Flask, request

def ends_with(s, suf):
    return s.endswith(suf)

def stem_word(w):
    if len(w) < 4:
        return w
    if ends_with(w, "'s") and len(w) > 3:
        w = w[:-2]
    if ends_with(w, "sses") and len(w) > 6:
        return w[:-2]
    if ends_with(w, "ies") and len(w) > 5:
        return w[:-3] + "y"
    if ends_with(w, "s") and len(w) > 4 and not ends_with(w, "ss"):
        w = w[:-1]
    if ends_with(w, "ing") and len(w) > 6:
        return w[:-3]
    if ends_with(w, "ed") and len(w) > 5:
        return w[:-2]
    if ends_with(w, "ly") and len(w) > 6:
        return w[:-2]
    if ends_with(w, "ment") and len(w) > 8:
        return w[:-4]
    return w

def read_varint(f):
    v = 0
    shift = 0
    while True:
        b = f.read(1)
        if not b:
            raise EOFError("unexpected EOF")
        b = b[0]
        v |= (b & 0x7F) << shift
        if (b & 0x80) == 0:
            break
        shift += 7
    return v

def load_docs(docs_tsv):
    docs = []
    with open(docs_tsv, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.rstrip("\n")
            parts = line.split("\t")
            if len(parts) >= 3:
                docs.append((parts[1], parts[2]))
    return docs

def load_dict(dict_tsv):
    d = {}
    with open(dict_tsv, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.rstrip("\n")
            parts = line.split("\t")
            if len(parts) == 3:
                term = parts[0]
                off = int(parts[1])
                df = int(parts[2])
                d[term] = (off, df)
    return d

def load_postings(bin_path, off, df):
    res = []
    with open(bin_path, "rb") as f:
        f.seek(off, os.SEEK_SET)
        cur = 0
        for i in range(df):
            gap = read_varint(f)
            cur = gap if i == 0 else (cur + gap)
            res.append(cur)
    return res

def intersect_sorted(a, b):
    r = []
    i = 0
    j = 0
    while i < len(a) and j < len(b):
        if a[i] == b[j]:
            r.append(a[i]); i += 1; j += 1
        elif a[i] < b[j]:
            i += 1
        else:
            j += 1
    return r

def union_sorted(a, b):
    r = []
    i = 0
    j = 0
    while i < len(a) or j < len(b):
        if j >= len(b) or (i < len(a) and a[i] < b[j]):
            r.append(a[i]); i += 1
        elif i >= len(a) or (j < len(b) and b[j] < a[i]):
            r.append(b[j]); j += 1
        else:
            r.append(a[i]); i += 1; j += 1
    return r

def complement_sorted(a, n_docs):
    r = []
    j = 0
    for doc_id in range(n_docs):
        if j < len(a) and a[j] == doc_id:
            j += 1
        else:
            r.append(doc_id)
    return r

TT_TERM, TT_AND, TT_OR, TT_NOT, TT_LP, TT_RP = range(6)

_word_re = re.compile(r"[A-Za-z0-9][A-Za-z0-9'\-]*")

def query_tokenize(q):
    out = []
    i = 0
    while i < len(q):
        c = q[i]
        if c.isspace():
            i += 1
            continue
        if c == "(":
            out.append((TT_LP, ""))
            i += 1
            continue
        if c == ")":
            out.append((TT_RP, ""))
            i += 1
            continue
        m = _word_re.match(q, i)
        if m:
            w = m.group(0).lower()
            up = w.upper()
            if up == "AND":
                out.append((TT_AND, ""))
            elif up == "OR":
                out.append((TT_OR, ""))
            elif up == "NOT":
                out.append((TT_NOT, ""))
            else:
                w = stem_word(w)
                if len(w) >= 2:
                    out.append((TT_TERM, w))
            i = m.end()
            continue
        i += 1
    return out

def prec(tt):
    if tt == TT_NOT:
        return 3
    if tt == TT_AND:
        return 2
    if tt == TT_OR:
        return 1
    return 0

def to_rpn(tokens):
    out = []
    st = []
    for tt, txt in tokens:
        if tt == TT_TERM:
            out.append((tt, txt))
        elif tt in (TT_AND, TT_OR, TT_NOT):
            while st and st[-1][0] in (TT_AND, TT_OR, TT_NOT) and prec(st[-1][0]) >= prec(tt):
                out.append(st.pop())
            st.append((tt, txt))
        elif tt == TT_LP:
            st.append((tt, txt))
        elif tt == TT_RP:
            while st and st[-1][0] != TT_LP:
                out.append(st.pop())
            if st and st[-1][0] == TT_LP:
                st.pop()
    while st:
        out.append(st.pop())
    return out

def eval_rpn(rpn, term_dict, postings_bin, n_docs):
    st = []
    for tt, txt in rpn:
        if tt == TT_TERM:
            if txt not in term_dict:
                st.append([])
            else:
                off, df = term_dict[txt]
                st.append(load_postings(postings_bin, off, df))
        elif tt == TT_NOT:
            if not st:
                return []
            a = st.pop()
            st.append(complement_sorted(a, n_docs))
        elif tt in (TT_AND, TT_OR):
            if len(st) < 2:
                return []
            b = st.pop()
            a = st.pop()
            st.append(intersect_sorted(a, b) if tt == TT_AND else union_sorted(a, b))
    return st[0] if len(st) == 1 else []

def make_app(index_dir):
    dict_path = os.path.join(index_dir, "dict.tsv")
    docs_path = os.path.join(index_dir, "docs.tsv")
    post_path = os.path.join(index_dir, "postings.bin")

    term_dict = load_dict(dict_path)
    docs = load_docs(docs_path)
    n_docs = len(docs)

    app = Flask(__name__)

    @app.get("/")
    def home():
        q = request.args.get("q", "").strip()
        topk = request.args.get("topk", "10").strip()
        try:
            topk_i = max(1, min(100, int(topk)))
        except:
            topk_i = 10

        results_html = ""
        if q:
            toks = query_tokenize(q)
            rpn = to_rpn(toks)
            hits = eval_rpn(rpn, term_dict, post_path, n_docs)
            shown = hits[:topk_i]
            items = []
            for doc_id in shown:
                if 0 <= doc_id < n_docs:
                    src, path = docs[doc_id]
                    items.append(f"<li><b>{doc_id}</b> [{escape(src)}] {escape(path)}</li>")
            results_html = f"<p>hits: {len(hits)}</p><ol>{''.join(items)}</ol>"

        html = f"""
<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>Boolean Search (Lab08)</title>
<style>
body {{ font-family: Arial, sans-serif; margin: 24px; }}
input[type=text] {{ width: 70%; }}
code {{ background: #f3f3f3; padding: 2px 4px; }}
</style>
</head>
<body>
<h2>Boolean Search (Lab08)</h2>
<form method="get">
<input type="text" name="q" value="{escape(q)}" placeholder="Query: AND/OR/NOT, parentheses">
<label style="margin-left:12px;">TopK:</label>
<input type="text" name="topk" value="{escape(str(topk_i))}" style="width:60px;">
<button type="submit">Search</button>
</form>
<p>Examples:</p>
<ul>
<li><code>ocean AND pollution</code></li>
<li><code>marine AND (biology OR ecology)</code></li>
<li><code>ship AND NOT war</code></li>
<li><code>(sea OR ocean) AND temperature</code></li>
</ul>
{results_html}
</body>
</html>
"""
        return html

    return app

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--index_dir", default=os.path.join("out_bool", "index"))
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=5000)
    args = ap.parse_args()
    app = make_app(args.index_dir)
    app.run(host=args.host, port=args.port, debug=False, use_reloader=False)

if __name__ == "__main__":
    main()
