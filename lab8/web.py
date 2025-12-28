# lab8/web.py
import os
import sys
from html import escape

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import argparse
from flask import Flask, request

from lab8.boolsearch import load_dict, load_docs, search


def make_app(index_dir: str):
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
        topk = int(request.args.get("k", "10") or "10")

        results_html = ""
        if q:
            ids, total = search(q, term_dict, post_path, n_docs, topk)
            items = []
            for d in ids:
                src, path = docs[d]
                items.append(
                    f"<li><code>{d}</code> "
                    f"<b>{escape(src)}</b> — {escape(os.path.basename(path))}"
                    f"<br><small>{escape(path)}</small></li>"
                )
            results_html = (
                f"<p>Hits: <b>{total}</b>, shown: <b>{len(ids)}</b></p>"
                f"<ol>{''.join(items)}</ol>"
            )

        return f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Boolean Search (Lab08)</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; }}
    input[type=text] {{ width: 70%; padding: 8px; }}
    input[type=number] {{ width: 80px; padding: 6px; }}
    button {{ padding: 8px 14px; }}
    code {{ background: #f2f2f2; padding: 2px 4px; }}
    small {{ color: #555; }}
  </style>
</head>
<body>
  <h2>Boolean Search (Lab08)</h2>
  <form method="get" action="/">
    <input type="text" name="q" value="{escape(q)}" placeholder="Query: AND/OR/NOT, parentheses">
    <label>TopK:</label>
    <input type="number" name="k" value="{topk}" min="1" max="100">
    <button type="submit">Search</button>
  </form>

  <p><b>Examples:</b></p>
  <ul>
    <li><code>ocean AND pollution</code></li>
    <li><code>marine AND (biology OR ecology)</code></li>
    <li><code>ship AND NOT war</code></li>
    <li><code>(sea OR ocean) AND temperature</code></li>
  </ul>

  <hr>
  {results_html}
</body>
</html>
        """

    return app


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--index_dir", default=os.path.join("out_bool", "index"))
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=5000)
    args = ap.parse_args()

    app = make_app(args.index_dir)
    app.run(host=args.host, port=args.port, debug=False)


if __name__ == "__main__":
    main()
