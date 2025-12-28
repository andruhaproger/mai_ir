import re

TOKEN_RE = re.compile(r"[a-z0-9]+(?:['-][a-z0-9]+)*", re.IGNORECASE)

def tokenize(text: str):
    return [m.group(0).lower() for m in TOKEN_RE.finditer(text)]

def simple_stem_en(tok: str) -> str:
    t = tok
    if len(t) <= 3:
        return t
    t = t.strip("-'")
    if t.endswith("'s") and len(t) - 2 >= 3:
        t = t[:-2]

    rules = [
        ("ization", "ize"),
        ("ational", "ate"),
        ("fulness", "ful"),
        ("ousness", "ous"),
        ("iveness", "ive"),
        ("tional", "tion"),
        ("biliti", "ble"),
        ("lessly", "less"),
        ("ments", "ment"),
        ("ations", "ation"),
        ("ation", "ate"),
        ("encies", "ency"),
        ("ness", ""),
        ("ment", ""),
        ("tion", ""),
        ("sion", ""),
        ("able", ""),
        ("ible", ""),
        ("ship", ""),
        ("hood", ""),
        ("ward", ""),
        ("wise", ""),
        ("ing", ""),
        ("ed", ""),
        ("ly", ""),
        ("es", "e"),
        ("s", ""),
    ]
    for suf, rep in rules:
        if t.endswith(suf) and len(t) - len(suf) >= 3:
            t = t[: -len(suf)] + rep
            break
    return t
