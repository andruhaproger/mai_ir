import argparse
import os
import math
import matplotlib.pyplot as plt


def read_counts_tsv(path: str, limit_vocab: int = 0):
    freqs = []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for i, line in enumerate(f):
            if not line.strip():
                continue
            parts = line.rstrip("\n").split("\t")
            if len(parts) != 2:
                continue
            try:
                cnt = int(parts[1])
            except ValueError:
                continue
            freqs.append(cnt)
            if limit_vocab and len(freqs) >= limit_vocab:
                break
    freqs.sort(reverse=True)
    return freqs


def fit_zipf_s(freqs, start_rank: int, end_rank: int):
    end_rank = min(end_rank, len(freqs))
    if start_rank >= end_rank:
        return 1.0, float(freqs[0])

    xs = []
    ys = []
    for r in range(start_rank, end_rank + 1):
        f = freqs[r - 1]
        if f <= 0:
            continue
        xs.append(math.log(r))
        ys.append(math.log(f))

    n = len(xs)
    if n < 2:
        return 1.0, float(freqs[0])

    mx = sum(xs) / n
    my = sum(ys) / n
    num = sum((xs[i] - mx) * (ys[i] - my) for i in range(n))
    den = sum((xs[i] - mx) ** 2 for i in range(n))
    b = num / den if den else -1.0
    a = my - b * mx

    s = -b
    C = math.exp(a)
    return s, C


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--counts_tsv", required=True, help="TSV from lab_03 or lab_04: term\\tcount")
    ap.add_argument("--out_dir", default=os.path.join("out", "lab_05"))
    ap.add_argument("--limit_vocab", type=int, default=0, help="0 = use all rows from tsv")
    ap.add_argument("--fit", action="store_true", help="Fit exponent s for f ≈ C / r^s")
    ap.add_argument("--fit_start", type=int, default=50)
    ap.add_argument("--fit_end", type=int, default=50000)
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    freqs = read_counts_tsv(args.counts_tsv, limit_vocab=args.limit_vocab)
    if not freqs:
        raise RuntimeError("No frequencies read from counts_tsv")

    ranks = list(range(1, len(freqs) + 1))

    if args.fit:
        s, C = fit_zipf_s(freqs, args.fit_start, args.fit_end)
        model = [C / (r ** s) for r in ranks]
        title = f"Zipf fit: f ≈ C / r^s (s={s:.3f})"
        out_png = os.path.join(args.out_dir, "zipf_fit.png")
        out_txt = os.path.join(args.out_dir, "zipf_fit.txt")
        with open(out_txt, "w", encoding="utf-8") as f:
            f.write(f"s={s}\nC={C}\nfit_start={args.fit_start}\nfit_end={args.fit_end}\n")
    else:
        C = float(freqs[0])
        model = [C / r for r in ranks]
        title = "Zipf plot (f ≈ C / r)"
        out_png = os.path.join(args.out_dir, "zipf.png")

    plt.figure()
    plt.loglog(ranks, freqs, marker=".", linestyle="None")
    plt.loglog(ranks, model)
    plt.xlabel("Rank (log)")
    plt.ylabel("Frequency (log)")
    plt.title(title)
    plt.grid(True, which="both")
    plt.savefig(out_png, dpi=160, bbox_inches="tight")
    plt.show()

    print("Saved:", out_png)
    if args.fit:
        print("Saved:", out_txt)


if __name__ == "__main__":
    main()
