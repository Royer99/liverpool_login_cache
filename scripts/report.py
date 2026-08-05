"""Turn Locust CSV output into a percentile table and a latency-over-time chart.

Reads ``<prefix>_stats.csv`` and ``<prefix>_stats_history.csv`` (produced by
``--csv <prefix> --csv-full-history``), prints p50/p95/p99/max per named
operation — never a bare mean — and writes ``<prefix>_latency.png`` with the
READ_LATENCY_SLO_MS line drawn on it.

Standalone usage:
    python scripts/report.py --csv-prefix results
"""

from __future__ import annotations

import argparse
import csv
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from config import load_config  # noqa: E402


def print_table(stats_csv: Path, slo_ms: float) -> None:
    """Percentile table per named operation from the aggregate stats CSV."""
    with open(stats_csv) as f:
        rows = [r for r in csv.DictReader(f) if r["Name"] != "Aggregated"]
    if not rows:
        raise SystemExit(f"no per-operation rows in {stats_csv}")
    print(f"{'operation':<18}{'count':>10}{'fail':>7}{'p50':>8}{'p95':>8}{'p99':>8}{'max':>9}  SLO({slo_ms:g}ms)")
    print("-" * 78)
    for r in sorted(rows, key=lambda r: r["Name"]):
        p99 = float(r["99%"])
        verdict = ""
        if r["Name"] in ("get_session", "mget_sessions"):
            verdict = "PASS" if p99 <= slo_ms else "FAIL"
        print(f"{r['Name']:<18}{int(r['Request Count']):>10,}{int(r['Failure Count']):>7}"
              f"{float(r['50%']):>8.1f}{float(r['95%']):>8.1f}{p99:>8.1f}"
              f"{float(r['100%']):>9.1f}  {verdict}")
    print("\nLatencies are client-observed from the load generator (incl. network RTT).")
    print("Access pattern: HOT_ACCESS_SHARE of reads over the most recent "
          "HOT_SET_RATIO of sessions — quote them together.")


def plot_history(history_csv: Path, slo_ms: float, out_png: Path,
                 op: str = "get_session") -> None:
    """p50/p95/p99 over time for *op*, with the SLO line."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    ts: list[datetime] = []
    p50, p95, p99 = [], [], []
    with open(history_csv) as f:
        for r in csv.DictReader(f):
            if r["Name"] != op or not r["50%"]:
                continue
            ts.append(datetime.fromtimestamp(int(r["Timestamp"]), tz=timezone.utc))
            p50.append(float(r["50%"]))
            p95.append(float(r["95%"]))
            p99.append(float(r["99%"]))
    if not ts:
        print(f"(no history rows for {op}; was --csv-full-history set?)")
        return
    fig, ax = plt.subplots(figsize=(11, 5))
    ax.plot(ts, p50, label="p50", lw=1.2)
    ax.plot(ts, p95, label="p95", lw=1.2)
    ax.plot(ts, p99, label="p99", lw=1.2)
    ax.axhline(slo_ms, color="red", ls="--", lw=1.5, label=f"SLO {slo_ms:g} ms")
    ax.set_title(f"{op} latency over time (client-observed)")
    ax.set_ylabel("ms")
    ax.set_ylim(bottom=0)
    ax.legend()
    ax.grid(alpha=0.3)
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(out_png, dpi=120)
    print(f"chart written to {out_png}")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--csv-prefix", default="results", help="Locust --csv prefix")
    p.add_argument("--op", default="get_session", help="operation to chart over time")
    args = p.parse_args()
    cfg = load_config(require_uri=False)

    prefix = Path(args.csv_prefix)
    print_table(prefix.parent / f"{prefix.name}_stats.csv", cfg.read_latency_slo_ms)
    plot_history(prefix.parent / f"{prefix.name}_stats_history.csv",
                 cfg.read_latency_slo_ms,
                 prefix.parent / f"{prefix.name}_latency.png", op=args.op)


if __name__ == "__main__":
    main()
