#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

OLD = """def _calendar_before(asof_date: Any, n: int, fdr_reader: Callable[..., pd.DataFrame] | None = None) -> list[pd.Timestamp]:
    d = pd.Timestamp(asof_date).normalize()
    if callable(fdr_reader):
        start = (d - pd.Timedelta(days=max(90, n * 4))).strftime("%Y-%m-%d")
        end = d.strftime("%Y-%m-%d")
        for symbol in ["KS11", "KOSPI"]:
            try:
                z = fdr_reader(symbol, start, end)
                if z is not None and not z.empty:
                    dates = [pd.Timestamp(x).normalize() for x in pd.to_datetime(z.index) if pd.Timestamp(x).normalize() < d]
                    return sorted(list(dict.fromkeys(dates)))[-n:]
            except Exception:
                continue
    # Fail-safe business-day approximation is diagnostic only. Caller records source quality.
    return list(pd.bdate_range(end=d - pd.Timedelta(days=1), periods=n))
"""

NEW = """def _calendar_before(asof_date: Any, n: int, fdr_reader: Callable[..., pd.DataFrame] | None = None) -> list[pd.Timestamp]:
    d = pd.Timestamp(asof_date).normalize()
    if callable(fdr_reader):
        start = (d - pd.Timedelta(days=max(90, n * 4))).strftime("%Y-%m-%d")
        end = d.strftime("%Y-%m-%d")
        candidates = []
        diagnostics = []
        for symbol in ["KS11", "KOSPI", "005930"]:
            try:
                z = fdr_reader(symbol, start, end)
                if z is None or z.empty:
                    diagnostics.append((symbol, "EMPTY", 0))
                    continue
                dates = sorted({
                    pd.Timestamp(x).normalize()
                    for x in pd.to_datetime(z.index, errors="coerce")
                    if pd.notna(x) and pd.Timestamp(x).normalize() < d
                })
                if dates:
                    candidates.append((dates[-1], len(dates), symbol, dates))
                    diagnostics.append((symbol, dates[-1].strftime("%Y-%m-%d"), len(dates)))
                else:
                    diagnostics.append((symbol, "NO_PRIOR_DATE", 0))
            except Exception as exc:
                diagnostics.append((symbol, f"ERROR:{type(exc).__name__}", 0))
        if candidates:
            candidates.sort(key=lambda t: (t[0], t[1]), reverse=True)
            last_date, _, symbol, dates = candidates[0]
            chosen = dates[-n:]
            print(
                "V25_CALENDAR_BEFORE_FRESHEST",
                "asof", d.strftime("%Y-%m-%d"),
                "source", symbol,
                "latest", last_date.strftime("%Y-%m-%d"),
                "days", len(chosen),
                "candidates", diagnostics,
            )
            return chosen
    # Fail-safe business-day approximation is diagnostic only. Caller records source quality.
    return list(pd.bdate_range(end=d - pd.Timedelta(days=1), periods=n))
"""

def apply(path: Path) -> None:
    src = path.read_text(encoding="utf-8")
    if NEW in src:
        print("V25_CALENDAR_PATCH_ALREADY_APPLIED", path)
        return
    if OLD not in src:
        raise SystemExit("V25_CALENDAR_PATCH_TARGET_NOT_FOUND")
    path.write_text(src.replace(OLD, NEW, 1), encoding="utf-8")
    print("V25_CURRENT_CALENDAR_FRESHNESS_PATCH_APPLIED", path)

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", required=True)
    args = ap.parse_args()
    apply(Path(args.apply))

if __name__ == "__main__":
    main()
