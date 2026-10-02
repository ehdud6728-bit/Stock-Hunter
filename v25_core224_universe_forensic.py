from __future__ import annotations

"""CORE224 NO_DIRECT_UNIVERSE forensic diagnostic.

RESEARCH/DIAGNOSTIC ONLY.
- Does not change production search / score / rank / order logic.
- Does not promote fallback universe authority.
- Uses an isolated HistoricalUniverseRuntime cache so the diagnostic does not
  mutate the normal CORE224/V20 authority cache.
"""

import argparse
import json
import os
import traceback
from pathlib import Path
from typing import Any

import pandas as pd


def _jsonable(v: Any):
    if isinstance(v, pd.Timestamp):
        return v.isoformat()
    if hasattr(v, "item"):
        try:
            return v.item()
        except Exception:
            pass
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    return v


def _row_dict(df: pd.DataFrame) -> dict[str, Any]:
    if df is None or df.empty:
        return {}
    return {str(k): _jsonable(v) for k, v in df.iloc[-1].to_dict().items()}


def _count_files(p: Path) -> int:
    try:
        return sum(1 for x in p.rglob("*") if x.is_file())
    except Exception:
        return -1


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--asof", required=True)
    ap.add_argument("--output-dir", default="reports/v25_live_seed_build")
    ap.add_argument("--report-dir", default="reports/v25_live_seed_build/core224_universe_forensic")
    args = ap.parse_args()

    asof = pd.Timestamp(args.asof).normalize()
    rep = Path(args.report_dir)
    rep.mkdir(parents=True, exist_ok=True)

    iso_cache = rep / "isolated_asof_cache"
    os.environ["V20_ASOF_CACHE_DIR"] = str(iso_cache)

    result: dict[str, Any] = {
        "revision": "CORE224_NO_DIRECT_UNIVERSE_FORENSIC_R1_20261002",
        "research_only": True,
        "production_changed": False,
        "production_search_changed": False,
        "production_score_changed": False,
        "production_rank_changed": False,
        "production_order_changed": False,
        "asof": asof.strftime("%Y-%m-%d"),
        "env": {
            k: os.environ.get(k)
            for k in [
                "TEST_PROFILE",
                "STOCKHUNTER_PYKRX_FORCE_STUB",
                "V1081_BACKTEST_SOURCE",
                "V1081_DIRECT_DATE_MODE",
                "V1081_DIRECT_MAX_DATES",
                "V1081_DIRECT_TOP_N",
                "V1081_ASOF_LIQUIDITY_DAYS",
                "V23_ZERO_RECOMPUTE",
                "V23_SHARD_WORKER_ONLY",
                "V23_MERGE_ONLY_PARENT",
                "V23_MATERIALIZED_DIR",
                "V20_ASOF_CACHE_DIR",
            ]
        },
        "normal_cache_inventory": {},
        "direct_probe": {},
        "runtime": {},
        "exception": "",
    }

    for rel in [
        "reports/.cache/v20_price_history",
        "reports/.cache/v20_asof_snapshots",
        "reports/.cache/v25_actual_amount_history",
        "reports/.cache/v25_core224_live",
    ]:
        p = Path(rel)
        result["normal_cache_inventory"][rel] = {
            "exists": p.exists(),
            "file_count": _count_files(p) if p.exists() else 0,
        }

    try:
        import FinanceDataReader as fdr
        from pykrx import stock
        import historical_asof_universe as h

        result["pykrx"] = {
            "module": getattr(stock, "__name__", type(stock).__name__),
            "get_market_ticker_list": bool(callable(getattr(stock, "get_market_ticker_list", None))),
            "get_market_ohlcv": bool(
                callable(getattr(stock, "get_market_ohlcv", None))
                or callable(getattr(stock, "get_market_ohlcv_by_ticker", None))
            ),
            "get_market_cap": bool(
                callable(getattr(stock, "get_market_cap", None))
                or callable(getattr(stock, "get_market_cap_by_ticker", None))
            ),
        }

        liq_days = int(float(os.environ.get("V1081_ASOF_LIQUIDITY_DAYS", "20")))
        dates = h._calendar_before(asof, liq_days, fdr.DataReader)
        result["direct_probe"]["calendar_dates"] = [pd.Timestamp(x).strftime("%Y-%m-%d") for x in dates]
        result["direct_probe"]["calendar_count"] = len(dates)

        if dates:
            d1 = pd.Timestamp(dates[-1]).normalize()
            ymd = d1.strftime("%Y%m%d")
            result["direct_probe"]["d1"] = d1.strftime("%Y-%m-%d")

            try:
                listing, listing_errors = h._ticker_names_pykrx(stock, ymd)
                result["direct_probe"]["ticker_list_rows"] = int(len(listing))
                result["direct_probe"]["ticker_list_errors"] = list(listing_errors or [])
            except Exception as exc:
                result["direct_probe"]["ticker_list_rows"] = 0
                result["direct_probe"]["ticker_list_errors"] = [f"{type(exc).__name__}:{exc}"]

            try:
                snap, src = h._get_market_snapshot(stock, ymd)
                result["direct_probe"]["d1_market_snapshot_rows"] = int(len(snap))
                result["direct_probe"]["d1_market_snapshot_source"] = str(src)
                if isinstance(snap, pd.DataFrame) and not snap.empty:
                    result["direct_probe"]["d1_actual_snapshot_rows"] = int(h._actual_snapshot_rows(snap))
            except Exception as exc:
                result["direct_probe"]["d1_market_snapshot_rows"] = 0
                result["direct_probe"]["d1_market_snapshot_error"] = f"{type(exc).__name__}:{exc}"

            try:
                cap, src = h._get_cap_snapshot(stock, ymd)
                result["direct_probe"]["d1_cap_rows"] = int(len(cap))
                result["direct_probe"]["d1_cap_source"] = str(src)
            except Exception as exc:
                result["direct_probe"]["d1_cap_rows"] = 0
                result["direct_probe"]["d1_cap_error"] = f"{type(exc).__name__}:{exc}"

        rt = h.HistoricalUniverseRuntime(
            stock_module=stock,
            listing_loader=lambda: fdr.StockListing("KRX"),
            fdr_reader=fdr.DataReader,
        )
        mem, stats, avail = rt.build(asof, output_dir=rep / "runtime")

        result["runtime"] = {
            "membership_rows": int(len(mem)) if isinstance(mem, pd.DataFrame) else 0,
            "summary_rows": int(len(stats)) if isinstance(stats, pd.DataFrame) else 0,
            "availability_rows": int(len(avail)) if isinstance(avail, pd.DataFrame) else 0,
            "summary_last": _row_dict(stats),
            "availability_last": _row_dict(avail),
        }

        if isinstance(mem, pd.DataFrame):
            mem.to_csv(rep / "forensic_membership.csv", index=False, encoding="utf-8-sig")
        if isinstance(stats, pd.DataFrame):
            stats.to_csv(rep / "forensic_summary.csv", index=False, encoding="utf-8-sig")
        if isinstance(avail, pd.DataFrame):
            avail.to_csv(rep / "forensic_availability.csv", index=False, encoding="utf-8-sig")

    except Exception as exc:
        result["exception"] = f"{type(exc).__name__}:{exc}"
        result["traceback"] = traceback.format_exc()[-8000:]

    (rep / "core224_universe_forensic.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )

    print("CORE224_UNIVERSE_FORENSIC_COMPLETE")
    print(json.dumps(result, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
