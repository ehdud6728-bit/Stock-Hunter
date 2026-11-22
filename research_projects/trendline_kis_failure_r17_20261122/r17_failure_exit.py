#!/usr/bin/env python3
"""R17: causal failure-risk diagnostics and adaptive structure exits.

RESEARCH_ONLY. Replays the frozen R16 cohorts with KIS-originated daily prices.
No percent stop/take-profit thresholds, no in-sample best-model promotion,
no LIVE/SEARCH/WATCH writes, and no claim of a prospective market-wide OOS.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import io
import json
import statistics
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
R16 = "research_projects/trendline_unseen_tickers_r16_20261121/r16_new_ticker_pilot.py"
EXPECTED_KIS_RUN = "44988188271"
EXPECTED_KIS_ARCHIVE_SHA = "e5edabf1b111d424a0afa8503111d7dd4d6e4827036aa7eea512ef067d122dbc"
EXPECTED_KIS_ROWS = 42782
COST_PERCENT = 0.2
TRAIN_END = "2023-12-31"
TEST_START = "2024-01-01"
HORIZON = 60
MODELS = ("R14_INTRADAY", "R14_CLOSE_NEXT_OPEN", "D0_LOW_NEXT_OPEN",
          "PREV5_LOW_NEXT_OPEN", "PREV10_LOW_NEXT_OPEN")
FEATURES = ("close_location", "amount_vs_prev20", "volume_vs_prev20",
            "close_vs_ma224_pct", "return_prev20_pct", "breakout_vs_prior55_high_pct")


def sha(buf: bytes) -> str:
    return hashlib.sha256(buf).hexdigest()


def load_r16(repo: Path):
    script = repo / R16
    spec = importlib.util.spec_from_file_location("frozen_r16_for_r17", script)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def kis_frame(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.set_index(pd.to_datetime(frame.date))
    out.index.name = "날짜"
    return out[["open", "high", "low", "close", "volume", "amount"]].rename(
        columns={"open": "시가", "high": "고가", "low": "저가",
                 "close": "종가", "volume": "거래량", "amount": "거래대금"})


def import_kis(repo: Path, artifact: Path, out: Path):
    """Fail closed on mismatched KIS data and independently validate each code."""
    import zipfile
    if artifact.is_file():
        with zipfile.ZipFile(artifact) as z:
            manifest_bytes = z.read("R16_KIS_SOURCE_MANIFEST.json")
            csv_bytes = z.read("R16_KIS_UNADJUSTED_30.csv")
    else:
        manifest_bytes = (artifact / "R16_KIS_SOURCE_MANIFEST.json").read_bytes()
        csv_bytes = (artifact / "R16_KIS_UNADJUSTED_30.csv").read_bytes()
    manifest = json.loads(manifest_bytes)
    assert manifest["source_run_id"] == EXPECTED_KIS_RUN
    assert manifest["archive_sha256"] == EXPECTED_KIS_ARCHIVE_SHA == sha(csv_bytes)
    assert manifest["selected_count"] == 30 and manifest["row_count"] == EXPECTED_KIS_ROWS
    assert manifest["full_r116_replay_done"] is False
    assert manifest["independent_corporate_action_validation"] is False
    raw = pd.read_csv(io.BytesIO(csv_bytes), dtype={"code": str, "date": str})
    assert list(raw.columns) == ["date", "code", "open", "high", "low", "close", "volume", "amount"]
    assert len(raw) == EXPECTED_KIS_ROWS and not raw.duplicated(["code", "date"]).any()
    assert set(raw.code) == set(manifest["selected_codes"])
    r16 = load_r16(repo)
    assert r16.selected_codes(repo) == manifest["selected_codes"]
    subsets = {code: g.copy() for code, g in raw.groupby("code", sort=False)}
    source = r16.collect(repo, out, fetcher=lambda code: kis_frame(subsets[code]))
    assert set(source["valid_codes"]) == set(manifest["structurally_valid_codes"])
    assert source["n_valid_tickers"] == manifest["structurally_valid_ticker_count"] == 26
    source.update({
        "data_provider": "KIS_original_unadjusted_daily_fhkst03010100",
        "kis_run_id": EXPECTED_KIS_RUN,
        "kis_source_repo": manifest["source_repo"],
        "kis_source_commit": manifest["source_commit"],
        "kis_csv_sha256": sha(csv_bytes),
        "kis_manifest_sha256": sha(manifest_bytes),
        "source_rows_rejected": len(raw) - source["source_row_count"],
    })
    (out / "R16_SOURCE_MANIFEST.json").write_text(
        json.dumps(source, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    return r16, source


def make_features(data: pd.DataFrame, t: int) -> dict:
    """Use only historical bars through the signal's D0 close."""
    x = data.iloc[t]
    before = data.iloc[:t]
    spread = float(x.high - x.low)
    def ratio_prior(column: str, lookback: int):
        if len(before) < lookback:
            return None
        baseline = float(before[column].iloc[-lookback:].mean())
        return float(x[column]) / baseline if baseline > 0 else None
    return {
        "close_location": float((x.close - x.low) / spread) if spread > 0 else None,
        "amount_vs_prev20": ratio_prior("amount", 20),
        "volume_vs_prev20": ratio_prior("volume", 20),
        "close_vs_ma224_pct": 100 * (float(x.close) / float(data.close.iloc[t-223:t+1].mean())-1)
            if t >= 223 else None,
        "return_prev20_pct": 100 * (float(x.close) / float(data.close.iloc[t-20])-1)
            if t >= 20 else None,
        "breakout_vs_prior55_high_pct": 100*(float(x.close)/float(before.high.iloc[-55:].max())-1)
            if len(before) >= 55 and float(before.high.iloc[-55:].max()) > 0 else None,
    }


def exit_close_next_open(data: pd.DataFrame, t: int, days: int, kind: str) -> dict:
    """Decision on prior daily close, execution at following OPEN; no lookahead."""
    assert kind in ("D0_LOW_NEXT_OPEN", "PREV5_LOW_NEXT_OPEN", "PREV10_LOW_NEXT_OPEN")
    assert t + days < len(data)
    entry = float(data.iloc[t+1].open)
    assert entry > 0
    for i in range(t+1, t+days):
        if kind == "D0_LOW_NEXT_OPEN":
            threshold = float(data.iloc[t].low)
        else:
            n = 5 if kind == "PREV5_LOW_NEXT_OPEN" else 10
            threshold = float(data.low.iloc[max(0, i-n):i].min())
        if float(data.iloc[i].close) < threshold:
            return {
                "net_pct": round(100*(float(data.iloc[i+1].open)/entry-1)-COST_PERCENT, 6),
                "exit_day": i+1-t, "exit_reason": "PRIOR_CLOSE_BELOW_ASOF_STRUCTURE",
            }
    return {"net_pct": round(100*(float(data.iloc[t+days].close)/entry-1)-COST_PERCENT, 6),
            "exit_day": days, "exit_reason": "WINDOW_END_CLOSE"}


def describe(values: list[dict], model: str) -> dict:
    vals = [x["outcomes"][model]["net_pct"] for x in values]
    if not vals:
        return {"n": 0, "mean_net_pct": None, "median_net_pct": None}
    s = sorted(vals)
    return {
        "n": len(vals),
        "mean_net_pct": round(statistics.mean(vals), 6),
        "median_net_pct": round(statistics.median(vals), 6),
        "win_rate_pct": round(100*sum(v>0 for v in vals)/len(vals), 3),
        "p10_net_pct": round(s[int((len(s)-1)*.10)], 6),
        "min_net_pct": round(min(vals), 6),
        "mean_excluding_best_pct": round(statistics.mean(s[:-1]),6) if len(s)>1 else None,
        "median_exit_day": statistics.median(x["outcomes"][model]["exit_day"] for x in values),
    }


def first_passage_atr(data: pd.DataFrame, t: int, horizon: int = 20) -> str:
    """An explanatory ex-post path label, never a tradable signal or feature.

    First hit of +/- one pre-entry ATR from D+1 OPEN. Simultaneous daily
    high/low hits are AMBIGUOUS and must never be ordered heuristically.
    """
    prev_close = data.close.shift(1)
    tr = pd.concat((data.high-data.low, (data.high-prev_close).abs(),
                    (data.low-prev_close).abs()),axis=1).max(axis=1)
    atr = float(tr.iloc[t-13:t+1].mean()) if t>=13 else 0
    if atr<=0:
        return "UNKNOWN_ATR"
    entry = float(data.iloc[t+1].open)
    for i in range(t+1, t+horizon+1):
        hi = float(data.iloc[i].high)>=entry+atr
        lo = float(data.iloc[i].low)<=entry-atr
        if hi and lo:
            return "AMBIGUOUS_SAME_DAY_UP_DOWN"
        if hi:
            return "UPSIDE_ONE_ATR_FIRST"
        if lo:
            return "DOWNSIDE_ONE_ATR_FIRST"
    return "NEITHER_IN_WINDOW"


def frozen_eligible(rec: dict) -> bool:
    return (rec["status"] == "EVALUATED_R14_FROZEN_POLICY" and
            all(rec["horizons"][str(h)][model]["exit"]["status"].startswith("PROVISIONAL_")
                for h in (20,60) for model in (
                    "LINE_AND_CONFIRMED_PIVOT_INTRADAY",
                    "LINE_AND_CONFIRMED_PIVOT_CLOSE_NEXT_OPEN")))


def median_training_cutoffs(rows: list[dict]) -> dict:
    # One preregistered median per D0 feature using 2021–23 only.
    train = [r for r in rows if r["date"] <= TRAIN_END]
    return {feature: statistics.median(float(r["features"][feature]) for r in train
                                       if r["features"][feature] is not None)
            for feature in FEATURES}


def split_diagnostics(rows: list[dict], cutoffs: dict) -> dict:
    # Descriptive differences only; no coefficient tuning or winner selection.
    result = {}
    for subset, filtered in (("TRAIN_2021_2023",[r for r in rows if r["date"]<=TRAIN_END]),
                             ("TEMPORAL_2024_2025",[r for r in rows if r["date"]>=TEST_START])):
        result[subset] = {}
        for name, threshold in cutoffs.items():
            result[subset][name] = {"training_median_cutoff": threshold, "groups": {}}
            for side, rs in (("at_or_below", [r for r in filtered if r["features"][name] is not None
                              and float(r["features"][name]) <= threshold]),
                             ("above", [r for r in filtered if r["features"][name] is not None
                              and float(r["features"][name]) > threshold])):
                result[subset][name]["groups"][side] = {
                    "n":len(rs), "r14_mean_D60_net_pct": describe(rs,"R14_INTRADAY")["mean_net_pct"],
                    "dynamic_prev5_mean_D60_net_pct":describe(rs,"PREV5_LOW_NEXT_OPEN")["mean_net_pct"],
                    "downside_ATR_first_n":sum(r["first_passage_20"] == "DOWNSIDE_ONE_ATR_FIRST" for r in rs),
                    "upside_ATR_first_n":sum(r["first_passage_20"] == "UPSIDE_ONE_ATR_FIRST" for r in rs),
                    "ambiguous_path_n":sum(r["first_passage_20"] == "AMBIGUOUS_SAME_DAY_UP_DOWN" for r in rs),
                }
    return result


def analyze(repo: Path, r16_out: Path, out: Path) -> dict:
    r16_manifest = json.loads((r16_out/"R16_SOURCE_MANIFEST.json").read_text())
    assert r16_manifest.get("kis_run_id",r16_manifest.get("kis_source_run_id")) == EXPECTED_KIS_RUN
    source = r16_out / "R16_FROZEN_HOLDOUT_UNADJUSTED_OHLCV.csv.gz"
    assert sha(source.read_bytes()) == r16_manifest["raw_archive_sha256"]
    raw = pd.read_csv(source,dtype={"code":str,"date":str})
    assert len(raw) == r16_manifest["source_row_count"]
    frozen = json.loads((r16_out/"R16_HOLDOUT_FULL_AUDIT.json").read_text())
    assert frozen["price_archive_sha256"] == r16_manifest["raw_archive_sha256"]
    assert frozen["combined"]["n_complete_same_events_D20_and_D60"] == 425
    price = {code:group.sort_values("date").reset_index(drop=True)
             for code,group in raw.groupby("code",sort=False)}
    date_index = {code:{date:i for i,date in enumerate(frame.date)} for code,frame in price.items()}
    rows=[]
    for rec in frozen["records"]:
        if not frozen_eligible(rec):
            continue
        code=rec["code"];t=date_index[code][rec["date"]];df=price[code]
        assert t+HORIZON<len(df)
        outcomes={}
        for r17_name, original_name in (
            ("R14_INTRADAY", "LINE_AND_CONFIRMED_PIVOT_INTRADAY"),
            ("R14_CLOSE_NEXT_OPEN","LINE_AND_CONFIRMED_PIVOT_CLOSE_NEXT_OPEN")):
            prior=rec["horizons"]["60"][original_name]["exit"]
            outcomes[r17_name]={"net_pct":float(prior["net_total20bps_pct"]),
                                "exit_day":int(prior["exit_day"]),
                                "exit_reason":prior["exit_reason"]}
        for kind in MODELS[2:]:
            outcomes[kind]=exit_close_next_open(df,t,HORIZON,kind)
        rows.append({"code":code,"date":rec["date"],"year":rec["year"],
                     "scale":rec["scale"],"features":make_features(df,t),
                     "first_passage_20":first_passage_atr(df,t),
                     "outcomes":outcomes})
    assert len(rows)==425
    cuts=median_training_cutoffs(rows)
    report={
        "revision":"R17_CAUSAL_FAILURE_PATH_AND_ADAPTIVE_STRUCTURE_EXIT_DESCRIPTIVE_ONLY",
        "source_kis_run":EXPECTED_KIS_RUN,"source_price_sha256":r16_manifest["raw_archive_sha256"],
        "r16_complete_same_cohort":425,"r17_events":len(rows),
        "cost_roundtrip_pct":COST_PERCENT,"entry":"DPLUS1_OPEN_AFTER_D0_CLOSE_SIGNAL",
        "entry_is_not_same_day_auction":True,
        "structure_exit_execution":"CLOSE_SIGNAL_FOLLOWING_DAY_OPEN_NO_INTRADAY_ORDER_ASSUMPTION",
        "frozen_r14_exits_unmodified":True,"no_pct_stop_or_profit_target_search":True,
        "source_provider_PIT_corporate_actions_certified":False,
        "full_market_random_sample":False,"prospective_out_of_sample":False,
        "temporal_split_is_pristine_OOS":False,
        "no_strategy_winner_selected":True,"can_publish_trade_edge":False,
        "live_search_watch_unchanged":True,"orders_placed":0,
        "models":{k:describe(rows,k) for k in MODELS},
        "by_year":{year:{k:describe([r for r in rows if r["year"]==int(year)],k)
                         for k in MODELS}
                   for year in sorted({str(r["year"]) for r in rows})},
        "first_passage_20_counts":{k:sum(r["first_passage_20"]==k for r in rows)
                 for k in sorted({r["first_passage_20"] for r in rows})},
        "training_median_cutoffs":cuts,
        "predeclared_feature_groups":split_diagnostics(rows,cuts),
    }
    out.mkdir(parents=True,exist_ok=True)
    (out/"R17_SUMMARY.json").write_text(json.dumps(report,indent=2,ensure_ascii=False,allow_nan=False)+"\n")
    (out/"R17_EVENTS.json").write_text(json.dumps(rows,indent=2,ensure_ascii=False,allow_nan=False)+"\n")
    return report


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repo",default=str(ROOT))
    p.add_argument("--artifact",help="BLUE R16 capture ZIP or extracted artifact directory")
    p.add_argument("--r16-out",required=True)
    p.add_argument("--out",required=True)
    p.add_argument("--phase",choices=("all","analyze"),default="all")
    args=p.parse_args()
    repo=Path(args.repo).resolve();r16_out=Path(args.r16_out).resolve();out=Path(args.out).resolve()
    if args.phase=="all":
        if not args.artifact:
            p.error("--artifact is required for import")
        r16,_ = import_kis(repo,Path(args.artifact),r16_out)
        r16.evaluate(repo,r16_out)
    report=analyze(repo,r16_out,out)
    print("R17_RESULT",json.dumps({"n":report["r17_events"],"models":report["models"],
                                     "first_passage":report["first_passage_20_counts"]},ensure_ascii=False))


if __name__=="__main__":
    main()
