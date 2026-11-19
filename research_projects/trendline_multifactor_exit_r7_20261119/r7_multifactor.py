#!/usr/bin/env python3
"""R7 RESEARCH_ONLY: causal ATR / price-structure / volume exit diagnostics.

Input is the SHA-pinned three-stock R5/R6 population. No new entry universe,
no strategy promotion, no volume-at-price claims from daily bars. Each decision
uses only known prices as of the preceding close; any stop checked within a
daily OHLC bar was fixed before that session opened.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import statistics
import sys
from collections import Counter
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
R6_SCRIPT_SHA256 = "53044200c87fafd114e1929f1347c9445171e61f39e72a5b1dc70f3ff1d42911"
HORIZON = 20
COST_BPS = 20
ATR_PERIOD = 14
VOLUME_LOOKBACK = 20
STRUCTURE_LOOKBACK = 10
VOLUME_MULT = 1.5
ATR_MULT_INITIAL = 3.0
ATR_MULT_CHANDELIER = 3.0
ATR_MULT_WEAKNESS = 2.5

# Parameters fixed in source, not chosen after seeing R7 results.
RULES = (
    "R6_TIME_D20_BASELINE",
    "ATR_INITIAL_3",
    "ATR_CHANDELIER_3",
    "STRUCTURE_PRIOR10_CLOSE_NEXT_OPEN",
    "VOLUME_DOWN_1_5_CLOSE_NEXT_OPEN",
    "CONFLUENCE_2OF3_CLOSE_NEXT_OPEN",
)


def frozen_r6(repo: Path = ROOT):
    source = repo / "research_projects/trendline_exit_timing_r6_20261119/r6_exit_timing.py"
    if hashlib.sha256(source.read_bytes()).hexdigest() != R6_SCRIPT_SHA256:
        raise ValueError("CANONICAL_R6_SOURCE_SHA256_MISMATCH")
    sys.path.insert(0, str(source.parent))
    import r6_exit_timing
    return r6_exit_timing


def wilder_atr(data: pd.DataFrame, n: int = ATR_PERIOD) -> pd.Series:
    """Wilder RMA, seeded by SMA of first n true ranges; no future observations."""
    if n < 1:
        raise ValueError("ATR_PERIOD_MUST_BE_POSITIVE")
    result = [math.nan] * len(data)
    true_ranges = []
    previous_close = None
    for k, row in enumerate(data.itertuples(index=False)):
        high, low, close = float(row.high), float(row.low), float(row.close)
        if min(high, low, close) <= 0 or high < low:
            raise ValueError("INVALID_ATR_PRICE")
        tr = high - low if previous_close is None else max(
            high - low, abs(high - previous_close), abs(low - previous_close))
        true_ranges.append(tr)
        if k == n - 1:
            result[k] = sum(true_ranges[:n]) / n
        elif k >= n:
            result[k] = ((n - 1) * result[k-1] + tr) / n
        previous_close = close
    return pd.Series(result, index=data.index, dtype="float64")


def _valid(value) -> bool:
    return value is not None and math.isfinite(float(value)) and float(value) > 0


def _pct(top: float, bottom: float) -> float:
    return round(100. * (top / bottom - 1.), 6)


def feature_at_d0(data: pd.DataFrame, t: int, atr: pd.Series) -> dict:
    """Frozen pre-entry inputs: D0 is visible only AFTER its close."""
    prior20 = data.iloc[max(t - VOLUME_LOOKBACK, 0):t]
    prior60 = data.iloc[max(t - 60, 0):t]
    if not _valid(atr.iloc[t]) or len(prior20) < 20:
        return {"status":"UNKNOWN_INSUFFICIENT_PRE_D0_HISTORY"}
    vol20 = float(prior20.volume.mean())
    amount20 = float(prior20.amount.mean())
    d0 = data.iloc[t]
    return {
        "status":"KNOWN_AFTER_D0_CLOSE",
        "atr14_wilder_d0":round(float(atr.iloc[t]), 6),
        "atr14_pct_of_d0_close":round(100 * float(atr.iloc[t]) / float(d0.close), 6),
        "d0_volume_div_pre20_avg":round(float(d0.volume) / vol20, 6) if vol20>0 else None,
        "d0_amount_div_pre20_avg":round(float(d0.amount) / amount20, 6) if amount20>0 else None,
        "pre20_high":float(prior20.high.max()),
        "pre20_low":float(prior20.low.min()),
        "pre60_high":float(prior60.high.max()) if len(prior60)==60 else None,
        "pre60_low":float(prior60.low.min()) if len(prior60)==60 else None,
        "d0_close_vs_pre20_high_pct":_pct(float(d0.close), float(prior20.high.max())),
        "actual_volume_at_price":"NOT_AVAILABLE_FROM_DAILY_OHLCV",
        "asof":"AFTER_D0_CLOSE_NO_FUTURE_BARS",
    }


def components_at_close(data: pd.DataFrame, t: int, h: int,
                        atr: pd.Series, line_level=None, line_slope=None) -> dict:
    """Signals for decision AFTER D+h close; if used, sell D+h+1 OPEN.

    Structure signal is close below prior 10 completed low OR below same frozen
    R5 line. Volume is strictly a down-close/high-volume proxy; it is NOT
    seller-initiated flow. ATR weakness uses prior-complete bars and ATR.
    """
    i = t + h
    prior10 = data.iloc[i-STRUCTURE_LOOKBACK:i]
    prior20 = data.iloc[i-VOLUME_LOOKBACK:i]
    if len(prior10) < STRUCTURE_LOOKBACK or len(prior20) < VOLUME_LOOKBACK:
        return {"status":"UNKNOWN_HISTORY"}
    prior_atr = float(atr.iloc[i-1])
    if not _valid(prior_atr):
        return {"status":"UNKNOWN_ATR"}
    bar = data.iloc[i]
    previous_close = float(data.iloc[i-1].close)
    price_structure_broken = float(bar.close) < float(prior10.low.min())
    original_line_relost = (
        line_level is not None and line_slope is not None and
        float(bar.close) < (float(line_level)+h*float(line_slope)) * .996
    )
    # A frozen line is one kind of pre-known structure; either breakdown counts.
    structure = price_structure_broken or original_line_relost
    vol_avg = float(prior20.volume.mean())
    high_volume_decline = (
        vol_avg > 0 and float(bar.volume) >= VOLUME_MULT * vol_avg and
        float(bar.close) < previous_close and float(bar.close) < float(bar.open)
    )
    prior_peak = max(float(x) for x in data.iloc[t+1:i].high) if h>1 else float(data.iloc[t+1].open)
    previous_entry = float(data.iloc[t+1].open)
    peak_before_today = max(previous_entry, prior_peak)
    atr_weak = float(bar.close) < peak_before_today - ATR_MULT_WEAKNESS * prior_atr
    return {
        "status":"KNOWN_AFTER_DPLUS_CLOSE",
        "structure":bool(structure),
        "prior10_low_break":bool(price_structure_broken),
        "frozen_line_relost":bool(original_line_relost),
        "volume_down":bool(high_volume_decline),
        "atr_weakness":bool(atr_weak),
        "volume_ratio_to_pre20":round(float(bar.volume)/vol_avg, 6) if vol_avg>0 else None,
        "atr14_asof_previous_close":round(prior_atr, 6),
        "signal_count":int(bool(structure))+int(bool(high_volume_decline))+int(bool(atr_weak)),
        "asof":f"AFTER_DPLUS_{h}_CLOSE",
    }


def _exit(data: pd.DataFrame, t: int, h: int, entry: float,
          price: float, reason: str, **extra) -> dict:
    gross = _pct(price, entry)
    return {
        "status":"PROVISIONAL_DAILY_BAR_3_STOCK_NOT_OOS",
        "exit_day":h, "exit_date":str(data.iloc[t+h].date.date()),
        "exit_price":round(float(price), 6), "reason":reason,
        "gross_pct":gross, "net_total20bps_pct":round(gross-COST_BPS/100, 6),
        "same_day_high_low_order_known":False,
        **extra,
    }


def simulate(data: pd.DataFrame, t: int, atr: pd.Series,
             rule: str, *, line_level=None, line_slope=None) -> dict:
    if rule not in RULES:
        raise ValueError("UNREGISTERED_R7_EXIT_RULE")
    if len(data.iloc[t+1:t+HORIZON+1]) < HORIZON:
        return {"status":"UNKNOWN_INCOMPLETE_DPLUS20"}
    entry = float(data.iloc[t+1].open)
    if entry<=0:
        return {"status":"UNKNOWN_ENTRY_PRICE"}
    if rule=="R6_TIME_D20_BASELINE":
        return _exit(data, t, HORIZON, entry,
                     float(data.iloc[t+HORIZON].close), "TIME_D20_CLOSE")
    if not _valid(atr.iloc[t]) or t < VOLUME_LOOKBACK:
        return {"status":"UNKNOWN_PRE_ENTRY_FEATURE"}
    atr_d0 = float(atr.iloc[t])
    stop_floor = entry - ATR_MULT_INITIAL * atr_d0
    highest_prior = entry
    previous_stop = stop_floor

    for h in range(1,HORIZON+1):
        i=t+h
        row=data.iloc[i]
        op, hi, lo, close=(float(row[k]) for k in ("open","high","low","close"))
        if min(op, hi, lo, close) <= 0:
            return {"status":"UNKNOWN_INVALID_DAILY_BAR"}
        if rule in ("ATR_INITIAL_3","ATR_CHANDELIER_3"):
            stop=stop_floor
            if rule=="ATR_CHANDELIER_3":
                previous_atr=float(atr.iloc[i-1])
                if not _valid(previous_atr):
                    return {"status":"UNKNOWN_PREVIOUS_ATR"}
                # NEVER incorporate today's high or ATR into today's stop.
                stop=max(previous_stop,highest_prior-ATR_MULT_CHANDELIER*previous_atr)
                previous_stop=stop
            if op<=stop:
                return _exit(data,t,h,entry,op,"GAP_BELOW_PRESET_STOP_OPEN",
                             stop_before_open=round(stop,6),gap_stop=True)
            if lo<=stop:
                return _exit(data,t,h,entry,stop,"PRESET_ATR_STOP_INTRADAY",
                             stop_before_open=round(stop,6),gap_stop=False)
            highest_prior=max(highest_prior,hi)
        else:
            if h == HORIZON:
                break  # Never act on an unobservable D+21 opening.
            components=components_at_close(data,t,h,atr,line_level,line_slope)
            if components["status"]!="KNOWN_AFTER_DPLUS_CLOSE":
                return {"status":"UNKNOWN_CLOSE_COMPONENT"}
            if rule=="STRUCTURE_PRIOR10_CLOSE_NEXT_OPEN":
                signal=components["structure"]
            elif rule=="VOLUME_DOWN_1_5_CLOSE_NEXT_OPEN":
                signal=components["volume_down"]
            else:
                signal=components["signal_count"]>=2
            if signal:
                return _exit(data,t,h+1,entry,float(data.iloc[i+1].open),
                             "PRIOR_CLOSE_SIGNAL_NEXT_OPEN",
                             trigger_day=h,trigger_asof=components["asof"],
                             triggers={k:components[k] for k in (
                                 "structure","volume_down","atr_weakness")})
    return _exit(data,t,HORIZON,entry,
                 float(data.iloc[t+HORIZON].close),"TIME_D20_FALLBACK")


def stats(rows: list[dict]) -> dict:
    good=[x for x in rows if x["status"].startswith("PROVISIONAL_")]
    values=[x["net_total20bps_pct"] for x in good]
    sorted_values=sorted(values)
    return {
        "input_count":len(rows),"complete_count":len(good),
        "unknown_count":len(rows)-len(good),
        "mean_pct":round(statistics.mean(values),6) if values else None,
        "median_pct":round(statistics.median(values),6) if values else None,
        "p10_pct":round(sorted_values[max(0,int((len(values)-1)*.1))],6) if values else None,
        "win_count":sum(v>0 for v in values),
        "median_exit_day":round(statistics.median(x["exit_day"] for x in good),2) if good else None,
        "gap_stop_count":sum(bool(x.get("gap_stop")) for x in good),
        "reason_counts":dict(Counter(x["reason"] for x in good)),
    }


def nonoverlap_by_stock(records: list[dict], horizon: int=HORIZON) -> list[dict]:
    """Earliest-first 20-bar embargo; no future returns or outcomes consulted."""
    kept=[]
    latest_end={}
    for e in sorted(records,key=lambda x:(x["code"],x["idx"],x["line_id"])):
        if int(e["idx"])<=latest_end.get(e["code"],-1):
            continue
        kept.append(e)
        latest_end[e["code"]]=int(e["idx"])+horizon
    return kept


def study(repo: Path = ROOT) -> dict:
    r6=frozen_r6(repo)
    frozen=r6.study(repo)
    r5=r6.r5_module(repo)
    prepare=r5._modules(repo)[0]
    data={code:prepare(r5.frozen_data(repo,code)) for code in r5.SOURCES}
    atr={code:wilder_atr(prices) for code,prices in data.items()}
    primary=r6.primary_events(frozen["events"])
    nonoverlap_keys={
        (e["code"],e["date"],e["line_id"])
        for e in nonoverlap_by_stock(primary)
    }
    records=[]
    for e in primary:
        code=e["code"]; idx=int(e["idx"])
        baseline=e["exit_models"]["TIME_D20"]
        features=feature_at_d0(data[code],idx,atr[code])
        model={
            r:simulate(data[code],idx,atr[code],r,
                       line_level=e.get("break_line_level_d0"),
                       line_slope=e.get("line_slope_per_bar"))
            for r in RULES
        }
        if baseline["status"].startswith("PROVISIONAL_"):
            if abs(model["R6_TIME_D20_BASELINE"]["net_total20bps_pct"]-
                   baseline["net_total20bps_pct"])>1e-6:
                raise AssertionError("R6_R7_DPLUS20_RECONCILIATION_FAILURE")
        else:
            assert model["R6_TIME_D20_BASELINE"]["status"].startswith("UNKNOWN")
        records.append({
            "code":code,"date":e["date"],"year":int(e["date"][:4]),
            "idx":idx,"scale":e["scale"],"line_id":e["line_id"],
            "entry_date":e.get("entry_date"),
            "entry_open":e.get("entry_dplus1_open"),
            "nonoverlap_20d_earliest_first":(code,e["date"],e["line_id"]) in nonoverlap_keys,
            "features_D0":features,
            "R6_peak_EXPOST":e["opportunity_EXPOST"],
            "models":model,
        })
    assert len(primary)==368 and len(frozen["events"])==506
    def grouped(sample):
        # R6 baseline has 363 complete paths, but four of those have no usable
        # pre-entry feature history for the new models. Any model comparison
        # must use the identical event set; never compare mismatched averages.
        common=[e for e in sample if all(
            e["models"][rule]["status"].startswith("PROVISIONAL_")
            for rule in RULES
        )]
        paired={
            rule:round(statistics.mean(
                e["models"][rule]["net_total20bps_pct"] -
                e["models"]["R6_TIME_D20_BASELINE"]["net_total20bps_pct"]
                for e in common
            ),6) if common else None
            for rule in RULES
        }
        return {
            "events":len(sample),
            "rules":{r:stats([e["models"][r] for e in sample]) for r in RULES},
            "same_events_only":{
                "n":len(common),
                "rule_stats":{rule:stats([e["models"][rule] for e in common])
                              for rule in RULES},
                "mean_difference_vs_d20_percentage_points":paired,
            },
            "features_ready":sum(e["features_D0"]["status"]=="KNOWN_AFTER_D0_CLOSE" for e in sample),
        }
    clustered=[r for r in records if r["nonoverlap_20d_earliest_first"]]
    return {
        "revision":"R7_CAUSAL_ATR_VOLUME_STRUCTURE_3STOCK_PROVISIONAL",
        "source_revision":frozen["revision"],
        "research_only":True,
        "predeclared_params":{
            "atr":"WILDER_RMA_14_TRUE_RANGE_WITH_DAY0_HL_SEED",
            "volume_pre20_avg":VOLUME_LOOKBACK,
            "structure_previous_low":STRUCTURE_LOOKBACK,
            "high_volume_down_ratio":VOLUME_MULT,
            "initial_atr_multiple":ATR_MULT_INITIAL,
            "chandelier_atr_multiple":ATR_MULT_CHANDELIER,
            "atr_weakness_multiple":ATR_MULT_WEAKNESS,
            "composite":"AT_LEAST_2_OF_STRUCTURE_VOLUME_DOWN_ATR_WEAKNESS",
            "stop_at_start_of_day":"PRIOR_COMPLETED_HIGH_AND_ATR_ONLY",
            "close_signal_execution":"NEXT_TRADING_DAY_OPEN",
            "fallback":"DPLUS20_CLOSE",
            "entry":"DPLUS1_OPEN",
            "total_cost_bps":COST_BPS,
        },
        "rules":list(RULES),
        "source_population":"FROZEN_3_HANDPICKED_STOCKS_2021_TO_2024",
        "stock_codes":sorted(r5.SOURCES),
        "unique_stock_dates":len(records),
        "nonoverlap_20d_count":len(clustered),
        "overall_primary":grouped(records),
        "by_code":{c:grouped([e for e in records if e["code"]==c]) for c in r5.SOURCES},
        "by_year":{str(y):grouped([e for e in records if e["year"]==y])
                   for y in range(2021,2025)},
        "nonoverlap_20d_primary":grouped(clustered),
        "volume_at_price_actual":"UNAVAILABLE_DAILY_OHLCV_NOT_VOLUME_PROFILE",
        "transaction_direction_from_volume":"UNKNOWN_NOT_INFERRED",
        "marketwide_point_in_time_universe":False,
        "independent_price_and_adjustment_history_certified":False,
        "intraday_high_low_order_certified":False,
        "out_of_sample":False,
        "strategy_winner":None,
        "can_publish_strategy_performance":False,
        "can_modify_live_search_watch":False,
        "env_lower_touch_events_merged":False,
        "records":records,
    }


def run(repo: Path, out: Path) -> dict:
    result=study(repo)
    out.mkdir(parents=True,exist_ok=True)
    (out/"R7_FULL_AUDIT.json").write_text(json.dumps(
        result,ensure_ascii=False,indent=2,allow_nan=False)+"\n",encoding="utf-8")
    summary={k:v for k,v in result.items() if k!="records"}
    (out/"R7_SUMMARY.json").write_text(json.dumps(
        summary,ensure_ascii=False,indent=2,allow_nan=False)+"\n",encoding="utf-8")
    rows=[]
    for e in result["records"]:
        for rule in RULES:
            m=e["models"][rule]
            rows.append({
                "code":e["code"],"signal_date":e["date"],"line_id":e["line_id"],
                "scale":e["scale"],"year":e["year"],
                "nonoverlap_20d":e["nonoverlap_20d_earliest_first"],
                "entry_date":e["entry_date"],"entry_open":e["entry_open"],
                "atr14_d0":e["features_D0"].get("atr14_wilder_d0"),
                "d0_vol_pre20_ratio":e["features_D0"].get("d0_volume_div_pre20_avg"),
                "rule":rule,"status":m["status"],"exit_day":m.get("exit_day"),
                "exit_date":m.get("exit_date"),"reason":m.get("reason"),
                "net20bps_pct":m.get("net_total20bps_pct"),
                "peak_day_EXPOST":e["R6_peak_EXPOST"].get("peak_day"),
                "mfe_gross_pct_EXPOST":e["R6_peak_EXPOST"].get("mfe_gross_pct"),
            })
    pd.DataFrame(rows).to_csv(out/"R7_PRIMARY_EVENTS_AND_MODELS.csv",
                              index=False,encoding="utf-8-sig")
    return result


if __name__=="__main__":
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repo",default=".")
    p.add_argument("--out",required=True)
    args=p.parse_args()
    result=run(Path(args.repo).resolve(),Path(args.out))
    print(json.dumps({k:v for k,v in result.items() if k not in
                      ("records","by_code","by_year")},ensure_ascii=False,indent=2))
