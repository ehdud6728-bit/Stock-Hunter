"""BLUE20 repeated resistance experiment. All decisions use completed sessions."""
from __future__ import annotations

import argparse
import json
import shutil
import sys
from hashlib import sha256
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
WINDOWS = (60, 120, 240)
VARIANTS = [(n, method) for n in WINDOWS for method in ("range", "typical")]
PRIMARY = (120, "range")
KEY = ["code", "date", "band"]
IDENTITY = KEY + ["gc_date", "entry_date", "baseline_exit_date", "baseline_exit_reason"]
CASES = {
    ("028300", "2023-02-20"): "HLB",
    ("145990", "2023-05-08"): "삼양사",
    ("383220", "2024-07-17"): "F&F",
    ("180640", "2023-09-14"): "한진칼",
    ("006220", "2024-01-24"): "제주은행",
    ("222080", "2024-03-05"): "씨아이에스",
    ("038500", "2024-06-05"): "삼표시멘트",
    ("034120", "2023-02-07"): "SBS",
}


def identity_digest(base):
    canonical = base.sort_values(KEY)[IDENTITY].to_csv(index=False, lineterminator="\n")
    return sha256(canonical.encode()).hexdigest()


def rejection_tests(window, lower, upper):
    """Require an approach from below, a failed close and a completed rejection."""
    h, c, a = (window[k].to_numpy() for k in ("high", "close", "prior_atr20"))
    tests = []
    for i in np.flatnonzero((h[1:-3] >= lower) & (h[1:-3] <= upper + a[1:-3] * .25)
                            & (c[:-4] < lower) & (c[1:-3] <= upper)) + 1:
        if tests and i - tests[-1][0] < 5:
            continue
        forward = c[i+1:i+4]
        # A close above the zone ends this rejection attempt.
        cleared = np.flatnonzero(forward > upper)
        if len(cleared):
            forward = forward[:cleared[0]]
        if not len(forward) or not np.isfinite(a[i]) or a[i] <= 0:
            continue
        depth = (h[i] - forward.min()) / a[i]
        if depth >= .5:
            tests.append((int(i), float(depth), float(100 * (1 - forward.min() / h[i]))))
    return tests


def resistance(history, bar, n, method, profile_fn):
    facts = dict(coverage=False, zone_low=np.nan, zone_high=np.nan, tests=0,
                 rejection_atr=np.nan, rejection_pct=np.nan, mass_pct=np.nan,
                 overlap=np.nan, progress_atr=np.nan, strong=False, block=False,
                 test_dates="", rejection_atrs="", rejection_pcts="")
    body = bar.close - bar.open
    atr = bar.prior_atr20
    large = np.isfinite(atr) and atr > 0 and body >= 2 * atr
    facts["large_bull"] = bool(large)
    facts["body_atr"] = body / atr if atr > 0 else np.nan
    if len(history) < n or not np.isfinite(atr) or atr <= 0:
        return facts, None
    window = history.iloc[-n:]
    profile = profile_fn(window, method=method)
    if profile is None:
        return facts, None
    facts["coverage"] = True
    weights, centers, width = (profile[k] for k in ("weights", "centers", "width"))
    candidates = []
    body_low, body_high = sorted((bar.open, bar.close))
    for i, center in enumerate(centers):
        lower, upper = center - width / 2, center + width / 2
        if upper < body_low or lower > max(body_high, bar.close + atr):
            continue
        mass = weights[i] / weights.sum()
        if mass < 1.5 / len(weights):
            continue
        tests = rejection_tests(window, lower, upper)
        depth = float(np.median([t[1] for t in tests])) if tests else np.nan
        strong = len(tests) >= 2 and depth >= .75
        overlap = max(0., min(body_high, upper) - max(body_low, lower))
        ratio = overlap / (body_high - body_low) if body_high > body_low else 0.
        progress = (bar.close - upper) / atr
        block = bool(large and strong and ratio >= .20 and progress < .5)
        score = (block, strong, ratio >= .20, -abs(progress), mass)
        candidates.append((score, dict(zone_low=lower, zone_high=upper, tests=len(tests),
                            rejection_atr=depth, mass_pct=100 * mass, overlap=ratio,
                            progress_atr=progress, strong=strong, block=block,
                            rejection_pct=float(np.median([t[2] for t in tests])) if tests else np.nan,
                            test_dates="|".join(str(window.date.iloc[t[0]]) for t in tests),
                            rejection_atrs="|".join(f"{t[1]:.4f}" for t in tests),
                            rejection_pcts="|".join(f"{t[2]:.4f}" for t in tests))))
    if candidates:
        facts.update(max(candidates, key=lambda item: item[0])[1])
    return facts, profile


def confirmations(d, j, facts, band):
    """Return close-based decisions, with a separate daily evidence ledger."""
    upper = facts["zone_high"]
    if not np.isfinite(upper):
        return None, None, "", []
    atr = float(d.prior_atr20.iloc[j])
    lower = facts["zone_low"]
    level = f"bb20_{band.lower()}_shift26"
    streak = 0
    crossed = False
    breakout_amount = np.nan
    settle = pullback = None
    route = ""
    daily = []
    for k in range(j, min(j+21, len(d))):
        row = d.iloc[k]
        above = row.close >= upper + .25 * atr
        streak = streak + 1 if above else 0
        if row.close < lower:
            crossed = False
        support = bool(crossed and lower <= row.low <= upper + .25 * atr
                       and row.close > upper and row.close > row.open)
        pull = bool(support and row.amount < breakout_amount)
        recross = bool(k > j and row.open < row[level] < row.close
                       and d.close.iloc[k-1] <= d[level].iloc[k-1] and above)
        if j < k <= j+10:
            if settle is None and streak >= 2:
                settle = k
            if pullback is None and (pull or recross):
                pullback, route = k, "PULLBACK" if pull else "NEW_BAND_SIGNAL"
        if above and not crossed:
            crossed, breakout_amount = True, row.amount
        body_low, body_high = sorted((row.open, row.close))
        overlap = max(0., min(body_high, upper) - max(body_low, lower))
        progress = (row.close-d.close.iloc[k-1]) / atr if k else np.nan
        daily.append(dict(date=row.date, offset=k-j, close=row.close,
                          close_progress_atr=(row.close-upper)/atr,
                          above_streak=streak, support=support, pullback=pull,
                          new_band_signal=recross, amount_ratio20=row.amount_ratio20,
                          body_zone_overlap=overlap/(body_high-body_low) if body_high > body_low else 0.,
                          upper_wick=getattr(row, "upper_wick_to_range", np.nan),
                          ma60_slope10=getattr(row, "ma60_slope10", np.nan),
                          ma112_slope10=getattr(row, "ma112_slope10", np.nan),
                          efficiency_atr_per_turnover=progress/row.amount_ratio20
                          if row.amount_ratio20 > 0 else np.nan))
    return settle, pullback, route, daily


def trade(d, j, decision, route, simulate):
    if decision is None:
        return dict(route=route, entered=False, net_pct=0., exit_reason="NO_TRADE")
    entry = decision + 1
    price = float(d.open.iloc[entry])
    result = simulate(d, j, entry, price, False)
    exit_index = int(d.index[d.date.eq(result["exit_date"])][0])
    # Completed closes through the day before exit, then the executable exit.
    marks = np.r_[price, d.close.iloc[entry:exit_index].to_numpy(), result["exit_price"]]
    drawdown = 100 * (marks / np.maximum.accumulate(marks) - 1).min()
    return dict(**result, entered=True, route=route, decision_date=d.date.iloc[decision],
                entry_date=d.date.iloc[entry], entry_offset=entry-j, entry_price=price,
                mdd_pct=drawdown, peak_pct=100 * (d.high.iloc[entry:j+21].max()/price-1))


def run(base, frames, profile_fn, simulate):
    all_rows, all_daily, chart_data = [], [], {}
    for code, group in base.groupby("code"):
        d = frames[code]
        index = {date: i for i, date in enumerate(d.date)}
        for e in group.itertuples(index=False):
            j, gc = index[e.date], index[e.gc_date]
            old = trade(d, j, gc, "ORIGINAL_RSI", simulate)
            if (old["entry_date"] != e.entry_date or old["exit_date"] != e.baseline_exit_date
                    or old["exit_reason"] != e.baseline_exit_reason
                    or not np.isclose(old["net_pct"], e.baseline_net_pct, atol=1e-6, rtol=0)
                    or not np.isclose(old["entry_price"], e.baseline_entry_price)
                    or not np.isclose(d.close.iloc[j], e.d0_close)
                    or not np.isclose(d.amount_ratio20.iloc[j], e.d0_amount_ratio20, atol=1e-5, rtol=0)):
                raise ValueError(f"Original trade mismatch {code} {e.date} {e.band}")
            bar = d.iloc[j]
            identity = dict(code=code, date=e.date, band=e.band, year=int(e.year),
                            gc_offset=gc-j, d20_date=d.date.iloc[j+20])
            fields = dict(d0_close=bar.close, d0_amount_ratio20=bar.amount_ratio20,
                          ma60_slope10=bar.ma60_slope10, ma112_slope10=bar.ma112_slope10,
                          upper_wick=bar.upper_wick_to_range,
                          d20_high=float(d.high.iloc[j+20]),
                          path_high=float(d.high.iloc[j+1:j+21].max()))
            a = trade(d, j, j, "D0", simulate)
            fields["opportunity_peak_pct"] = a["peak_pct"]
            for n, method in VARIANTS:
                facts, profile = resistance(d.iloc[:j], bar, n, method, profile_fn)
                c, delayed, route, daily = confirmations(d, j, facts, e.band)
                variant = dict(window=n, method=method)
                if (n, method) == PRIMARY:
                    all_daily.extend({**identity, "d0_date": e.date, **row} for row in daily)
                progress = (bar.close - d.close.iloc[j-1]) / bar.prior_atr20
                facts["efficiency_atr_per_turnover"] = (
                    progress / bar.amount_ratio20 if bar.amount_ratio20 > 0 else np.nan)
                facts["support_d1"] = bool(len(daily) > 1 and daily[1]["support"])
                facts["support_d2"] = bool(len(daily) > 2 and daily[2]["support"])
                if facts["block"]:
                    policies = dict(A=a, B=trade(d, j, None, "BLOCK", simulate),
                                    C=trade(d, j, c, "SETTLE", simulate),
                                    D=trade(d, j, delayed, route or "NO_PULLBACK_OR_SIGNAL", simulate))
                else:
                    policies = dict.fromkeys("ABCD", a)
                record = {**identity, **variant, **fields, **facts}
                if (n, method) == PRIMARY or ((n, method) == (240, "range") and facts["block"]):
                    chart_data[(code, e.date, e.band, n, method)] = (j, profile)
                for policy, order in {**policies, "RSI": old}.items():
                    all_rows.append(dict(**record, policy=policy, **order))
    return pd.DataFrame(all_rows), pd.DataFrame(all_daily), chart_data


def mark_cohorts(ledger):
    keys = ledger[KEY + ["d20_date"]].drop_duplicates().sort_values(["code", "date", "band"])
    same_day = keys.drop_duplicates(["code", "date"])
    end = {}
    kept = []
    for row in same_day.itertuples(index=False):
        if row.date > end.get(row.code, ""):
            kept.append((row.code, row.date, row.band))
            end[row.code] = row.d20_date
    result = ledger.copy()
    idx = pd.MultiIndex.from_frame(result[KEY])
    result["unique_day"] = idx.isin(pd.MultiIndex.from_frame(same_day[KEY]))
    result["nonoverlap"] = idx.isin(pd.MultiIndex.from_tuples(kept))
    return result


def summary(ledger):
    rows = []
    cohorts = {"all": ledger, "same_day": ledger[ledger.unique_day],
               "nonoverlap": ledger[ledger.nonoverlap],
               "gc0": ledger[ledger.gc_offset.eq(0)]}
    for cohort, data in cohorts.items():
        for band in ("UPPER", "LOWER", "ALL"):
            subset = data if band == "ALL" else data[data.band.eq(band)]
            for year in (2023, 2024, "ALL"):
                scope = subset if year == "ALL" else subset[subset.year.eq(year)]
                for (n, method, policy), group in scope.groupby(["window", "method", "policy"]):
                    entered = group[group.entered]
                    missed = group[~group.entered].opportunity_peak_pct
                    stops = entered.exit_reason.str.startswith("SL").sum()
                    rows.append(dict(cohort=cohort, band=band, year=year, window=n, method=method,
                                     policy=policy, n=len(group), coverage=int(group.coverage.sum()),
                                     blocked=int(group.block.sum()), entries=len(entered),
                                     entry_pct=100*len(entered)/len(group),
                                     net_opportunity_pct=group.net_pct.mean(), net_trade_pct=entered.net_pct.mean(),
                                     sl_trade_pct=100*stops/len(entered) if len(entered) else np.nan,
                                     sl_opportunity_pct=100*stops/len(group),
                                     missed10=int((missed >= 10-1e-9).sum()),
                                     missed20=int((missed >= 20-1e-9).sum()),
                                     mean_d20_high_vs_d0_pct=(100*(group.d20_high/group.d0_close-1)).mean(),
                                     mean_path_high_vs_d0_pct=(100*(group.path_high/group.d0_close-1)).mean(),
                                     mean_entry_to_d20_peak_pct=entered.peak_pct.mean(),
                                     mean_trade_mdd_pct=entered.mdd_pct.mean(),
                                     worst_trade_mdd_pct=entered.mdd_pct.min(),
                                     mean_entry_offset=entered.entry_offset.mean()))
    return pd.DataFrame(rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--research-root", type=Path, default=HERE.parent)
    args = parser.parse_args()
    for module in ("blue20_ma_structure_research", "blue20_candle_exhaustion_research",
                   "blue20_profile_cloud_avwap_research"):
        sys.path.insert(0, str(args.research_root / module))
    from analyze_ma import load_events, load_manifest
    from analyze_candle_exhaustion import build_table, candle_features
    from post_entry_flow import simulate_fixed_or_structural
    from study import volume_profile
    from deliver import write_outputs

    manifest = pd.read_csv(args.manifest, dtype=str)
    frames, _ = load_manifest(args.manifest, set(manifest.code))
    frozen = json.loads((HERE / "frozen_reference.json").read_text(encoding="utf-8"))
    prices = {row.code: sha256((args.manifest.parent / row.path).read_bytes()
                             .replace(b"\r\n", b"\n")).hexdigest()
              for row in manifest.itertuples(index=False)}
    if set(prices) != set(frozen["cohort_codes"]):
        raise ValueError("Frozen 500-stock cohort mismatch")
    if any(prices.get(code) != digest for code, digest in frozen["event_stock_prices"].items()):
        raise ValueError("Archived event-stock price mismatch")
    frames = {code: candle_features(d) for code, d in frames.items()}
    base = build_table(load_events(args.events), frames)
    if identity_digest(base) != frozen["identity_sha256"]:
        raise ValueError("Archived 1804 event identity mismatch")
    ledger, daily, chart_data = run(base, frames, volume_profile, simulate_fixed_or_structural)
    ledger = mark_cohorts(ledger)
    args.out.mkdir(parents=True, exist_ok=True)
    write_outputs(args.out, ledger, daily, summary(ledger), frames, chart_data)
    shutil.copyfile(HERE / "PROTOCOL_KO.md", args.out / "PROTOCOL_KO.md")
    audit = dict(research_only=True, events=len(base), stocks=len(frames),
                 baseline_trade_replays_matched=len(base), cross_run_identity_matched=len(base),
                 archived_stock_price_matches=len(frozen["event_stock_prices"]),
                 original_source_runs=[42238850644, 42217814430, 42200217422],
                 identity_sha256=frozen["identity_sha256"])
    (args.out / "AUDIT.json").write_text(json.dumps(audit, indent=2), encoding="utf-8")
    print(json.dumps(audit), flush=True)


if __name__ == "__main__":
    main()
