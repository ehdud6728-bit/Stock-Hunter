"""Independent point-in-time BLUE20 structure research."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
CASES = {
    ("028300", "2023-02-20"): ("HLB", "SL"),
    ("145990", "2023-05-08"): ("삼양사", "SL"),
    ("383220", "2024-07-17"): ("F&F", "SL"),
    ("180640", "2023-09-14"): ("한진칼", "SL"),
    ("006220", "2024-01-24"): ("제주은행", "TP"),
    ("222080", "2024-03-05"): ("씨아이에스", "TP"),
    ("038500", "2024-06-05"): ("삼표시멘트", "TP"),
    ("034120", "2023-02-07"): ("SBS", "TP"),
}
LOOKBACK = 120
BINS = 32
ANCHOR_WINDOW = 60
FEATURES = [
    "vp_breakout", "vp_poc_breakout", "vp_alt_breakout", "vp_resistance_bucket",
    "structure_resistance_bucket",
    "cloud_position", "ma_stack", "ma60_gap_bucket", "ma112_gap_bucket",
    "ma224_gap_bucket", "avwap_position", "body_bucket",
    "turnover_bucket", "d0_candle_shape", "d1_support", "d1_candle",
    "d1_turnover_bucket", "d1_intraday_both_hold", "confluence", "hot_confluence",
]


def volume_profile(window, bins=BINS, method="range"):
    """Approximate per-price volume from historical daily high/low and volume."""
    low, high, volume = (window[c].to_numpy(float) for c in ("low", "high", "volume"))
    if volume.sum() <= 0 or high.max() <= low.min():
        return None
    edges = np.linspace(low.min(), high.max(), bins + 1)
    centers = (edges[:-1] + edges[1:]) / 2
    if method == "typical":
        price = (high + low + window.close.to_numpy(float)) / 3
        idx = np.clip(np.searchsorted(edges, price, side="right") - 1, 0, bins - 1)
        weights = np.bincount(idx, weights=volume, minlength=bins)
    elif method == "range":
        span = high - low
        overlap = np.maximum(0, np.minimum(high[:, None], edges[1:]) -
                             np.maximum(low[:, None], edges[:-1]))
        distributed = overlap / np.where(span > 0, span, 1)[:, None]
        flat = np.flatnonzero(span == 0)
        idx = np.clip(np.searchsorted(edges, low[flat], side="right") - 1, 0, bins - 1)
        distributed[flat, idx] = 1
        weights = volume @ distributed
    else:
        raise ValueError(method)
    peak = left = right = int(weights.argmax())
    mass = weights[peak]
    while mass < 0.70 * weights.sum():
        take_left = right == bins - 1 or (left > 0 and weights[left-1] >= weights[right+1])
        if take_left:
            left -= 1
            mass += weights[left]
        else:
            right += 1
            mass += weights[right]
    hvn = [
        float(centers[i]) for i, w in enumerate(weights)
        if w >= 0.50 * weights[peak]
        and (i == 0 or w > weights[i-1])
        and (i == bins - 1 or w >= weights[i+1])
    ]
    return dict(poc=float(centers[peak]), vah=float(edges[right+1]),
                val=float(edges[left]), hvn=hvn, centers=centers,
                weights=weights, width=float(edges[1] - edges[0]))


def add_cloud(frame):
    d = frame.copy()

    def midpoint(n):
        return (d.high.rolling(n).max() + d.low.rolling(n).min()) / 2

    d["span_a"] = ((midpoint(9) + midpoint(26)) / 2).shift(26)
    d["span_b"] = midpoint(52).shift(26)
    return d


def anchor_avwap(d, j):
    prior = d.iloc[max(0, j-ANCHOR_WINDOW):j]
    # Latest prior low in a tie. Freeze the anchor before examining D0.
    anchor = int(prior.index[prior.low.eq(prior.low.min())][-1])
    typical = (d.high + d.low + d.close) / 3
    vol = d.volume.iloc[anchor:]
    line = (typical.iloc[anchor:] * vol).cumsum() / vol.cumsum().replace(0, np.nan)
    return anchor, line


def price_position(price, lower, upper):
    if not np.isfinite([lower, upper]).all():
        return "MISSING"
    return "ABOVE" if price > upper else "BELOW" if price < lower else "INSIDE"


def distance_bucket(distance, valid):
    if not valid:
        return "MISSING"
    if not np.isfinite(distance):
        return "NONE_IN_LEVEL_SET"
    return "0-3%" if distance < 3 else "3-8%" if distance < 8 else "8%+"


def bucket(value, thresholds, names):
    return names[np.searchsorted(thresholds, value, side="right")] if np.isfinite(value) else "MISSING"


def next_level(levels, close):
    return min((x for x in levels if np.isfinite(x) and x > close), default=np.nan)


def cleared(bar, previous_close, level):
    if not np.isfinite(level):
        return "MISSING"
    if bar.open <= level < bar.close:
        return "BODY_CROSS"
    if previous_close <= level < bar.open and bar.close > level:
        return "GAP_CLEAR"
    return "NO_CLEAR"


def features_for_event(d, j, event):
    b = d.iloc[j]
    close = b.close
    prior = d.iloc[j-LOOKBACK:j] if j >= LOOKBACK else d.iloc[:0]
    profile = volume_profile(prior) if len(prior) else None
    alternative = volume_profile(prior, method="typical") if profile else None
    anchor, avwap = anchor_avwap(d.iloc[:j+1], j)
    av = avwap.iloc[-1]
    spans = (b.span_a, b.span_b)
    bottom, top = sorted(spans) if np.isfinite(spans).all() else (np.nan, np.nan)
    p = profile or dict(poc=np.nan, vah=np.nan, val=np.nan, hvn=[])
    vp = [p["poc"], p["vah"], p["val"], *p["hvn"]]
    vp_next = next_level(vp, close)
    all_next = next_level([*vp, b.ma60, b.ma112, b.ma224, bottom, top, av], close)
    gap = lambda level: 100 * (level / close - 1)
    prior_close = d.close.iloc[j-1]
    vp_break = cleared(b, prior_close, p["vah"])
    cloud = price_position(close, bottom, top)
    ma_valid = np.isfinite([b.ma60, b.ma112, b.ma224]).all()
    stack = ("BULL" if b.ma60 > b.ma112 > b.ma224 else
             "BEAR" if b.ma60 < b.ma112 < b.ma224 else "MIXED") if ma_valid else "MISSING"
    aligned = (vp_break in ("BODY_CROSS", "GAP_CLEAR") and cloud == "ABOVE"
               and close > av and close > b.ma60 and close > b.ma112 and close > b.ma224)
    hot = event.d0_body_abs_prior_atr20 >= 3 and event.d0_amount_ratio20 >= 10
    # D1 is a separate EOD observation, based on D0-frozen support.
    d1 = d.iloc[j+1]
    midpoint = (b.open+b.close)/2
    if not profile:
        support = "MISSING"
    elif d1.close >= max(midpoint, p["vah"]):
        support = "BOTH"
    elif d1.close >= midpoint:
        support = "MID_ONLY"
    elif d1.close >= p["vah"]:
        support = "VAH_ONLY"
    else:
        support = "NEITHER"
    facts = {
        "vp_poc": p["poc"], "vp_vah": p["vah"], "vp_val": p["val"],
        "vp_hvn": ";".join(f"{x:.6f}" for x in p["hvn"]),
        "vp_breakout": vp_break, "vp_poc_breakout": cleared(b, prior_close, p["poc"]),
        "vp_alt_breakout": cleared(b, prior_close, alternative["vah"] if alternative else np.nan),
        "vp_alt_vah": alternative["vah"] if alternative else np.nan,
        "vp_next_resistance": vp_next, "vp_resistance_pct": gap(vp_next),
        "vp_resistance_bucket": distance_bucket(gap(vp_next), bool(profile)),
        "structure_next_resistance": all_next, "structure_resistance_pct": gap(all_next),
        "structure_resistance_bucket": distance_bucket(
            gap(all_next), bool(profile) and ma_valid and np.isfinite([av, top]).all()),
        "cloud_bottom": bottom, "cloud_top": top, "cloud_position": cloud,
        "avwap_anchor_date": d.date.iloc[anchor], "avwap_d0": av,
        "avwap_position": price_position(close, av, av),
        "ma60": b.ma60, "ma112": b.ma112, "ma224": b.ma224,
        "ma60_gap_pct": -100 * (b.ma60-close) / b.ma60,
        "ma112_gap_pct": -100 * (b.ma112-close) / b.ma112,
        "ma224_gap_pct": -100 * (b.ma224-close) / b.ma224, "ma_stack": stack,
        "body_bucket": bucket(event.d0_body_abs_prior_atr20, [1, 2, 3],
                              ["<1x", "1-2x", "2-3x", "3x+"]),
        "turnover_bucket": bucket(event.d0_amount_ratio20, [2, 5, 10, 20],
                                  ["<2x", "2-5x", "5-10x", "10-20x", "20x+"]),
        "d1_date_recomputed": d1.date, "d1_close_recomputed": d1.close,
        "d1_low_recomputed": d1.low, "d1_support": support,
        "d1_intraday_both_hold": bool(profile and d1.low >= max(midpoint, p["vah"])),
        "d1_candle": d1.candle_shape,
        "d1_turnover_ratio20": d1.amount_ratio20,
        "d1_turnover_bucket": bucket(d1.amount_ratio20, [2, 5, 10, 20],
                                     ["<2x", "2-5x", "5-10x", "10-20x", "20x+"]),
        "confluence": ("ALIGNED" if aligned else "OTHER") if
                      profile and ma_valid and np.isfinite([av, top]).all() else "MISSING",
        "hot_confluence": "HOT_ALIGNED" if hot and aligned else "HOT_OTHER" if hot else "NOT_HOT",
    }
    for days in (60, 112, 224):
        facts[f"ma{days}_gap_bucket"] = bucket(
            facts[f"ma{days}_gap_pct"], [0, 5, 10, 20],
            ["BELOW", "0-5%", "5-10%", "10-20%", "20%+"])
    return facts, profile


def make_ledger(base, frames, simulate):
    groups = base.groupby("code", sort=False)
    records, chart_data = [], {}
    for code, group in groups:
        d = add_cloud(frames[code])
        index = {date: i for i, date in enumerate(d.date)}
        for event in group.itertuples(index=False):
            j, entry = index[event.date], index[event.entry_date]
            if not j or not j < entry < j+20 < len(d):
                raise ValueError(f"Invalid source timing {code} {event.date}")
            replay = simulate(d, j, entry, float(d.open.iloc[entry]), False)
            if (not np.isclose(replay["net_pct"], event.baseline_net_pct, atol=1e-6, rtol=0)
                    or replay["exit_reason"] != event.baseline_exit_reason
                    or replay["exit_date"] != event.baseline_exit_date
                    or not np.isclose(d.close.iloc[j], event.d0_close)
                    or not np.isclose(d.open.iloc[entry], event.baseline_entry_price)
                    or not np.isclose(d.amount_ratio20.iloc[j], event.d0_amount_ratio20,
                                      atol=1e-5, rtol=0)):
                raise ValueError(f"Baseline replay disagreement {code} {event.date}")
            facts, profile = features_for_event(d, j, event)
            peak = float(d.high.iloc[j+1:j+21].max())
            item = {**event._asdict(), **facts,
                    "d20_date": d.date.iloc[j+20], "d1_to_d20_high": peak,
                    "d1_to_d20_high_vs_d0_pct": 100*(peak/event.d0_close-1),
                    "d20_high": float(d.high.iloc[j+20]),
                    "d20_high_vs_d0_pct": 100*(d.high.iloc[j+20]/event.d0_close-1)}
            if event.gc_offset == 0:
                if entry != j+1:
                    raise ValueError("D0 GC baseline must enter D1 open")
                delay = simulate(d, j, j+2, float(d.open.iloc[j+2]), False)
                item.update(d2_open=float(d.open.iloc[j+2]), d2_date=d.date.iloc[j+2],
                            d2_net=delay["net_pct"], d2_exit_reason=delay["exit_reason"],
                            d2_exit_date=delay["exit_date"])
            records.append(item)
            key = (code, event.date)
            if key in CASES and event.band == "UPPER":
                if not event.baseline_exit_reason.startswith(CASES[key][1]) or not profile:
                    raise ValueError(f"Requested case mismatch {key}")
                chart_data[key] = (d, j, profile, item)
    if set(chart_data) != set(CASES):
        raise ValueError("Requested chart case absent")
    return pd.DataFrame(records), chart_data


def nonoverlap(ledger):
    """Greedy first event per stock across both bands, full D0 through D+20."""
    kept, last = [], {}
    for row in ledger.sort_values(["date", "code", "band"]).itertuples():
        if row.date > last.get(row.code, ""):
            kept.append(row.Index)
            last[row.code] = row.d20_date
    return ledger.loc[kept]


def summarize(ledger):
    rows = []
    for sample, data in [("ALL", ledger), ("NONOVERLAP", nonoverlap(ledger))]:
        for year, annual in [("ALL", data), *data.groupby("year")]:
            for band, b in annual.groupby("band"):
                for feature in ["baseline", *FEATURES]:
                    groups = [("ALL", b)] if feature == "baseline" else b.groupby(feature)
                    for value, g in groups:
                        n = len(g)
                        rows.append(dict(sample=sample, year=year, band=band, feature=feature,
                                         value=value, events=n, stocks=g.code.nunique(),
                                         signal_days=g.date.nunique(),
                                         n_lt30=n < 30, sl_pct=100*g.baseline_stop.mean(),
                                         tp_pct=100*g.baseline_tp.mean(),
                                         net_pct=g.baseline_net_pct.mean(),
                                         mean_d1_to_d20_peak_pct=g.d1_to_d20_high_vs_d0_pct.mean(),
                                         mean_d20_day_high_pct=g.d20_high_vs_d0_pct.mean()))
    return pd.DataFrame(rows)


def compare_prior(candle, ma_path):
    ma = pd.read_csv(ma_path, dtype={"code": str})
    key = ["code", "date", "band"]
    common = candle.merge(ma, on=key, validate="one_to_one", suffixes=("", "_ma"))
    if len(common) != len(ma) or len(ma) != len(candle):
        raise ValueError("Previous runs differ in event identity")
    for left, right in [("entry_date", "entry_date_ma"), ("gc_date", "gc_date_ma"),
                        ("baseline_exit_date", "exit_date"), ("baseline_exit_reason", "exit_reason")]:
        if not common[left].eq(common[right]).all():
            raise ValueError(f"Previous runs disagree on {left}")
    for left, right in [("baseline_net_pct", "net_pct"), ("baseline_entry_price", "entry_price"),
                        ("d0_amount_ratio20", "d0_amount_ratio")]:
        if not np.allclose(common[left], common[right], rtol=0, atol=1e-6):
            raise ValueError(f"Previous runs disagree on {left}")
    return len(common)


def verify_archive_hashes(manifest, audit, ma_audit=None):
    from hashlib import sha256
    expected = json.loads(audit.read_text(encoding="utf-8"))["unique_price_sha256"]
    if ma_audit:
        other = json.loads(ma_audit.read_text(encoding="utf-8"))["price_sha256_by_code"]
        if expected != other:
            raise ValueError("Previous runs do not share the same price hashes")
    m = pd.read_csv(manifest, dtype=str)
    for row in m.itertuples(index=False):
        if row.code not in expected:
            continue
        data = (manifest.parent / row.path).read_bytes()
        if sha256(data.replace(b"\r\n", b"\n")).hexdigest() != expected[row.code]:
            raise ValueError(f"Archive hash differs after LF normalization: {row.code}")
    if not set(expected).issubset(m.code):
        raise ValueError("Archive audit missing code in rebuilt manifest")
    return len(expected)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    source = p.add_mutually_exclusive_group(required=True)
    source.add_argument("--events", type=Path, help="Replayed original multifactor events")
    source.add_argument("--ledger", type=Path, help="Candle artifact ledger for local verification")
    p.add_argument("--compare-ma-ledger", type=Path)
    p.add_argument("--candle-audit", type=Path)
    p.add_argument("--ma-audit", type=Path)
    p.add_argument("--manifest", type=Path, required=True)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--research-root", type=Path, default=HERE.parent)
    args = p.parse_args()
    for module in ("blue20_ma_structure_research", "blue20_candle_exhaustion_research"):
        sys.path.insert(0, str(args.research_root / module))
    from analyze_ma import load_events, load_manifest, sha256
    from analyze_candle_exhaustion import build_table, candle_features, screen_baseline
    from post_entry_flow import simulate_fixed_or_structural
    from gallery import write_gallery

    args.out.mkdir(parents=True, exist_ok=True)
    manifest = pd.read_csv(args.manifest, dtype=str)
    # Check all 500 files, even for names that contribute no observed GC event.
    frames, hashes = load_manifest(args.manifest, set(manifest.code))
    frames = {code: candle_features(d) for code, d in frames.items()}
    if args.ledger:
        base = pd.read_csv(args.ledger, dtype={"code": str}, low_memory=False)
    else:
        base = build_table(load_events(args.events), frames)
    screen_baseline(base)
    prior_matches = compare_prior(base, args.compare_ma_ledger) if args.compare_ma_ledger else None
    archive_matches = verify_archive_hashes(args.manifest, args.candle_audit, args.ma_audit) if args.candle_audit else None
    print(f"Baseline ready: {len(base)} events, {len(frames)} input stocks", flush=True)
    ledger, charts = make_ledger(base, frames, simulate_fixed_or_structural)
    ledger.to_csv(args.out / "EVENTS.csv.gz", index=False, compression="gzip")
    summarize(ledger).to_csv(args.out / "GROUPS.csv", index=False)
    policies(ledger).to_csv(args.out / "D1_D2_POLICIES.csv", index=False)
    write_gallery(args.out, charts, CASES, anchor_avwap)
    dedup = nonoverlap(ledger)
    audit = {
        "research_only": True, "production_ready": False, "out_of_sample": False,
        "prior_runs": [42217814430, 42200217422],
        "cohort_stocks": len(frames), "events": len(ledger),
        "event_stocks": int(ledger.code.nunique()),
        "by_band": ledger.band.value_counts().to_dict(),
        "by_year_band": ledger.groupby(["year", "band"]).size().unstack().to_dict(),
        "baseline_trade_replays_matched": len(ledger),
        "prior_cross_run_ledger_rows_matched": prior_matches,
        "prior_cross_run_event_stock_price_hashes_matched": archive_matches,
        "baseline_source_sha256": sha256(args.events or args.ledger),
        "input_manifest_sha256": sha256(args.manifest),
        "verified_price_sha256": hashes,
        "same_stock_day_duplicates_beyond_first": int(ledger.duplicated(["code", "date"]).sum()),
        "repeated_stock_events_beyond_first": int(ledger.duplicated("code").sum()),
        "repeated_day_events_beyond_first": int(ledger.duplicated("date").sum()),
        "max_events_one_day": int(ledger.date.value_counts().max()),
        "nonoverlap_events": len(dedup),
        "nonoverlap_by_band": dedup.band.value_counts().to_dict(),
        "d0_vp_coverage": int(ledger.vp_poc.notna().sum()),
        "d0_cloud_coverage": int(ledger.cloud_top.notna().sum()),
        "d0_avwap_coverage": int(ledger.avwap_d0.notna().sum()),
        "d0_ma224_coverage": int(ledger.ma224.notna().sum()),
        "alt_profile_class_changed": int(ledger.vp_breakout.ne(ledger.vp_alt_breakout).sum()),
        "chart_cases": len(charts),
        "d1_descriptive_or_d2_only": True,
        "profit_metrics_use_stop_first_and_cost_pct": 0.25,
    }
    (args.out / "AUDIT.json").write_text(json.dumps(audit, indent=2), encoding="utf-8")
    from report import write_report
    write_report(args.out, ledger, audit)
    print(json.dumps({k: audit[k] for k in ("events", "by_band", "chart_cases", "nonoverlap_events")}))


def policies(ledger):
    rows = []
    gc = ledger[ledger.gc_offset.eq(0)]
    for year, annual in [("ALL", gc), *gc.groupby("year")]:
        for band, b in annual.groupby("band"):
            for name, trade in [
                ("D1_IMMEDIATE", pd.Series(True, index=b.index)),
                ("D2_UNCONDITIONAL", pd.Series(True, index=b.index)),
                ("D2_MID_AND_PRIOR_VAH", b.d1_support.eq("BOTH")),
            ]:
                immediate = name == "D1_IMMEDIATE"
                net = b.baseline_net_pct if immediate else b.d2_net
                reason = b.baseline_exit_reason if immediate else b.d2_exit_reason
                executed = int(trade.sum())
                sl = int(reason[trade].str.startswith("SL").sum())
                tp = int(reason[trade].str.startswith("TP").sum())
                total = net.where(trade, 0)
                rows.append(dict(year=year, band=band, policy=name, opportunities=len(b),
                                 trades=executed, skips=len(b)-executed, sl=sl, tp=tp,
                                 sl_pct=100*sl/executed if executed else np.nan,
                                 tp_pct=100*tp/executed if executed else np.nan,
                                 missed_baseline_tp=int(b.loc[~trade, "baseline_tp"].sum()),
                                 net_trade_pct=net[trade].mean(), net_opportunity_pct=total.mean(),
                                 paired_delta_pp=(total-b.baseline_net_pct).mean()))
    return pd.DataFrame(rows)


if __name__ == "__main__":
    main()

