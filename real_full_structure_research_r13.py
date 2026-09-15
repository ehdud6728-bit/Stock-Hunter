from __future__ import annotations

import argparse
import gzip
import json
import math
import pickle
import re
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

REVISION = "R1_3_EXACT_CAUSAL_ANCHOR_DUAL_AXIS_SHADOW_20260915"
ANCHOR_METHOD = "CAUSAL_LOW_HIGH_PULLBACK_V1"
ANCHOR_PROVENANCE = "EXACT_ASOF_HISTORY_SHADOW_SERIALIZER"
SHADOW_ID = "REAL_FULL_R1C1"
SHADOW_REVISION = "R1C1_PB_VOLUME_CONTRACTION_V1"


def norm_code(v: Any) -> str:
    s = str(v or "").strip().upper()
    if s.endswith(".0") and s[:-2].isdigit():
        s = s[:-2]
    for suffix in (".KS", ".KQ", ".KRX"):
        if s.endswith(suffix):
            s = s[:-len(suffix)]
            break
    s = "".join(ch for ch in s if ch.isalnum())
    if len(s) == 7 and s.startswith("A"):
        s = s[1:]
    if s.isdigit() and len(s) <= 6:
        return s.zfill(6)
    return s[-6:] if len(s) >= 6 else s


def num(v: Any) -> float:
    try:
        x = float(v)
        return x if math.isfinite(x) else float("nan")
    except Exception:
        return float("nan")


def ratio(a: Any, b: Any) -> float:
    a = num(a)
    b = num(b)
    return a / b if math.isfinite(a) and math.isfinite(b) and b != 0 else float("nan")


def med(s: pd.Series) -> float:
    x = pd.to_numeric(s, errors="coerce").dropna()
    return float(x.median()) if len(x) else float("nan")


def qtile(s: pd.Series, q: float) -> float:
    x = pd.to_numeric(s, errors="coerce").dropna()
    return float(x.quantile(q)) if len(x) else float("nan")


def cliffs_delta(a: pd.Series, b: pd.Series) -> float:
    x = pd.to_numeric(a, errors="coerce").dropna().to_numpy(dtype=float)
    y = pd.to_numeric(b, errors="coerce").dropna().to_numpy(dtype=float)
    if not len(x) or not len(y):
        return float("nan")
    return float(np.sign(x[:, None] - y[None, :]).mean())


def boolish(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    return s in {"1", "true", "yes", "y", "on"}


def read_identity(path: Path, expected: tuple[int, int, int, int]) -> dict[str, int]:
    df = pd.read_csv(path)
    if len(df) != 1:
        raise SystemExit(f"R13_IDENTITY_ROWS_INVALID:{len(df)}")
    r = df.iloc[0]
    got = (
        int(num(r.get("snapshot_dates"))),
        int(num(r.get("top15_events"))),
        int(num(r.get("d5_mature_events"))),
        int(num(r.get("context_pattern_matrices"))),
    )
    if got != expected or int(num(r.get("identity_pass"))) != 1:
        raise SystemExit(f"R13_IDENTITY_FAIL got={got} expected={expected}")
    return {
        "snapshot_dates": got[0],
        "top15_events": got[1],
        "d5_mature_events": got[2],
        "context_pattern_matrices": got[3],
    }


def load_price_frame(cache_root: Path, code: str, cache: dict[str, pd.DataFrame]) -> pd.DataFrame:
    if code in cache:
        return cache[code]
    files = []
    for p in cache_root.glob(f"{code}_*.pkl.gz"):
        m = re.search(r"_(\d+)\.pkl\.gz$", p.name)
        days = int(m.group(1)) if m else 0
        files.append((days, p))
    if not files:
        cache[code] = pd.DataFrame()
        return cache[code]
    p = sorted(files, key=lambda t: t[0], reverse=True)[0][1]
    try:
        with gzip.open(p, "rb") as fh:
            obj = pickle.load(fh)
        fr = obj.get("frame") if isinstance(obj, dict) else obj
        if not isinstance(fr, pd.DataFrame):
            raise TypeError(type(fr).__name__)
        fr = fr.copy()
        fr.index = pd.to_datetime(fr.index, errors="coerce").normalize()
        fr = fr[fr.index.notna()].sort_index()
        for c in ["Open", "High", "Low", "Close", "Volume"]:
            if c in fr.columns:
                fr[c] = pd.to_numeric(fr[c], errors="coerce")
        cache[code] = fr
    except Exception:
        cache[code] = pd.DataFrame()
    return cache[code]


def anchor_join(ledger: pd.DataFrame, anchors: pd.DataFrame) -> pd.DataFrame:
    x = ledger.copy()
    a = anchors.copy()
    x["_code_key"] = x["code"].map(norm_code)
    x["_date_key"] = pd.to_datetime(x["origin_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    a["_code_key"] = a["code"].map(norm_code)
    a["_date_key"] = pd.to_datetime(a["signal_date"], errors="coerce").dt.strftime("%Y-%m-%d")

    # Fail on conflicting duplicate serialized anchors; exact duplicates are safe to collapse.
    compare_cols = [c for c in [
        "anchor_status", "anchor_method", "wave1_low_date", "wave1_low_price",
        "wave1_high_date", "wave1_high_price", "pullback_low_date", "pullback_low_price",
        "wave1_rise_pct", "pullback_retrace_pct", "temporal_invariant",
        "anchor_provenance", "selector_logic_changed"
    ] if c in a.columns]
    conflicts = []
    for key, g in a.groupby(["_date_key", "_code_key"], dropna=False):
        if len(g) > 1 and len(g[compare_cols].astype(str).drop_duplicates()) > 1:
            conflicts.append(key)
    if conflicts:
        raise SystemExit(f"R13_ANCHOR_CONFLICTING_DUPLICATES:{conflicts[:5]}")
    a = a.sort_values(["_date_key", "_code_key"]).drop_duplicates(["_date_key", "_code_key"], keep="last")

    drop = [c for c in ["code", "name", "signal_date"] if c in a.columns]
    z = x.merge(a.drop(columns=drop), on=["_date_key", "_code_key"], how="left", suffixes=("", "_anchor"), indicator="_anchor_merge")
    if len(z) != len(x):
        raise SystemExit(f"R13_ANCHOR_JOIN_ROW_EXPANSION ledger={len(x)} joined={len(z)}")
    if not z["_anchor_merge"].eq("both").all():
        miss = z.loc[~z["_anchor_merge"].eq("both"), ["origin_date", "code", "name"]].head(10).to_dict("records")
        raise SystemExit(f"R13_ANCHOR_JOIN_MISSING:{miss}")
    return z


def stage_features(row: pd.Series, fr: pd.DataFrame) -> dict[str, Any]:
    out: dict[str, Any] = {}
    if fr.empty or not {"Open", "High", "Low", "Close", "Volume"}.issubset(fr.columns):
        return {"stage_status": "PRICE_HISTORY_UNAVAILABLE"}
    try:
        od = pd.Timestamp(row["origin_date"]).normalize()
        ld = pd.Timestamp(row["wave1_low_date"]).normalize()
        hd = pd.Timestamp(row["wave1_high_date"]).normalize()
        pdte = pd.Timestamp(row["pullback_low_date"]).normalize()
    except Exception:
        return {"stage_status": "ANCHOR_DATE_PARSE_FAIL"}

    h = fr.loc[:od].copy()
    if h.empty:
        return {"stage_status": "NO_CAUSAL_HISTORY"}
    h["AmountProxy"] = pd.to_numeric(h["Close"], errors="coerce") * pd.to_numeric(h["Volume"], errors="coerce")
    idx = list(h.index)
    pos = {d: i for i, d in enumerate(idx)}
    if not all(d in pos for d in [ld, hd, pdte, od]):
        return {"stage_status": "ANCHOR_DATE_NOT_IN_CACHE"}
    il, ih, ip, io = pos[ld], pos[hd], pos[pdte], pos[od]
    if not (il < ih < ip <= io):
        return {"stage_status": "ANCHOR_ORDER_INVALID"}

    pre = h.iloc[max(0, il - 20):il]
    wave = h.iloc[il:ih + 1]
    # Pullback segment is high+1 through pullback-low. If PB low is signal day,
    # the event is ineligible for R1C1 because signal-day final daily bar is not
    # allowed to masquerade as a 15:03-causal observation.
    pb = h.iloc[ih + 1:ip + 1]
    recovery = h.iloc[ip:io]  # excludes signal day
    signal = h.iloc[io]

    out.update({
        "stage_status": "PASS",
        "wave1_bars": int(ih - il + 1),
        "pb_to_low_bars": int(ip - ih),
        "recovery_bars": int(io - ip),
        "total_pattern_bars": int(io - il + 1),
        "pb_anchor_pre_origin": int(pdte < od),
    })

    for metric, col in [("volume", "Volume"), ("amount_proxy", "AmountProxy")]:
        premed, wavemed, pbmed, recmed = med(pre[col]), med(wave[col]), med(pb[col]), med(recovery[col])
        out[f"pre20_{metric}_median"] = premed
        out[f"wave1_{metric}_median"] = wavemed
        out[f"pb_{metric}_median"] = pbmed
        out[f"recovery_{metric}_median"] = recmed
        out[f"wave1_{metric}_vs_pre20"] = ratio(wavemed, premed)
        out[f"pb_{metric}_vs_wave1"] = ratio(pbmed, wavemed)
        out[f"recovery_{metric}_vs_pb"] = ratio(recmed, pbmed)
        out[f"signal_{metric}_vs_pb"] = ratio(signal[col], pbmed)
        out[f"signal_{metric}_vs_pre20"] = ratio(signal[col], premed)
        out[f"signal_{metric}_vs_wave1"] = ratio(signal[col], wavemed)

    high = num(row.get("wave1_high_price"))
    low = num(row.get("wave1_low_price"))
    pbl = num(row.get("pullback_low_price"))
    close = num(signal.get("Close"))
    op = num(signal.get("Open"))
    shi = num(signal.get("High"))
    slo = num(signal.get("Low"))
    rise = num(row.get("wave1_rise_pct"))
    out["pb_drawdown_from_high_pct"] = (pbl / high - 1.0) * 100.0 if high > 0 and pbl > 0 else np.nan
    out["signal_close_vs_high_pct"] = (close / high - 1.0) * 100.0 if high > 0 and close > 0 else np.nan
    out["signal_recovery_from_pb_low_pct"] = (close / pbl - 1.0) * 100.0 if pbl > 0 and close > 0 else np.nan
    out["signal_candle_ret_pct"] = (close / op - 1.0) * 100.0 if op > 0 and close > 0 else np.nan
    out["signal_close_location_pct"] = (close - slo) / (shi - slo) * 100.0 if shi > slo else np.nan
    out["wave1_rise_per_bar_pct"] = rise / out["wave1_bars"] if math.isfinite(rise) and out["wave1_bars"] else np.nan
    out["retrace_over_100"] = int(num(row.get("pullback_retrace_pct")) > 100) if math.isfinite(num(row.get("pullback_retrace_pct"))) else np.nan

    ma224 = pd.to_numeric(h["Close"], errors="coerce").rolling(224, min_periods=224).mean()
    for nm, d in [("wave1_low", ld), ("wave1_high", hd), ("pb_low", pdte), ("signal", od)]:
        mv = num(ma224.loc[d]) if d in ma224.index else np.nan
        cv = num(h.loc[d, "Close"]) if d in h.index else np.nan
        out[f"{nm}_ma224_distance_pct"] = (cv / mv - 1.0) * 100.0 if mv > 0 and cv > 0 else np.nan
    return out


def compare_feature(df: pd.DataFrame, feature: str, group_col: str, a_name: str, b_name: str, axis: str, cohort: str) -> dict[str, Any]:
    a = df.loc[df[group_col].eq(a_name), feature]
    b = df.loc[df[group_col].eq(b_name), feature]
    return {
        "axis": axis,
        "cohort": cohort,
        "feature": feature,
        "group_a": a_name,
        "group_b": b_name,
        "n_a": int(pd.to_numeric(a, errors="coerce").notna().sum()),
        "n_b": int(pd.to_numeric(b, errors="coerce").notna().sum()),
        "a_median": med(a),
        "a_q25": qtile(a, 0.25),
        "a_q75": qtile(a, 0.75),
        "b_median": med(b),
        "b_q25": qtile(b, 0.25),
        "b_q75": qtile(b, 0.75),
        "median_diff_a_minus_b": med(a) - med(b) if math.isfinite(med(a)) and math.isfinite(med(b)) else np.nan,
        "cliffs_delta_a_minus_b": cliffs_delta(a, b),
    }


def build_comparisons(df: pd.DataFrame) -> pd.DataFrame:
    features = [
        "wave1_rise_pct", "wave1_rise_per_bar_pct", "pullback_retrace_pct", "pb_drawdown_from_high_pct",
        "wave1_bars", "pb_to_low_bars", "recovery_bars", "total_pattern_bars",
        "signal_close_vs_high_pct", "signal_recovery_from_pb_low_pct", "signal_candle_ret_pct", "signal_close_location_pct",
        "upper60_space_pct", "low60_distance_pct", "ma224_distance_pct",
        "wave1_volume_vs_pre20", "pb_volume_vs_wave1", "recovery_volume_vs_pb", "signal_volume_vs_pb", "signal_volume_vs_pre20", "signal_volume_vs_wave1",
        "wave1_amount_proxy_vs_pre20", "pb_amount_proxy_vs_wave1", "recovery_amount_proxy_vs_pb", "signal_amount_proxy_vs_pb", "signal_amount_proxy_vs_pre20", "signal_amount_proxy_vs_wave1",
        "universe_amount_ratio_prev_vs20", "universe_volume_ratio_prev_vs20",
    ]
    rows = []
    for cohort_name, cohort in [("ALL", df), ("V72_SCORE100", df[df["v72_score100"].eq(1)])]:
        for feature in features:
            if feature not in cohort.columns:
                continue
            rows.append(compare_feature(cohort, feature, "ignition_group", "HIT", "NO_HIT", "IGNITION", cohort_name))
        hits = cohort[cohort["ignition_group"].eq("HIT")].copy()
        for feature in features:
            if feature not in hits.columns:
                continue
            rows.append(compare_feature(hits, feature, "path_group", "CLEAN", "DAMAGE", "PATH_INTEGRITY", cohort_name))
    return pd.DataFrame(rows)


def direction_stability(comp: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (axis, feature), g in comp.groupby(["axis", "feature"], sort=False):
        q = g.set_index("cohort")
        if not {"ALL", "V72_SCORE100"}.issubset(q.index):
            continue
        da = num(q.loc["ALL", "cliffs_delta_a_minus_b"])
        ds = num(q.loc["V72_SCORE100", "cliffs_delta_a_minus_b"])
        same = int(math.isfinite(da) and math.isfinite(ds) and ((da > 0 and ds > 0) or (da < 0 and ds < 0) or (da == 0 and ds == 0)))
        rows.append({
            "axis": axis,
            "feature": feature,
            "delta_all": da,
            "delta_score100": ds,
            "same_direction": same,
            "min_abs_delta": min(abs(da), abs(ds)) if math.isfinite(da) and math.isfinite(ds) else np.nan,
            "note": "DESCRIPTIVE_SAME_SAMPLE_NO_THRESHOLD_SELECTION",
        })
    return pd.DataFrame(rows).sort_values(["axis", "same_direction", "min_abs_delta"], ascending=[True, False, False], kind="stable")


def shadow_matrix(df: pd.DataFrame) -> pd.DataFrame:
    x = df[df["v72_score100"].eq(1)].copy()
    x["shadow_eligible"] = x["stage_status"].eq("PASS") & x["pb_anchor_pre_origin"].eq(1) & pd.to_numeric(x["pb_volume_vs_wave1"], errors="coerce").notna()
    x["r1c1_shadow_positive"] = x["shadow_eligible"] & pd.to_numeric(x["pb_volume_vs_wave1"], errors="coerce").lt(1.0)
    x["r1c1_shadow_state"] = np.where(~x["shadow_eligible"], "INELIGIBLE", np.where(x["r1c1_shadow_positive"], "PB_CONTRACTION", "NO_PB_CONTRACTION"))
    x["ma224_context_r13"] = np.where(pd.to_numeric(x.get("below_ma224"), errors="coerce").eq(1), "BELOW224", "AT_OR_ABOVE224")
    x["hit_plus5_d5"] = x["ignition_group"].eq("HIT").astype(int)
    x["clean_path"] = x["outcome4"].eq("CLEAN_WIN").astype(int)
    x["path_damage"] = x["outcome4"].isin(["GIVEBACK", "DEEP_MAE_WIN"]).astype(int)
    rows = []
    for state, g in x.groupby("r1c1_shadow_state", dropna=False):
        hits = int(g["hit_plus5_d5"].sum())
        hitg = g[g["hit_plus5_d5"].eq(1)]
        rows.append({
            "shadow_state": state,
            "n": int(len(g)),
            "hit_plus5_d5_n": hits,
            "hit_plus5_d5_rate_pct": hits / len(g) * 100.0 if len(g) else np.nan,
            "clean_n": int(g["clean_path"].sum()),
            "path_damage_n": int(g["path_damage"].sum()),
            "clean_among_hits_pct": float(hitg["clean_path"].mean() * 100.0) if len(hitg) else np.nan,
            "historical_only": 1,
        })
    return x, pd.DataFrame(rows)


def self_test() -> None:
    assert norm_code("660.0") == "000660"
    assert norm_code("A005930") == "005930"
    assert abs(ratio(2, 4) - 0.5) < 1e-12
    a = pd.Series([1, 2, 3])
    b = pd.Series([0, 1, 2])
    assert cliffs_delta(a, b) > 0
    print("REAL_FULL_STRUCTURE_RESEARCH_R13_SELF_TEST_PASS")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--r12-ledger", default="reports/real_full_structure_research_r12/r12_event_ledger.csv")
    ap.add_argument("--r1-identity", default="reports/real_full_structure_research_r1/identity_audit.csv")
    ap.add_argument("--anchor-csv", default="source/reports/v72_formula_selector_anchor_serialization.csv")
    ap.add_argument("--price-cache-dir", default="source/reports/.cache/v20_price_history")
    ap.add_argument("--output-dir", default="reports/real_full_structure_research_r13")
    ap.add_argument("--source-run-id", default="34799606739")
    ap.add_argument("--expected-snapshots", type=int, default=24)
    ap.add_argument("--expected-events", type=int, default=115)
    ap.add_argument("--expected-d5", type=int, default=115)
    ap.add_argument("--expected-matrices", type=int, default=13)
    ap.add_argument("--self-test", action="store_true")
    args = ap.parse_args()
    if args.self_test:
        self_test()
        return 0

    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    expected = (args.expected_snapshots, args.expected_events, args.expected_d5, args.expected_matrices)
    ident = read_identity(Path(args.r1_identity), expected)

    ledger = pd.read_csv(args.r12_ledger, dtype={"code": str})
    if len(ledger) != args.expected_events:
        raise SystemExit(f"R13_LEDGER_COUNT_FAIL:{len(ledger)}")
    anchors = pd.read_csv(args.anchor_csv, dtype={"code": str})
    x = anchor_join(ledger, anchors)

    # Anchor authority audit: research-exact as-of serializer, NOT native V72 anchor claim.
    anchor_ok = (
        x["anchor_status"].astype(str).eq("AVAILABLE")
        & x["anchor_method"].astype(str).eq(ANCHOR_METHOD)
        & x["temporal_invariant"].astype(str).eq("PASS")
    )
    if "selector_logic_changed" in x.columns:
        anchor_ok &= ~x["selector_logic_changed"].map(boolish)
    if "anchor_provenance" in x.columns:
        anchor_ok &= x["anchor_provenance"].astype(str).eq(ANCHOR_PROVENANCE)
    if int(anchor_ok.sum()) != len(x):
        bad = x.loc[~anchor_ok, [c for c in ["origin_date", "code", "name", "anchor_status", "anchor_method", "temporal_invariant", "anchor_provenance", "selector_logic_changed"] if c in x.columns]].head(10)
        raise SystemExit(f"R13_ANCHOR_AUTHORITY_FAIL count={int((~anchor_ok).sum())} sample={bad.to_dict('records')}")

    cache: dict[str, pd.DataFrame] = {}
    feature_rows = []
    for i, r in x.iterrows():
        code = norm_code(r.get("code"))
        sf = stage_features(r, load_price_frame(Path(args.price_cache_dir), code, cache))
        z = r.to_dict()
        z.update(sf)
        feature_rows.append(z)
    df = pd.DataFrame(feature_rows)
    if not df["stage_status"].eq("PASS").all():
        raise SystemExit(f"R13_STAGE_FEATURE_FAIL:{df['stage_status'].value_counts(dropna=False).to_dict()}")

    df["v72_score100"] = pd.to_numeric(df.get("v72_pullback_restart_score"), errors="coerce").eq(100).astype(int)
    df["ignition_group"] = np.where(df["outcome4"].astype(str).eq("NO_HIT"), "NO_HIT", "HIT")
    df["path_group"] = np.where(df["outcome4"].astype(str).eq("CLEAN_WIN"), "CLEAN", np.where(df["outcome4"].astype(str).isin(["GIVEBACK", "DEEP_MAE_WIN"]), "DAMAGE", "NOT_HIT"))
    df.to_csv(out / "r13_event_ledger.csv", index=False, encoding="utf-8-sig")

    anchor_audit = pd.DataFrame([{
        **ident,
        "event_rows": len(df),
        "unique_event_ids": int(df["event_id"].nunique()) if "event_id" in df.columns else np.nan,
        "anchor_available": int(df["anchor_status"].eq("AVAILABLE").sum()),
        "anchor_temporal_pass": int(df["temporal_invariant"].eq("PASS").sum()),
        "anchor_method_match": int(df["anchor_method"].eq(ANCHOR_METHOD).sum()),
        "anchor_provenance_match": int(df["anchor_provenance"].eq(ANCHOR_PROVENANCE).sum()) if "anchor_provenance" in df.columns else 0,
        "selector_logic_changed_true": int(df["selector_logic_changed"].map(boolish).sum()) if "selector_logic_changed" in df.columns else 0,
        "stage_feature_pass": int(df["stage_status"].eq("PASS").sum()),
        "pb_anchor_pre_origin": int(df["pb_anchor_pre_origin"].eq(1).sum()),
        "pb_anchor_on_origin": int(df["pb_anchor_pre_origin"].eq(0).sum()),
        "authority_note": "RESEARCH_EXACT_ASOF_SERIALIZER_NOT_NATIVE_V72_ANCHOR",
    }])
    anchor_audit.to_csv(out / "anchor_authority_audit.csv", index=False, encoding="utf-8-sig")

    comp = build_comparisons(df)
    comp.to_csv(out / "dual_axis_feature_comparison.csv", index=False, encoding="utf-8-sig")
    stab = direction_stability(comp)
    stab.to_csv(out / "axis_direction_stability.csv", index=False, encoding="utf-8-sig")

    shadow_events, shadow_summary = shadow_matrix(df)
    shadow_events.to_csv(out / "r1c1_shadow_historical_event_ledger.csv", index=False, encoding="utf-8-sig")
    shadow_summary.to_csv(out / "r1c1_shadow_historical_matrix.csv", index=False, encoding="utf-8-sig")

    # Context-only MA224 path audit inside V72 score100 hit events.
    s100_hits = df[df["v72_score100"].eq(1) & df["ignition_group"].eq("HIT")].copy()
    s100_hits["ma224_context_r13"] = np.where(pd.to_numeric(s100_hits.get("below_ma224"), errors="coerce").eq(1), "BELOW224", "AT_OR_ABOVE224")
    marows = []
    for ctx, g in s100_hits.groupby("ma224_context_r13"):
        marows.append({
            "context": ctx,
            "n_hits": int(len(g)),
            "clean_n": int(g["outcome4"].eq("CLEAN_WIN").sum()),
            "damage_n": int(g["outcome4"].isin(["GIVEBACK", "DEEP_MAE_WIN"]).sum()),
            "clean_among_hits_pct": float(g["outcome4"].eq("CLEAN_WIN").mean() * 100.0) if len(g) else np.nan,
            "role": "CONTEXT_ONLY_NOT_GATE",
        })
    pd.DataFrame(marows).to_csv(out / "score100_ma224_path_context_r13.csv", index=False, encoding="utf-8-sig")

    # Amount provenance separation.
    amount_audit = pd.DataFrame([{
        "event_rows": len(df),
        "stage_volume_coverage": int(pd.to_numeric(df.get("pb_volume_vs_wave1"), errors="coerce").notna().sum()),
        "stage_amount_proxy_coverage": int(pd.to_numeric(df.get("pb_amount_proxy_vs_wave1"), errors="coerce").notna().sum()),
        "stage_actual_krx_amount_coverage": 0,
        "d_minus_1_actual_amount_ratio_coverage": int(pd.to_numeric(df.get("universe_amount_ratio_prev_vs20"), errors="coerce").notna().sum()),
        "stage_amount_source": "CLOSE_X_VOLUME_PROXY_ONLY",
        "d_minus_1_amount_source": "V73_CAUSAL_UNIVERSE_REPORTED_TRADING_VALUE_CONTEXT",
        "rule_uses_amount_proxy": 0,
        "rule_uses_actual_d1_amount": 0,
        "note": "R1C1 uses Volume only; proxy amount remains descriptive until stage-wise actual KRX trading value exists.",
    }])
    amount_audit.to_csv(out / "amount_authority_r13.csv", index=False, encoding="utf-8-sig")

    # Freeze ONE research shadow only. This is not a production gate.
    eligible = shadow_events[shadow_events["shadow_eligible"]].copy()
    pos = eligible[eligible["r1c1_shadow_positive"]]
    neg = eligible[~eligible["r1c1_shadow_positive"]]
    hit_pos = float(pos["hit_plus5_d5"].mean() * 100.0) if len(pos) else np.nan
    hit_neg = float(neg["hit_plus5_d5"].mean() * 100.0) if len(neg) else np.nan
    policy = {
        "shadow_id": SHADOW_ID,
        "shadow_revision": SHADOW_REVISION,
        "status": "FROZEN_RESEARCH_ONLY_OOS_PENDING",
        "revision": REVISION,
        "source_run_id": str(args.source_run_id),
        "historical_authority": "DESCRIPTIVE_HISTORICAL_RESEARCH_ONLY",
        "scope": "V72_PULLBACK_RESTART_SCORE_EQ_100",
        "anchor_authority": {
            "method": ANCHOR_METHOD,
            "provenance": ANCHOR_PROVENANCE,
            "temporal_invariant": "PASS_REQUIRED",
            "native_v72_anchor_claim": False,
        },
        "eligibility": [
            "stage_status == PASS",
            "pullback_low_date < signal_date",
            "pb_volume_vs_wave1 is finite",
        ],
        "shadow_positive_condition": "median(PB daily Volume) / median(Wave1 daily Volume) < 1.0",
        "threshold_origin": "STRUCTURAL_RELATION_NOT_SAME_SAMPLE_OPTIMIZED: pullback volume contraction means below Wave1 median volume",
        "ma224": "CONTEXT_ONLY_NOT_GATE",
        "amount": "NOT_USED_IN_RULE; stage amount is Close×Volume proxy only",
        "historical_score100_eligible_n": int(len(eligible)),
        "historical_shadow_positive_n": int(len(pos)),
        "historical_shadow_negative_n": int(len(neg)),
        "historical_positive_hit_rate_pct": hit_pos,
        "historical_negative_hit_rate_pct": hit_neg,
        "historical_hit_rate_gap_pp": hit_pos - hit_neg if math.isfinite(hit_pos) and math.isfinite(hit_neg) else np.nan,
        "oos_append_only_required": True,
        "same_sample_retuning_prohibited": True,
        "production_eligible": False,
        "search_formula_changed": False,
        "score_changed": False,
        "rank_changed": False,
        "order_logic_changed": False,
        "core224_changed": False,
        "triangle1pb_changed": False,
        "low224_changed": False,
    }
    (out / "r1c1_shadow_policy_lock.json").write_text(json.dumps(policy, ensure_ascii=False, indent=2), encoding="utf-8")

    prov = {
        "revision": REVISION,
        "source_run_id": str(args.source_run_id),
        "identity": ident,
        "event_rows": len(df),
        "anchor_rows_pass": int(anchor_ok.sum()),
        "production_search_score_rank_order_changes": 0,
        "same_sample_threshold_tuning": 0,
        "shadow_frozen_research_only": 1,
        "shadow_production_eligible": 0,
        "core224_changed": 0,
        "triangle1pb_changed": 0,
        "low224_changed": 0,
    }
    (out / "r13_provenance.json").write_text(json.dumps(prov, ensure_ascii=False, indent=2), encoding="utf-8")

    # Board: deterministic facts only.
    def get_comp(axis: str, cohort: str, feature: str) -> pd.Series:
        q = comp[(comp.axis == axis) & (comp.cohort == cohort) & (comp.feature == feature)]
        return q.iloc[0] if len(q) else pd.Series(dtype=object)

    ign_wave = get_comp("IGNITION", "V72_SCORE100", "wave1_rise_per_bar_pct")
    ign_pbv = get_comp("IGNITION", "V72_SCORE100", "pb_volume_vs_wave1")
    path_dd = get_comp("PATH_INTEGRITY", "V72_SCORE100", "pb_drawdown_from_high_pct")
    path_sv = get_comp("PATH_INTEGRITY", "V72_SCORE100", "signal_volume_vs_wave1")
    ma = pd.DataFrame(marows).set_index("context") if marows else pd.DataFrame()
    below_clean = num(ma.loc["BELOW224", "clean_among_hits_pct"]) if "BELOW224" in ma.index else np.nan
    above_clean = num(ma.loc["AT_OR_ABOVE224", "clean_among_hits_pct"]) if "AT_OR_ABOVE224" in ma.index else np.nan

    board = f"""🧪 [REAL_FULL STRUCTURE RESEARCH R1.3 · EXACT CAUSAL ANCHOR × DUAL AXIS]\nrevision={REVISION}\nidentity={ident['snapshot_dates']}/{ident['top15_events']}/{ident['d5_mature_events']}/{ident['context_pattern_matrices']} PASS\nauthority=DESCRIPTIVE_HISTORICAL_RESEARCH_ONLY · production/search/score/rank/order changes=0\n\n[Anchor authority]\nevents={len(df)} · AVAILABLE={int(df['anchor_status'].eq('AVAILABLE').sum())} · temporal PASS={int(df['temporal_invariant'].eq('PASS').sum())}\nmethod={ANCHOR_METHOD} · provenance={ANCHOR_PROVENANCE}\nIMPORTANT: research-exact as-of serializer; native V72 anchor claim is NOT made.\nPB low strictly before signal={int(df['pb_anchor_pre_origin'].eq(1).sum())}/{len(df)}; on signal day={int(df['pb_anchor_pre_origin'].eq(0).sum())}\n\n[IGNITION axis · V72 score=100]\nWave1 rise/bar: HIT median={num(ign_wave.get('a_median')):.2f}% vs NO_HIT={num(ign_wave.get('b_median')):.2f}% · Cliff Δ={num(ign_wave.get('cliffs_delta_a_minus_b')):+.3f}\nPB volume/Wave1 volume: HIT median={num(ign_pbv.get('a_median')):.3f} vs NO_HIT={num(ign_pbv.get('b_median')):.3f} · Cliff Δ={num(ign_pbv.get('cliffs_delta_a_minus_b')):+.3f}\nR1C1 natural contraction shadow (<1.0), eligible only: positive n={len(pos)} hit={hit_pos:.1f}% vs negative n={len(neg)} hit={hit_neg:.1f}% · gap={hit_pos-hit_neg:+.1f}pp\n\n[PATH-INTEGRITY axis · score=100 hits]\nPB drawdown from Wave1 high: CLEAN median={num(path_dd.get('a_median')):.2f}% vs DAMAGE={num(path_dd.get('b_median')):.2f}% · Cliff Δ={num(path_dd.get('cliffs_delta_a_minus_b')):+.3f}\nSignal volume/Wave1 volume: CLEAN median={num(path_sv.get('a_median')):.3f} vs DAMAGE={num(path_sv.get('b_median')):.3f} · Cliff Δ={num(path_sv.get('cliffs_delta_a_minus_b')):+.3f}\nMA224 context clean-among-hit: BELOW224={below_clean:.1f}% vs AT/ABOVE224={above_clean:.1f}% · context only, NOT gate\n\n[Amount authority]\nStage-wise actual KRX amount coverage=0/{len(df)} · stage amount is Close×Volume proxy only.\nD-1 causal universe actual amount-ratio coverage={int(pd.to_numeric(df.get('universe_amount_ratio_prev_vs20'), errors='coerce').notna().sum())}/{len(df)}.\nR1C1 rule uses Volume only; amount proxy is NOT used as a gate.\n\n[R1C1 Shadow]\nstatus=FROZEN_RESEARCH_ONLY_OOS_PENDING\nscope=V72 score=100 · condition=PB median Volume / Wave1 median Volume < 1.0\nMA224=CONTEXT_ONLY · OOS append-only required · same-sample retuning prohibited\nproduction_eligible=NO\n"""
    (out / "real_full_structure_research_r13_report.txt").write_text(board, encoding="utf-8")
    print(board)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
