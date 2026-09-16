#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import gzip
import json
import math
import pickle
from pathlib import Path

import numpy as np
import pandas as pd

EXPECTED = {
    "events": 115,
    "score100": 96,
    "causal_eligible": 95,
    "contraction": 51,
    "non_contraction": 44,
    "contraction_hits": 36,
    "non_contraction_hits": 21,
}
ANCHOR_FIELDS = [
    "anchor_status", "anchor_method", "wave1_low_date", "wave1_low_price",
    "wave1_high_date", "wave1_high_price", "pullback_low_date", "pullback_low_price",
    "temporal_invariant", "anchor_provenance", "selector_logic_changed",
]


def norm_code(v) -> str:
    s = str(v or "").strip().upper()
    if s.endswith(".0") and s[:-2].isdigit(): s = s[:-2]
    if len(s) == 7 and s.startswith("A"): s = s[1:]
    return s.zfill(6) if s.isdigit() and len(s) <= 6 else s[-6:]


def eq_series(a: pd.Series, b: pd.Series, numeric: bool) -> pd.Series:
    if numeric:
        x = pd.to_numeric(a, errors="coerce")
        y = pd.to_numeric(b, errors="coerce")
        return pd.Series(np.isclose(x, y, equal_nan=True, rtol=0, atol=1e-9), index=a.index)
    return a.fillna("").astype(str).eq(b.fillna("").astype(str))



def load_materialized_v72(materialized_dir: Path) -> pd.DataFrame:
    rows = []
    for p in sorted(materialized_dir.glob("date_*.pkl.gz")):
        try:
            with gzip.open(p, "rb") as fh:
                obj = pickle.load(fh)
        except Exception as e:
            raise SystemExit(f"R141_MATERIALIZED_READ_FAIL:{p.name}:{type(e).__name__}:{e}")
        if not isinstance(obj, dict):
            raise SystemExit(f"R141_MATERIALIZED_NON_DICT:{p.name}")
        cand = obj.get("candidate_rows", []) or []
        if not isinstance(cand, list):
            raise SystemExit(f"R141_MATERIALIZED_CANDIDATE_ROWS_NON_LIST:{p.name}")
        for i, row in enumerate(cand, 1):
            if not isinstance(row, dict):
                raise SystemExit(f"R141_MATERIALIZED_CANDIDATE_ROW_NON_DICT:{p.name}:{i}")
            z = dict(row)
            # Historical REAL_FULL Top15 authority is list order, not the row's
            # legacy local `rank` field (some expanded candidates reuse rank=1).
            z["origin_rank_replay"] = i
            rows.append(z)
    if not rows:
        raise SystemExit(f"R141_MATERIALIZED_EMPTY:{materialized_dir}")
    df = pd.DataFrame(rows)
    required = [
        "signal_date", "code", "origin_rank_replay",
        "v72_pullback_restart_score", "v72_pullback_restart_score_raw",
    ]
    miss = [c for c in required if c not in df.columns]
    if miss:
        raise SystemExit(f"R141_MATERIALIZED_V72_COLUMNS_MISSING:{miss}")
    df["code"] = df["code"].map(norm_code)
    df["signal_date"] = pd.to_datetime(df["signal_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["origin_rank_replay"] = pd.to_numeric(df["origin_rank_replay"], errors="coerce")
    if df[["signal_date", "code", "origin_rank_replay"]].isna().any().any():
        raise SystemExit("R141_MATERIALIZED_IDENTITY_NA")
    if df.duplicated(["signal_date", "origin_rank_replay", "code"]).any():
        raise SystemExit("R141_MATERIALIZED_IDENTITY_DUPLICATE")
    return df


def validate_materialized_v72(r13: pd.DataFrame, materialized_dir: Path) -> dict:
    m = load_materialized_v72(materialized_dir)
    r = r13.copy()
    r["origin_rank"] = pd.to_numeric(r["origin_rank"], errors="coerce")
    cols = [
        "signal_date", "origin_rank_replay", "code",
        "v72_pullback_restart_score", "v72_pullback_restart_score_raw",
    ]
    j = r.merge(
        m[cols],
        left_on=["origin_date", "origin_rank", "code"],
        right_on=["signal_date", "origin_rank_replay", "code"],
        how="left", suffixes=("_r13", "_materialized"), indicator=True,
    )
    if len(j) != len(r) or not j["_merge"].eq("both").all():
        bad = j.loc[~j["_merge"].eq("both"), ["origin_date", "origin_rank", "code"]].head(10).to_dict("records")
        raise SystemExit(f"R141_MATERIALIZED_V72_IDENTITY_JOIN_FAIL:rows={len(j)}/{len(r)}:bad={bad}")
    out = {"materialized_rows": int(len(m)), "materialized_event_matches": int(len(j))}
    for c in ["v72_pullback_restart_score", "v72_pullback_restart_score_raw"]:
        a = pd.to_numeric(j[c + "_r13"], errors="coerce")
        b = pd.to_numeric(j[c + "_materialized"], errors="coerce")
        eq = np.isclose(a, b, equal_nan=True, rtol=0, atol=1e-9)
        nbad = int((~eq).sum())
        out[c + "_mismatch_count"] = nbad
        if nbad:
            bad = j.loc[~eq, ["origin_date", "origin_rank", "code", c + "_r13", c + "_materialized"]].head(10).to_dict("records")
            raise SystemExit(f"R141_MATERIALIZED_V72_EVENT_MISMATCH:{c}:n={nbad}:bad={bad}")
    out["materialized_v72_score_event_by_event_exact"] = True
    out["materialized_v72_raw_score_event_by_event_exact"] = True
    out["materialized_score100"] = int(pd.to_numeric(j["v72_pullback_restart_score_materialized"], errors="coerce").eq(100).sum())
    if out["materialized_rows"] != EXPECTED["events"] or out["materialized_event_matches"] != EXPECTED["events"]:
        raise SystemExit(f"R141_MATERIALIZED_EVENT_COUNT_FAIL:{out}")
    if out["materialized_score100"] != EXPECTED["score100"]:
        raise SystemExit(f"R141_MATERIALIZED_SCORE100_FAIL:{out['materialized_score100']}")
    return out


def validate(r13_ledger: Path, run1390_anchor: Path, r1c1_ledger: Path | None = None, out_json: Path | None = None, run1390_materialized_dir: Path | None = None) -> dict:
    r = pd.read_csv(r13_ledger, dtype={"code": str})
    a = pd.read_csv(run1390_anchor, dtype={"code": str})
    r["code"] = r["code"].map(norm_code)
    a["code"] = a["code"].map(norm_code)

    score = pd.to_numeric(r["v72_pullback_restart_score"], errors="coerce")
    ratio = pd.to_numeric(r["pb_volume_vs_wave1"], errors="coerce")
    sd = pd.to_datetime(r["origin_date"], errors="coerce")
    pdte = pd.to_datetime(r["pullback_low_date"], errors="coerce")
    hit = r["origin_d5_hit_plus5"].fillna(False).astype(bool)

    score100 = score.eq(100)
    eligible = score100 & r["stage_status"].astype(str).eq("PASS") & pdte.lt(sd) & np.isfinite(ratio)
    pos = eligible & ratio.lt(1.0)
    neg = eligible & ratio.ge(1.0)

    got = {
        "events": int(len(r)),
        "score100": int(score100.sum()),
        "causal_eligible": int(eligible.sum()),
        "contraction": int(pos.sum()),
        "non_contraction": int(neg.sum()),
        "contraction_hits": int((pos & hit).sum()),
        "non_contraction_hits": int((neg & hit).sum()),
    }
    for k, v in EXPECTED.items():
        if got[k] != v:
            raise SystemExit(f"R141_HISTORICAL_IDENTITY_FAIL:{k}:got={got[k]} expected={v}")

    s = r.loc[score100].copy()
    ad = a[["signal_date", "code"] + [c for c in ANCHOR_FIELDS if c in a.columns]].copy()
    dup = ad.duplicated(["signal_date", "code"], keep=False)
    if dup.any():
        bad = ad.loc[dup, ["signal_date", "code"]].drop_duplicates().head(10).to_dict("records")
        raise SystemExit(f"R141_RUN1390_ANCHOR_DUPLICATES:{bad}")
    m = s.merge(ad, left_on=["origin_date", "code"], right_on=["signal_date", "code"], how="left", suffixes=("_r13", "_run1390"), indicator=True)
    if not m["_merge"].eq("both").all():
        raise SystemExit(f"R141_ANCHOR_MATCH_FAIL:{m.loc[m['_merge'].ne('both'), ['origin_date','code']].head(10).to_dict('records')}")

    anchor_mismatch = {}
    for c in ANCHOR_FIELDS:
        rc, ac = c + "_r13", c + "_run1390"
        if rc not in m.columns or ac not in m.columns:
            continue
        numeric = c.endswith("_price")
        eq = eq_series(m[rc], m[ac], numeric)
        nbad = int((~eq).sum())
        anchor_mismatch[c] = nbad
        if nbad:
            raise SystemExit(f"R141_EVENT_BY_EVENT_ANCHOR_MISMATCH:{c}:n={nbad}")

    event_set_exact = None
    if r1c1_ledger and r1c1_ledger.exists():
        q = pd.read_csv(r1c1_ledger, dtype={"code": str})
        q["code"] = q["code"].map(norm_code)
        left = set(zip(s["origin_date"].astype(str), s["code"].astype(str)))
        right = set(zip(q["origin_date"].astype(str), q["code"].astype(str)))
        event_set_exact = (left == right and len(q) == EXPECTED["score100"])
        if not event_set_exact:
            raise SystemExit("R141_SCORE100_EVENT_SET_MISMATCH")

    materialized_check = validate_materialized_v72(r, run1390_materialized_dir) if run1390_materialized_dir else {}

    report = {
        "status": "PASS",
        **materialized_check,
        "authority": "REAL_FULL Run #1390 + R1.3 frozen ledger",
        **got,
        "contraction_hit_rate_pct": 100.0 * got["contraction_hits"] / got["contraction"],
        "non_contraction_hit_rate_pct": 100.0 * got["non_contraction_hits"] / got["non_contraction"],
        "hit_rate_gap_pp": 100.0 * got["contraction_hits"] / got["contraction"] - 100.0 * got["non_contraction_hits"] / got["non_contraction"],
        "score100_anchor_event_matches": int(len(m)),
        "score100_anchor_event_by_event_exact": True,
        "score100_event_set_exact_vs_r1c1_ledger": event_set_exact,
        "anchor_field_mismatch_counts": anchor_mismatch,
        "threshold": "pb_volume_vs_wave1 < 1.0",
        "threshold_changed": False,
        "ma224_gate_used": False,
        "production_eligible": False,
    }
    if out_json:
        out_json.parent.mkdir(parents=True, exist_ok=True)
        out_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return report


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--r13-ledger", required=True)
    ap.add_argument("--run1390-anchor", required=True)
    ap.add_argument("--r1c1-ledger", default="")
    ap.add_argument("--run1390-materialized-dir", default="")
    ap.add_argument("--out-json", default="")
    a = ap.parse_args()
    validate(
        Path(a.r13_ledger),
        Path(a.run1390_anchor),
        Path(a.r1c1_ledger) if a.r1c1_ledger else None,
        Path(a.out_json) if a.out_json else None,
        Path(a.run1390_materialized_dir) if a.run1390_materialized_dir else None,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
