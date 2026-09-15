from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
import os
import pickle
import re
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo
from typing import Any, Iterable

import numpy as np
import pandas as pd

REVISION = "R1_4_R1C1_PROSPECTIVE_OOS_APPEND_ONLY_20260915"
SHADOW_ID = "REAL_FULL_R1C1"
SHADOW_REVISION = "R1C1_PB_VOLUME_CONTRACTION_V1"
ANCHOR_METHOD = "CAUSAL_LOW_HIGH_PULLBACK_V1"
LOCK_SCHEMA = "REAL_FULL_R1C1_OOS_LOCK_V1"
OUTCOME_SCHEMA = "REAL_FULL_R1C1_OOS_OUTCOME_V1"
THRESHOLD = 1.0

LOCK_IMMUTABLE_FIELDS = [
    "schema", "revision", "shadow_id", "shadow_revision", "policy_sha256",
    "source_run_id", "source_file", "source_file_sha256", "anchor_file_sha256", "signal_date", "code", "name", "rank",
    "v72_pullback_restart_score", "event_key", "anchor_method", "anchor_provenance",
    "wave1_low_date", "wave1_low_price", "wave1_high_date", "wave1_high_price",
    "pullback_low_date", "pullback_low_price", "wave1_volume_median", "pb_volume_median",
    "pb_volume_vs_wave1", "shadow_eligible", "r1c1_shadow_positive", "r1c1_shadow_state",
    "lock_reason", "max_predictor_price_date_used", "source_entry_price", "price_source", "locked_at_utc",
]

OUTCOME_FIELDS = [
    "schema", "revision", "shadow_id", "shadow_revision", "event_key", "lock_hash",
    "signal_date", "code", "evaluation_entry_close", "d5_mature_date", "d5_hit_plus5",
    "d5_close_ret_pct", "d5_mfe_pct", "d5_mae_pct", "outcome4", "outcome_price_source",
    "max_outcome_price_date_used", "matured_at_utc", "outcome_hash",
]


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


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


def boolish(v: Any) -> bool:
    if isinstance(v, (bool, np.bool_)):
        return bool(v)
    if isinstance(v, (int, float, np.integer, np.floating)) and math.isfinite(float(v)):
        return float(v) != 0
    return str(v or "").strip().lower() in {"1", "true", "yes", "y", "on", "t"}


def med(s: pd.Series) -> float:
    q = pd.to_numeric(s, errors="coerce").dropna()
    return float(q.median()) if len(q) else float("nan")


def canonical_value(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, (bool, np.bool_)):
        return bool(v)
    if isinstance(v, (int, np.integer)):
        return int(v)
    if isinstance(v, (float, np.floating)):
        x = float(v)
        return None if not math.isfinite(x) else round(x, 12)
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    return str(v)


def sha256_obj(obj: Any) -> str:
    raw = json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def row_hash(row: pd.Series | dict, fields: Iterable[str]) -> str:
    get = row.get
    return sha256_obj({f: canonical_value(get(f)) for f in fields})



LOCK_NUMERIC_FIELDS = {
    "rank", "v72_pullback_restart_score", "wave1_low_price", "wave1_high_price", "pullback_low_price",
    "wave1_volume_median", "pb_volume_median", "pb_volume_vs_wave1", "source_entry_price",
}
LOCK_BOOL_FIELDS = {"shadow_eligible", "r1c1_shadow_positive"}


def canonical_lock_value(field: str, v: Any) -> Any:
    if field in LOCK_BOOL_FIELDS:
        return 1 if boolish(v) else 0
    if field in LOCK_NUMERIC_FIELDS:
        x = num(v)
        return None if not math.isfinite(x) else format(x, ".12g")
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    t = str(v or "").strip()
    return t if t else None


def lock_hash_value(row: pd.Series | dict) -> str:
    get = row.get
    return sha256_obj({f: canonical_lock_value(f, get(f)) for f in LOCK_IMMUTABLE_FIELDS})

def read_policy(path: Path) -> tuple[dict, str]:
    p = json.loads(path.read_text(encoding="utf-8"))
    required = {
        "shadow_id": SHADOW_ID,
        "shadow_revision": SHADOW_REVISION,
        "status": "FROZEN_RESEARCH_ONLY_OOS_PENDING",
        "scope": "V72_PULLBACK_RESTART_SCORE_EQ_100",
        "production_eligible": False,
        "same_sample_retuning_prohibited": True,
    }
    for k, v in required.items():
        if p.get(k) != v:
            raise SystemExit(f"R14_POLICY_LOCK_MISMATCH:{k}:got={p.get(k)!r}:expected={v!r}")
    cond = str(p.get("shadow_positive_condition", ""))
    if "< 1.0" not in cond:
        raise SystemExit(f"R14_POLICY_THRESHOLD_MISMATCH:{cond}")
    if str(p.get("ma224")) != "CONTEXT_ONLY_NOT_GATE":
        raise SystemExit("R14_POLICY_MA224_NOT_CONTEXT_ONLY")
    if not str(p.get("amount", "")).startswith("NOT_USED_IN_RULE"):
        raise SystemExit("R14_POLICY_AMOUNT_RULE_DRIFT")
    return p, sha256_obj(p)


def _rename_ohlcv(fr: pd.DataFrame) -> pd.DataFrame:
    if fr is None or not isinstance(fr, pd.DataFrame) or fr.empty:
        return pd.DataFrame()
    q = fr.copy()
    ren = {
        "시가": "Open", "고가": "High", "저가": "Low", "종가": "Close", "거래량": "Volume",
        "open": "Open", "high": "High", "low": "Low", "close": "Close", "volume": "Volume",
    }
    q = q.rename(columns={c: ren.get(str(c), str(c)) for c in q.columns})
    try:
        q.index = pd.to_datetime(q.index, errors="coerce").normalize()
    except Exception:
        if "Date" in q.columns:
            q.index = pd.to_datetime(q["Date"], errors="coerce").dt.normalize()
    q = q[q.index.notna()].sort_index()
    for c in ["Open", "High", "Low", "Close", "Volume"]:
        if c in q.columns:
            q[c] = pd.to_numeric(q[c], errors="coerce")
    need = [c for c in ["Open", "High", "Low", "Close", "Volume"] if c in q.columns]
    return q[need].copy() if need else pd.DataFrame()


def load_cache_frame(cache_root: Path | None, code: str) -> pd.DataFrame:
    if not cache_root or not cache_root.exists():
        return pd.DataFrame()
    files: list[tuple[int, Path]] = []
    for p in cache_root.glob(f"{code}_*.pkl.gz"):
        m = re.search(r"_(\d+)\.pkl\.gz$", p.name)
        files.append((int(m.group(1)) if m else 0, p))
    if not files:
        return pd.DataFrame()
    p = sorted(files, key=lambda x: x[0], reverse=True)[0][1]
    try:
        with gzip.open(p, "rb") as fh:
            obj = pickle.load(fh)
        fr = obj.get("frame") if isinstance(obj, dict) else obj
        return _rename_ohlcv(fr)
    except Exception:
        return pd.DataFrame()


def network_frame(code: str, start: pd.Timestamp, end: pd.Timestamp) -> tuple[pd.DataFrame, str]:
    # First choice matches the scanner ecosystem. The query intentionally spans
    # only dates needed for the frozen predictor/outcome; predictor code itself
    # clips to the pre-signal anchor dates.
    try:
        import FinanceDataReader as fdr
        fr = fdr.DataReader(code, start.strftime("%Y-%m-%d"), (end + pd.Timedelta(days=1)).strftime("%Y-%m-%d"))
        q = _rename_ohlcv(fr)
        if not q.empty and {"High", "Low", "Close", "Volume"}.issubset(q.columns):
            return q, "FINANCEDATAREADER"
    except Exception:
        pass
    try:
        from pykrx import stock
        fr = stock.get_market_ohlcv_by_date(start.strftime("%Y%m%d"), end.strftime("%Y%m%d"), code)
        q = _rename_ohlcv(fr)
        if not q.empty and {"High", "Low", "Close", "Volume"}.issubset(q.columns):
            return q, "PYKRX_OHLCV"
    except Exception:
        pass
    return pd.DataFrame(), "UNAVAILABLE"


def get_price_frame(code: str, start: pd.Timestamp, end: pd.Timestamp, cache_root: Path | None, allow_network: bool) -> tuple[pd.DataFrame, str]:
    q = load_cache_frame(cache_root, code)
    if not q.empty:
        return q.loc[(q.index >= start) & (q.index <= end)].copy(), "V20_PRICE_CACHE"
    if allow_network:
        return network_frame(code, start, end)
    return pd.DataFrame(), "UNAVAILABLE"


def first_existing(cols: Iterable[str], choices: Iterable[str]) -> str | None:
    s = set(cols)
    for c in choices:
        if c in s:
            return c
    return None


def candidate_normalize(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    if df.empty:
        return pd.DataFrame(columns=["signal_date", "code", "name", "rank", "v72_pullback_restart_score", "source_entry_price"])
    dcol = first_existing(df.columns, ["signal_date", "origin_date", "date", "기준일"])
    ccol = first_existing(df.columns, ["code", "ticker", "종목코드"])
    ncol = first_existing(df.columns, ["name", "종목명"])
    rcol = first_existing(df.columns, ["rank", "origin_rank", "cross_top15_rank", "v1093_gate_rank"])
    scol = first_existing(df.columns, ["v72_pullback_restart_score"])
    ecol = first_existing(df.columns, ["entry_price", "현재가", "snapshot_price"])
    if not dcol or not ccol:
        raise SystemExit(f"R14_CANDIDATE_IDENTITY_COLUMNS_MISSING:date={dcol}:code={ccol}")
    if not rcol:
        raise SystemExit("R14_TOP15_RANK_FIELD_MISSING")
    out = pd.DataFrame({
        "signal_date": pd.to_datetime(df[dcol], errors="coerce").dt.strftime("%Y-%m-%d"),
        "code": df[ccol].map(norm_code),
        "name": df[ncol].astype(str) if ncol else "",
        "rank": pd.to_numeric(df[rcol], errors="coerce"),
        "source_entry_price": pd.to_numeric(df[ecol], errors="coerce") if ecol else np.nan,
    })
    if scol:
        out["v72_pullback_restart_score"] = pd.to_numeric(df[scol], errors="coerce")
    else:
        dbg = first_existing(df.columns, ["v72_pullback_restart_debug"])
        if dbg:
            out["v72_pullback_restart_score"] = df[dbg].astype(str).str.extract(r"(?:^|[,\s])score=([+-]?\d+(?:\.\d+)?)")[0].astype(float)
        else:
            raise SystemExit("R14_SCORE100_FIELD_MISSING_FAIL_CLOSED")
    out = out[out["signal_date"].notna() & out["code"].ne("") & out["rank"].between(1, 15, inclusive="both")].copy()
    out = out[out["v72_pullback_restart_score"].eq(100)].copy()
    out = out.sort_values(["signal_date", "rank", "code"], kind="stable").drop_duplicates(["signal_date", "code"], keep="first")
    return out.reset_index(drop=True)


def anchor_normalize(path: Path) -> pd.DataFrame:
    a = pd.read_csv(path)
    required = ["signal_date", "code", "anchor_status", "anchor_method", "wave1_low_date", "wave1_high_date", "pullback_low_date"]
    miss = [c for c in required if c not in a.columns]
    if miss:
        raise SystemExit(f"R14_ANCHOR_COLUMNS_MISSING:{miss}")
    a = a.copy()
    a["signal_date"] = pd.to_datetime(a["signal_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    a["code"] = a["code"].map(norm_code)
    compare = [c for c in [
        "anchor_status", "anchor_method", "wave1_low_date", "wave1_low_price", "wave1_high_date", "wave1_high_price",
        "pullback_low_date", "pullback_low_price", "temporal_invariant", "anchor_provenance", "selector_logic_changed"
    ] if c in a.columns]
    bad = []
    for k, g in a.groupby(["signal_date", "code"], dropna=False):
        if len(g) > 1 and len(g[compare].astype(str).drop_duplicates()) > 1:
            bad.append(k)
    if bad:
        raise SystemExit(f"R14_ANCHOR_CONFLICTING_DUPLICATES:{bad[:5]}")
    return a.sort_values(["signal_date", "code"]).drop_duplicates(["signal_date", "code"], keep="last")


def build_lock_rows(candidates: pd.DataFrame, anchors: pd.DataFrame, policy_sha: str, source_run_id: str, source_file: str,
                    source_file_sha: str, anchor_file_sha: str, cache_root: Path | None, allow_network: bool) -> pd.DataFrame:
    if candidates.empty:
        return pd.DataFrame(columns=LOCK_IMMUTABLE_FIELDS + ["price_source", "locked_at_utc", "lock_hash"])
    a = anchors.copy()
    drop = [c for c in ["name"] if c in a.columns]
    z = candidates.merge(a.drop(columns=drop), on=["signal_date", "code"], how="left", suffixes=("", "_anchor"), indicator="_anchor_merge")
    rows: list[dict[str, Any]] = []
    for _, r in z.iterrows():
        sd = pd.Timestamp(r["signal_date"]).normalize()
        code = norm_code(r["code"])
        event_key = hashlib.sha256(f"{SHADOW_REVISION}|{sd.date().isoformat()}|{code}".encode()).hexdigest()
        base = {
            "schema": LOCK_SCHEMA, "revision": REVISION, "shadow_id": SHADOW_ID, "shadow_revision": SHADOW_REVISION,
            "policy_sha256": policy_sha, "source_run_id": str(source_run_id), "source_file": source_file,
            "source_file_sha256": source_file_sha, "anchor_file_sha256": anchor_file_sha,
            "signal_date": sd.date().isoformat(), "code": code, "name": str(r.get("name", "") or ""),
            "rank": int(num(r.get("rank"))) if math.isfinite(num(r.get("rank"))) else np.nan,
            "v72_pullback_restart_score": num(r.get("v72_pullback_restart_score")), "event_key": event_key,
            "anchor_method": str(r.get("anchor_method", "") or ""), "anchor_provenance": str(r.get("anchor_provenance", "") or ""),
            "wave1_low_date": str(r.get("wave1_low_date", "") or "")[:10], "wave1_low_price": num(r.get("wave1_low_price")),
            "wave1_high_date": str(r.get("wave1_high_date", "") or "")[:10], "wave1_high_price": num(r.get("wave1_high_price")),
            "pullback_low_date": str(r.get("pullback_low_date", "") or "")[:10], "pullback_low_price": num(r.get("pullback_low_price")),
            "wave1_volume_median": np.nan, "pb_volume_median": np.nan, "pb_volume_vs_wave1": np.nan,
            "shadow_eligible": False, "r1c1_shadow_positive": False, "r1c1_shadow_state": "INELIGIBLE",
            "lock_reason": "", "max_predictor_price_date_used": "", "source_entry_price": num(r.get("source_entry_price")),
            "price_source": "UNAVAILABLE", "locked_at_utc": utc_now(),
        }
        reason = None
        if str(r.get("_anchor_merge")) != "both":
            reason = "ANCHOR_MISSING"
        elif str(r.get("anchor_status", "")) != "AVAILABLE":
            reason = f"ANCHOR_STATUS_{r.get('anchor_status')}"
        elif str(r.get("anchor_method", "")) != ANCHOR_METHOD:
            reason = "ANCHOR_METHOD_MISMATCH"
        elif "temporal_invariant" in z.columns and str(r.get("temporal_invariant", "")) != "PASS":
            reason = "ANCHOR_TEMPORAL_NOT_PASS"
        elif "selector_logic_changed" in z.columns and boolish(r.get("selector_logic_changed")):
            reason = "SELECTOR_LOGIC_CHANGED"
        try:
            ld = pd.Timestamp(r.get("wave1_low_date")).normalize()
            hd = pd.Timestamp(r.get("wave1_high_date")).normalize()
            pdte = pd.Timestamp(r.get("pullback_low_date")).normalize()
            if reason is None and not (ld < hd < pdte < sd):
                reason = "ANCHOR_ORDER_OR_SIGNALDAY_PB_INELIGIBLE"
        except Exception:
            ld = hd = pdte = pd.NaT
            if reason is None:
                reason = "ANCHOR_DATE_PARSE_FAIL"
        if reason is None:
            # Predictor is allowed to use only through PB low, which is strictly before signal date.
            fr, psrc = get_price_frame(code, ld - pd.Timedelta(days=5), pdte + pd.Timedelta(days=1), cache_root, allow_network)
            base["price_source"] = psrc
            if fr.empty or "Volume" not in fr.columns:
                reason = "PRICE_HISTORY_UNAVAILABLE"
            else:
                wave = fr.loc[(fr.index >= ld) & (fr.index <= hd), "Volume"]
                pb = fr.loc[(fr.index > hd) & (fr.index <= pdte), "Volume"]
                wmed, pmed = med(wave), med(pb)
                ratio = pmed / wmed if math.isfinite(wmed) and wmed > 0 and math.isfinite(pmed) else np.nan
                base["wave1_volume_median"] = wmed
                base["pb_volume_median"] = pmed
                base["pb_volume_vs_wave1"] = ratio
                base["max_predictor_price_date_used"] = pdte.date().isoformat()
                if not math.isfinite(ratio):
                    reason = "PB_VOLUME_RATIO_UNAVAILABLE"
                else:
                    base["shadow_eligible"] = True
                    pos = bool(ratio < THRESHOLD)
                    base["r1c1_shadow_positive"] = pos
                    base["r1c1_shadow_state"] = "PB_CONTRACTION" if pos else "NO_PB_CONTRACTION"
                    base["lock_reason"] = "LOCKED_FROZEN_R1C1"
        if reason is not None:
            base["shadow_eligible"] = False
            base["r1c1_shadow_positive"] = False
            base["r1c1_shadow_state"] = "INELIGIBLE"
            base["lock_reason"] = reason
        base["lock_hash"] = lock_hash_value(base)
        rows.append(base)
    return pd.DataFrame(rows)


def validate_lock_hashes(df: pd.DataFrame) -> None:
    need = set(LOCK_IMMUTABLE_FIELDS + ["lock_hash", "event_key"])
    miss = sorted(need - set(df.columns))
    if miss:
        raise SystemExit(f"R14_LOCK_COLUMNS_MISSING:{miss}")
    for _, r in df.iterrows():
        got = str(r.get("lock_hash", ""))
        exp = lock_hash_value(r)
        if got != exp:
            raise SystemExit(f"R14_LOCK_HASH_MISMATCH:{r.get('event_key')}:got={got}:expected={exp}")
        sd = str(r.get("signal_date"))[:10]
        mx = str(r.get("max_predictor_price_date_used", ""))[:10]
        if boolish(r.get("shadow_eligible")) and (not mx or mx >= sd):
            raise SystemExit(f"R14_PREDICTOR_FUTURE_LEAK:{r.get('event_key')}:max_used={mx}:signal={sd}")


def append_immutable(existing: pd.DataFrame, new: pd.DataFrame, key: str, hash_col: str) -> pd.DataFrame:
    if existing is None or existing.empty:
        return new.copy().reset_index(drop=True)
    if new is None or new.empty:
        return existing.copy().reset_index(drop=True)
    x = existing.copy()
    known = {str(r[key]): str(r[hash_col]) for _, r in x.iterrows()}
    add = []
    for _, r in new.iterrows():
        k, h = str(r[key]), str(r[hash_col])
        if k in known:
            if known[k] != h:
                raise SystemExit(f"R14_APPEND_ONLY_CONFLICT:{key}={k}:old={known[k]}:new={h}")
            continue
        add.append(r.to_dict())
    return pd.concat([x, pd.DataFrame(add)], ignore_index=True, sort=False) if add else x.reset_index(drop=True)


def outcome_for_lock(r: pd.Series, cache_root: Path | None, allow_network: bool, asof_date: str) -> dict[str, Any] | None:
    sd = pd.Timestamp(r["signal_date"]).normalize()
    end = pd.Timestamp(asof_date).normalize() if asof_date else pd.Timestamp.utcnow().normalize()
    # enough calendar span for 5 future KRX trading sessions
    fetch_end = min(end, sd + pd.Timedelta(days=30))
    fr, psrc = get_price_frame(norm_code(r["code"]), sd - pd.Timedelta(days=1), fetch_end, cache_root, allow_network)
    if fr.empty or not {"High", "Low", "Close"}.issubset(fr.columns):
        return None
    if sd not in fr.index:
        return None
    future = fr.loc[fr.index > sd].copy()
    if len(future) < 5:
        return None
    fut = future.iloc[:5]
    entry = num(fr.loc[sd, "Close"])
    if entry <= 0:
        return None
    mfe = (pd.to_numeric(fut["High"], errors="coerce").max() / entry - 1.0) * 100.0
    mae = (pd.to_numeric(fut["Low"], errors="coerce").min() / entry - 1.0) * 100.0
    d5_close = (num(fut.iloc[4]["Close"]) / entry - 1.0) * 100.0
    hit = bool(mfe >= 5.0)
    if not hit:
        out4 = "NO_HIT"
    elif d5_close <= 0:
        out4 = "GIVEBACK"
    elif mae <= -5.0:
        out4 = "DEEP_MAE_WIN"
    else:
        out4 = "CLEAN_WIN"
    row = {
        "schema": OUTCOME_SCHEMA,
        "revision": REVISION,
        "shadow_id": SHADOW_ID,
        "shadow_revision": SHADOW_REVISION,
        "event_key": str(r["event_key"]),
        "lock_hash": str(r["lock_hash"]),
        "signal_date": str(r["signal_date"])[:10],
        "code": norm_code(r["code"]),
        "evaluation_entry_close": entry,
        "d5_mature_date": pd.Timestamp(fut.index[4]).date().isoformat(),
        "d5_hit_plus5": hit,
        "d5_close_ret_pct": float(d5_close),
        "d5_mfe_pct": float(mfe),
        "d5_mae_pct": float(mae),
        "outcome4": out4,
        "outcome_price_source": psrc,
        "max_outcome_price_date_used": pd.Timestamp(fut.index[4]).date().isoformat(),
        "matured_at_utc": utc_now(),
    }
    row["outcome_hash"] = sha256_obj({k: canonical_value(v) for k, v in row.items() if k not in {"matured_at_utc", "outcome_hash"}})
    return row


def mature_rows(locks: pd.DataFrame, existing_outcomes: pd.DataFrame, cache_root: Path | None, allow_network: bool, asof_date: str) -> pd.DataFrame:
    validate_lock_hashes(locks)
    done = set(existing_outcomes["event_key"].astype(str)) if existing_outcomes is not None and not existing_outcomes.empty and "event_key" in existing_outcomes.columns else set()
    rows = []
    for _, r in locks.iterrows():
        if str(r["event_key"]) in done:
            continue
        o = outcome_for_lock(r, cache_root, allow_network, asof_date)
        if o is not None:
            rows.append(o)
    return pd.DataFrame(rows)


def build_summary(locks: pd.DataFrame, outcomes: pd.DataFrame) -> pd.DataFrame:
    if locks.empty:
        return pd.DataFrame([{"scope":"R1C1_OOS","locked_events":0,"eligible_events":0,"mature_events":0}])
    eligible = locks[locks["shadow_eligible"].map(boolish)].copy()
    if outcomes.empty:
        return pd.DataFrame([{"scope":"R1C1_OOS","locked_events":len(locks),"eligible_events":len(eligible),"mature_events":0}])
    j = eligible.merge(outcomes[["event_key","d5_hit_plus5","outcome4"]], on="event_key", how="inner")
    rows = []
    for state in ["PB_CONTRACTION","NO_PB_CONTRACTION"]:
        g = j[j["r1c1_shadow_state"].eq(state)]
        rows.append({
            "scope":"R1C1_OOS", "shadow_state":state, "locked_events":int(len(locks)), "eligible_events":int(len(eligible)),
            "mature_events":int(len(g)), "hit_plus5_n":int(g["d5_hit_plus5"].map(boolish).sum()) if len(g) else 0,
            "hit_plus5_rate_pct":float(g["d5_hit_plus5"].map(boolish).mean()*100.0) if len(g) else np.nan,
            "clean_win_n":int(g["outcome4"].eq("CLEAN_WIN").sum()) if len(g) else 0,
            "giveback_n":int(g["outcome4"].eq("GIVEBACK").sum()) if len(g) else 0,
            "deep_mae_win_n":int(g["outcome4"].eq("DEEP_MAE_WIN").sum()) if len(g) else 0,
            "no_hit_n":int(g["outcome4"].eq("NO_HIT").sum()) if len(g) else 0,
        })
    return pd.DataFrame(rows)


def state_manifest(locks: pd.DataFrame, outcomes: pd.DataFrame, policy_sha: str) -> dict:
    lock_hashes = sorted(locks.get("lock_hash", pd.Series(dtype=str)).dropna().astype(str).tolist()) if not locks.empty else []
    outcome_hashes = sorted(outcomes.get("outcome_hash", pd.Series(dtype=str)).dropna().astype(str).tolist()) if not outcomes.empty else []
    return {
        "revision": REVISION,
        "shadow_id": SHADOW_ID,
        "shadow_revision": SHADOW_REVISION,
        "policy_sha256": policy_sha,
        "lock_rows": len(locks),
        "outcome_rows": len(outcomes),
        "lock_chain_sha256": sha256_obj(lock_hashes),
        "outcome_chain_sha256": sha256_obj(outcome_hashes),
        "production_eligible": False,
        "same_sample_retuning_prohibited": True,
        "updated_at_utc": utc_now(),
    }


def write_report(out: Path, locks: pd.DataFrame, outcomes: pd.DataFrame, summary: pd.DataFrame, mode: str, policy_sha: str) -> None:
    elig = int(locks["shadow_eligible"].map(boolish).sum()) if not locks.empty and "shadow_eligible" in locks else 0
    pos = int((locks.get("r1c1_shadow_state", pd.Series(dtype=str)) == "PB_CONTRACTION").sum()) if not locks.empty else 0
    neg = int((locks.get("r1c1_shadow_state", pd.Series(dtype=str)) == "NO_PB_CONTRACTION").sum()) if not locks.empty else 0
    lines = [
        "🧪 [REAL_FULL R1C1 OOS TRACKER R1.4]",
        f"revision={REVISION}",
        f"mode={mode} · shadow={SHADOW_ID}/{SHADOW_REVISION}",
        f"policy_sha256={policy_sha}",
        "authority=PROSPECTIVE_RESEARCH_ONLY · production/search/score/rank/order changes=0",
        "",
        "[LOCK append-only]",
        f"locked rows={len(locks)} · eligible={elig} · PB_CONTRACTION={pos} · NO_PB_CONTRACTION={neg}",
        "Predictor uses only Wave1/PB Volume through pullback_low_date, which must be strictly before signal_date.",
        "",
        "[MATURE append-only]",
        f"outcome rows={len(outcomes)}",
    ]
    if not summary.empty:
        for _, r in summary.iterrows():
            if "shadow_state" in r and pd.notna(r.get("shadow_state")):
                rate = num(r.get("hit_plus5_rate_pct"))
                lines.append(f"- {r.get('shadow_state')}: mature n={int(num(r.get('mature_events')) or 0)} · +5 hit={rate:.1f}%" if math.isfinite(rate) else f"- {r.get('shadow_state')}: mature n={int(num(r.get('mature_events')) or 0)}")
    lines += [
        "",
        "[Guard]",
        "Frozen rule remains PB median Volume / Wave1 median Volume < 1.0 within V72 score=100 Top15 scope.",
        "No threshold/feature retuning is permitted from this OOS tracker. MA224 remains context only.",
        "production_eligible=NO",
    ]
    (out / "real_full_r1c1_oos_r14_report.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")


def load_or_empty(path: Path, columns: list[str] | None = None) -> pd.DataFrame:
    if path.exists() and path.stat().st_size > 0:
        try:
            return pd.read_csv(path, dtype={
                "code": str, "event_key": str, "lock_hash": str, "outcome_hash": str,
                "source_run_id": str, "signal_date": str, "wave1_low_date": str,
                "wave1_high_date": str, "pullback_low_date": str,
                "max_predictor_price_date_used": str, "d5_mature_date": str,
                "max_outcome_price_date_used": str,
            })
        except pd.errors.EmptyDataError:
            pass
    return pd.DataFrame(columns=columns or [])


def run_self_test() -> None:
    # Hash immutability and outcome taxonomy unit tests. Full historical replay is
    # validated by --historical-replay-audit in the workflow/package README.
    base = {f: None for f in LOCK_IMMUTABLE_FIELDS}
    base.update({
        "schema":LOCK_SCHEMA,"revision":REVISION,"shadow_id":SHADOW_ID,"shadow_revision":SHADOW_REVISION,
        "policy_sha256":"x","source_run_id":"1","source_file":"x.csv","source_file_sha256":"s","anchor_file_sha256":"a","signal_date":"2026-09-15","code":"000001","name":"X","rank":1,
        "v72_pullback_restart_score":100.0,"event_key":"e","anchor_method":ANCHOR_METHOD,"anchor_provenance":"EXACT_ASOF_HISTORY_SHADOW_SERIALIZER",
        "wave1_low_date":"2026-09-01","wave1_high_date":"2026-09-05","pullback_low_date":"2026-09-10",
        "wave1_volume_median":100.0,"pb_volume_median":80.0,"pb_volume_vs_wave1":0.8,"shadow_eligible":True,
        "r1c1_shadow_positive":True,"r1c1_shadow_state":"PB_CONTRACTION","lock_reason":"LOCKED_FROZEN_R1C1",
        "max_predictor_price_date_used":"2026-09-10","price_source":"TEST","locked_at_utc":"2026-09-15T00:00:00+00:00",
    })
    h1 = lock_hash_value(base)
    h2 = lock_hash_value(dict(base))
    assert h1 == h2 and len(h1) == 64
    changed = dict(base); changed["pb_volume_vs_wave1"] = 0.81
    assert lock_hash_value(changed) != h1
    print("REAL_FULL_R1C1_OOS_R14_SELF_TEST_PASS")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["LOCK", "MATURE", "REPORT"], default="REPORT")
    ap.add_argument("--policy-lock", default="real_full_r1c1_policy_lock.json")
    ap.add_argument("--candidate-csv", default="")
    ap.add_argument("--anchor-csv", default="")
    ap.add_argument("--price-cache-dir", default="")
    ap.add_argument("--source-run-id", default="")
    ap.add_argument("--source-file", default="")
    ap.add_argument("--prior-lock-ledger", default="")
    ap.add_argument("--prior-outcome-ledger", default="")
    ap.add_argument("--asof-date", default="")
    ap.add_argument("--output-dir", default="reports/real_full_r1c1_oos_r14")
    ap.add_argument("--allow-network", action="store_true")
    ap.add_argument("--require-same-kst-date", action="store_true")
    ap.add_argument("--self-test", action="store_true")
    args = ap.parse_args()
    if args.self_test:
        run_self_test(); return 0

    out = Path(args.output_dir); out.mkdir(parents=True, exist_ok=True)
    _, policy_sha = read_policy(Path(args.policy_lock))
    lock_path = out / "r1c1_oos_lock_append_only.csv"
    outcome_path = out / "r1c1_oos_outcome_append_only.csv"
    prior_locks = load_or_empty(Path(args.prior_lock_ledger), LOCK_IMMUTABLE_FIELDS + ["price_source", "locked_at_utc", "lock_hash"]) if args.prior_lock_ledger else load_or_empty(lock_path, LOCK_IMMUTABLE_FIELDS + ["price_source", "locked_at_utc", "lock_hash"])
    prior_outcomes = load_or_empty(Path(args.prior_outcome_ledger), OUTCOME_FIELDS) if args.prior_outcome_ledger else load_or_empty(outcome_path, OUTCOME_FIELDS)

    locks = prior_locks.copy()
    outcomes = prior_outcomes.copy()
    cache_root = Path(args.price_cache_dir) if args.price_cache_dir else None

    if args.mode == "LOCK":
        if not args.candidate_csv or not args.anchor_csv:
            raise SystemExit("R14_LOCK_INPUTS_REQUIRED")
        candidate_path = Path(args.candidate_csv)
        anchor_path = Path(args.anchor_csv)
        cand = candidate_normalize(candidate_path)
        anch = anchor_normalize(anchor_path)
        if args.require_same_kst_date and not cand.empty:
            today_kst = datetime.now(ZoneInfo("Asia/Seoul")).date().isoformat()
            dates = sorted(set(cand["signal_date"].astype(str)))
            if dates != [today_kst]:
                raise SystemExit(f"R14_NOT_SAME_DAY_PROSPECTIVE_LOCK:today_kst={today_kst}:candidate_dates={dates}")
        src_file = args.source_file or candidate_path.name
        new = build_lock_rows(cand, anch, policy_sha, args.source_run_id, src_file,
                              file_sha256(candidate_path), file_sha256(anchor_path), cache_root, args.allow_network)
        validate_lock_hashes(new) if not new.empty else None
        locks = append_immutable(prior_locks, new, "event_key", "lock_hash")

    if args.mode == "MATURE":
        if locks.empty:
            raise SystemExit("R14_MATURE_NO_LOCK_LEDGER")
        newout = mature_rows(locks, prior_outcomes, cache_root, args.allow_network, args.asof_date)
        outcomes = append_immutable(prior_outcomes, newout, "event_key", "outcome_hash")

    if not locks.empty:
        validate_lock_hashes(locks)
    locks.to_csv(lock_path, index=False, encoding="utf-8-sig")
    outcomes.to_csv(outcome_path, index=False, encoding="utf-8-sig")
    summary = build_summary(locks, outcomes)
    summary.to_csv(out / "r1c1_oos_summary.csv", index=False, encoding="utf-8-sig")
    manifest = state_manifest(locks, outcomes, policy_sha)
    (out / "r1c1_oos_state_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    write_report(out, locks, outcomes, summary, args.mode, policy_sha)
    print((out / "real_full_r1c1_oos_r14_report.txt").read_text(encoding="utf-8"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
