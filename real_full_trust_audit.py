#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

# Shared price/Amount adapters only. No TRIANGLE detector/gate is called.
from triangle1pb_research import AmountAuthority, _load_any, normalize_price_frame

AUDIT_ID = "REAL_FULL_TRUST_AUDIT_R1"
LOADER_REVISION = "REAL_FULL_TRUST_R1_3_CURRENT_SOURCE_BRIDGE_AUTHORITY"
AUTHORITY = "RESEARCH_ONLY_NO_SELECTION_NO_SCORE_NO_RANK_NO_ORDER_CHANGE"

FREEZE_DATE = "2026-09-07"
PROSPECTIVE_START_DATE = "2026-09-08"
BOOTSTRAP_THROUGH = "2026-09-07"

RANK_SOURCE_NAME = "real_full_trust_source.csv"
SOURCE_META_NAME = "real_full_trust_source_meta.json"
RANK_SOURCE_SEMANTICS = "REAL_FULL_ALL_HITS_SORTED_ORDER_AT_GOOGLE_SHEET_HANDOFF"
TOP_N = 15
FORWARD_HORIZONS = (1, 3, 5, 10)

DESCRIPTIVE_MIN_OBSERVED_DAYS = 10
DESCRIPTIVE_MIN_D5_ROWS = 30
VALID_SOURCE_STATUSES = {"SOURCE_READY", "SOURCE_READY_ZERO"}


def _code(v: Any) -> str:
    s = re.sub(r"\D", "", str(v))
    return s[-6:].zfill(6) if s else ""


def _finite(v: Any) -> bool:
    try:
        return math.isfinite(float(v))
    except Exception:
        return False


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _kst_today() -> pd.Timestamp:
    return pd.Timestamp(datetime.now(ZoneInfo("Asia/Seoul")).date())


def _read_csv(path: Path, **kwargs) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, **kwargs)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _load_listing_prices(path: Path) -> pd.DataFrame:
    df = _read_csv(path, dtype={"Code":str, "code":str})
    if df.empty:
        return pd.DataFrame(columns=["code","snapshot_price"])
    code_col = "Code" if "Code" in df.columns else "code" if "code" in df.columns else None
    price_col = "Close" if "Close" in df.columns else "close" if "close" in df.columns else None
    if not code_col or not price_col:
        return pd.DataFrame(columns=["code","snapshot_price"])
    out = pd.DataFrame({
        "code": df[code_col].map(_code),
        "snapshot_price_fallback": pd.to_numeric(df[price_col], errors="coerce"),
    })
    out = out[out["code"].str.len().eq(6) & out["snapshot_price_fallback"].gt(0)]
    return out.drop_duplicates("code", keep="last")


def _load_optional_final_signals(path: Path, signal_date: pd.Timestamp) -> pd.DataFrame:
    df = _read_csv(path, dtype={"code":str})
    if df.empty or "signal_date" not in df.columns or "code" not in df.columns:
        return pd.DataFrame(columns=["code"])
    d = pd.to_datetime(df["signal_date"], errors="coerce").dt.normalize()
    df = df[d.eq(signal_date)].copy()
    if df.empty:
        return pd.DataFrame(columns=["code"])
    df["code"] = df["code"].map(_code)
    wanted = [
        "code","td_label","safe_score","n_score","final_decision",
        "search_pattern_primary","ai_pick_tier","strategy","pattern",
    ]
    for c in wanted:
        if c not in df.columns:
            df[c] = np.nan
    df["final_signal_same_day"] = 1
    return df[wanted + ["final_signal_same_day"]].drop_duplicates("code", keep="last")


def load_current_rank_board(
    rank_source: Path,
    source_meta_path: Path,
    listing_source: Optional[Path],
    final_signal_source: Optional[Path],
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    if not source_meta_path.exists():
        return pd.DataFrame(), {"status":"INVALID_SOURCE_META_MISSING"}

    try:
        bridge = json.loads(source_meta_path.read_text(encoding="utf-8"))
    except Exception as e:
        return pd.DataFrame(), {"status":"INVALID_SOURCE_META_JSON","error":str(e)[:120]}

    capture_status = str(bridge.get("capture_status") or "")
    signal_date_raw = str(bridge.get("signal_date") or "")
    signal_ts = pd.to_datetime(signal_date_raw, errors="coerce")
    if pd.isna(signal_ts):
        return pd.DataFrame(), {
            "status":"INVALID_SOURCE_META_DATE",
            "bridge_capture_status":capture_status,
        }
    signal_date = pd.Timestamp(signal_ts).normalize()
    capture_slot = str(bridge.get("capture_slot") or "UNKNOWN")
    handoff_rows = int(bridge.get("selected_handoff_rows") or 0)

    if capture_status == "CAPTURED_ZERO":
        # Valid zero only when the bridge itself saw the sheet-handoff DataFrame and it was empty.
        return pd.DataFrame(), {
            "status":"SOURCE_READY_ZERO",
            "signal_date":signal_date.date().isoformat(),
            "candidate_rows":0,
            "bridge_capture_status":capture_status,
            "capture_slot":capture_slot,
            "handoff_rows":handoff_rows,
            "snapshot_price_ready":0,
            "rank_source_sha256":_sha(rank_source) if rank_source.exists() else "",
            "source_meta_sha256":_sha(source_meta_path),
            "zero_event_interpretable":1,
        }

    if capture_status != "CAPTURED_NONEMPTY":
        return pd.DataFrame(), {
            "status":"INVALID_BRIDGE_CAPTURE",
            "signal_date":signal_date.date().isoformat(),
            "bridge_capture_status":capture_status,
            "capture_slot":capture_slot,
            "handoff_rows":handoff_rows,
            "zero_event_interpretable":0,
        }

    if not rank_source.exists():
        return pd.DataFrame(), {
            "status":"INVALID_RANK_SOURCE_MISSING",
            "signal_date":signal_date.date().isoformat(),
            "bridge_capture_status":capture_status,
        }

    raw = _read_csv(rank_source, dtype={"code":str})
    if raw.empty:
        return pd.DataFrame(), {
            "status":"INVALID_NONEMPTY_META_BUT_SOURCE_EMPTY",
            "signal_date":signal_date.date().isoformat(),
        }

    required = {
        "signal_date","rank","code","name","snapshot_price",
        "pattern_combo","overlap","score","score_bucket",
        "ai_pick_label","evidence",
    }
    missing = sorted(required - set(raw.columns))
    if missing:
        return pd.DataFrame(), {
            "status":"INVALID_RANK_SOURCE_COLUMNS",
            "signal_date":signal_date.date().isoformat(),
            "missing":"|".join(missing),
        }

    raw["signal_ts"] = pd.to_datetime(raw["signal_date"], errors="coerce").dt.normalize()
    raw = raw[raw["signal_ts"].eq(signal_date)].copy()
    raw["code"] = raw["code"].map(_code)
    raw["rank"] = pd.to_numeric(raw["rank"], errors="coerce")
    raw["score"] = pd.to_numeric(raw["score"], errors="coerce")
    raw["overlap"] = pd.to_numeric(raw["overlap"], errors="coerce")
    raw["snapshot_price"] = pd.to_numeric(raw["snapshot_price"], errors="coerce")
    board = raw[
        raw["code"].str.len().eq(6)
        & raw["rank"].between(1, TOP_N, inclusive="both")
    ].sort_values(["rank","code"]).drop_duplicates("code", keep="first").copy()

    if board.empty:
        return pd.DataFrame(), {
            "status":"INVALID_CURRENT_BOARD_EMPTY",
            "signal_date":signal_date.date().isoformat(),
        }

    # Listing is fallback only. The bridge-captured row price has first authority.
    if listing_source is not None and listing_source.exists():
        fallback = _load_listing_prices(listing_source)
        if not fallback.empty:
            board = board.merge(fallback, on="code", how="left")
            miss = ~pd.to_numeric(board["snapshot_price"], errors="coerce").gt(0)
            board.loc[miss, "snapshot_price"] = board.loc[miss, "snapshot_price_fallback"]
            board = board.drop(columns=["snapshot_price_fallback"], errors="ignore")

    if final_signal_source is not None and final_signal_source.exists():
        fs = _load_optional_final_signals(final_signal_source, signal_date)
        board = board.merge(fs, on="code", how="left")
    if "final_signal_same_day" not in board.columns:
        board["final_signal_same_day"] = 0
    board["final_signal_same_day"] = pd.to_numeric(
        board["final_signal_same_day"], errors="coerce"
    ).fillna(0).astype(int)

    board["rank_bucket"] = pd.cut(
        board["rank"], bins=[0,3,10,15],
        labels=["RANK_1_3","RANK_4_10","RANK_11_15"],
        include_lowest=True,
    ).astype(str)

    board["audit_score_bucket"] = pd.cut(
        board["score"], bins=[-np.inf,399.999999,699.999999,np.inf],
        labels=["SCORE_LT400","SCORE_400_699","SCORE_GE700"],
    ).astype(str)
    board.loc[board["score"].isna(), "audit_score_bucket"] = "SCORE_MISSING"

    board["snapshot_date"] = signal_date.date().isoformat()
    board["source_rank_file"] = rank_source.name
    board["source_rank_sha256"] = _sha(rank_source)
    board["source_listing_sha256"] = _sha(listing_source) if listing_source and listing_source.exists() else ""
    board["selection_authority"] = "FIRST_SUCCESSFUL_SAME_DAY_REAL_FULL_BRIDGE_TOP15"
    board["membership_frozen"] = 1
    board["capture_slot"] = capture_slot

    return board, {
        "status":"SOURCE_READY",
        "signal_date":signal_date.date().isoformat(),
        "candidate_rows":int(len(board)),
        "bridge_capture_status":capture_status,
        "capture_slot":capture_slot,
        "handoff_rows":handoff_rows,
        "rank_source_sha256":_sha(rank_source),
        "source_meta_sha256":_sha(source_meta_path),
        "snapshot_price_ready":int(pd.to_numeric(board["snapshot_price"], errors="coerce").gt(0).sum()),
        "zero_event_interpretable":0,
    }


def load_price_frames(
    price_root: Path,
    amount_root: Path,
    asof_root: Path,
    needed_codes: Optional[set[str]] = None,
) -> Tuple[Dict[str,pd.DataFrame], List[pd.Timestamp], Dict[str,Any]]:
    amount_auth = AmountAuthority(amount_root, asof_root)
    files = sorted(x for x in price_root.rglob("*") if x.is_file()) if price_root.exists() else []
    frames: Dict[str,pd.DataFrame] = {}
    failed = 0
    for p in files:
        try:
            z = normalize_price_frame(_load_any(p), p, amount_auth)
            if z is None:
                failed += 1
                continue
            code, df, _, _ = z
            if needed_codes and code not in needed_codes:
                continue
            x = df[["date","open","high","low","close"]].copy()
            x["date"] = pd.to_datetime(x["date"], errors="coerce").dt.normalize()
            x = x.dropna(subset=["date"]).sort_values("date").drop_duplicates("date", keep="last")
            if code in frames:
                frames[code] = (
                    pd.concat([frames[code],x],ignore_index=True)
                    .sort_values("date").drop_duplicates("date",keep="last").reset_index(drop=True)
                )
            else:
                frames[code] = x.reset_index(drop=True)
        except Exception:
            failed += 1
    trading_dates = sorted({
        pd.Timestamp(d).normalize()
        for df in frames.values()
        for d in pd.to_datetime(df["date"], errors="coerce").dropna().tolist()
    })
    return frames, trading_dates, {
        "price_files":len(files),
        "price_frames":len(frames),
        "price_load_fail":failed,
        "trading_dates":len(trading_dates),
    }


def _event_key(snapshot_date: Any, code: Any) -> str:
    d = pd.to_datetime(snapshot_date, errors="coerce")
    ds = pd.Timestamp(d).date().isoformat() if pd.notna(d) else ""
    return f"{ds}|{_code(code)}"


def _empty_ledger() -> pd.DataFrame:
    cols = [
        "audit_id","loader_revision","snapshot_date","first_observed_at_kst",
        "capture_slot","rank","rank_bucket","code","name","snapshot_price",
        "pattern_combo","overlap","score","score_bucket","audit_score_bucket",
        "ai_pick_label","evidence","final_signal_same_day",
        "td_label","safe_score","n_score","final_decision",
        "search_pattern_primary","ai_pick_tier","strategy","pattern",
        "source_rank_file","source_rank_sha256","source_listing_sha256",
        "selection_authority","membership_frozen","event_key",
    ]
    for h in FORWARD_HORIZONS:
        cols += [
            f"d{h}_complete",f"d{h}_close_ret_pct",
            f"d{h}_mfe_pct",f"d{h}_mae_pct",
            f"d{h}_hit_plus3",f"d{h}_hit_plus5",
        ]
    return pd.DataFrame(columns=cols)


def _mature_row(row: pd.Series, frame: Optional[pd.DataFrame]) -> Dict[str,Any]:
    out: Dict[str,Any] = {}
    for h in FORWARD_HORIZONS:
        out.update({
            f"d{h}_complete":0,
            f"d{h}_close_ret_pct":np.nan,
            f"d{h}_mfe_pct":np.nan,
            f"d{h}_mae_pct":np.nan,
            f"d{h}_hit_plus3":0,
            f"d{h}_hit_plus5":0,
        })
    if frame is None or frame.empty:
        return out
    price = float(row.get("snapshot_price")) if _finite(row.get("snapshot_price")) else np.nan
    if not _finite(price) or price <= 0:
        return out
    d = pd.Timestamp(row["snapshot_date"]).normalize()
    fut = frame[pd.to_datetime(frame["date"],errors="coerce").dt.normalize().gt(d)].copy()
    if fut.empty:
        return out
    for h in FORWARD_HORIZONS:
        if len(fut) < h:
            continue
        path = fut.iloc[:h]
        close_h = float(path.iloc[h-1]["close"])
        high_max = float(pd.to_numeric(path["high"],errors="coerce").max())
        low_min = float(pd.to_numeric(path["low"],errors="coerce").min())
        out[f"d{h}_complete"] = 1
        out[f"d{h}_close_ret_pct"] = (close_h / price - 1.0) * 100.0
        out[f"d{h}_mfe_pct"] = (high_max / price - 1.0) * 100.0
        out[f"d{h}_mae_pct"] = (low_min / price - 1.0) * 100.0
        out[f"d{h}_hit_plus3"] = int(high_max >= price * 1.03)
        out[f"d{h}_hit_plus5"] = int(high_max >= price * 1.05)
    return out


def update_outcomes(ledger: pd.DataFrame, frames: Dict[str,pd.DataFrame]) -> pd.DataFrame:
    if ledger.empty:
        return ledger
    out = ledger.copy()
    for i in out.index:
        code = _code(out.at[i,"code"])
        vals = _mature_row(out.loc[i], frames.get(code))
        for k,v in vals.items():
            out.at[i,k] = v
    return out


def _scorecard(ledger: pd.DataFrame, group_col: str) -> pd.DataFrame:
    if ledger.empty or group_col not in ledger.columns:
        return pd.DataFrame()
    rows = []
    for group, g in ledger.groupby(group_col, dropna=False):
        rec: Dict[str,Any] = {
            "audit_id":AUDIT_ID,
            "loader_revision":LOADER_REVISION,
            "group_type":group_col,
            "group":str(group),
            "rows_total":int(len(g)),
            "observed_days":int(pd.to_datetime(g["snapshot_date"],errors="coerce").nunique()),
        }
        for h in FORWARD_HORIZONS:
            complete = pd.to_numeric(g.get(f"d{h}_complete"),errors="coerce").fillna(0).eq(1)
            x = g[complete].copy()
            rec[f"d{h}_mature_rows"] = int(len(x))
            for metric in ("close_ret_pct","mfe_pct","mae_pct"):
                s = pd.to_numeric(x.get(f"d{h}_{metric}"),errors="coerce").dropna()
                rec[f"d{h}_{metric}_median"] = float(s.median()) if len(s) else np.nan
            for hit in ("hit_plus3","hit_plus5"):
                s = pd.to_numeric(x.get(f"d{h}_{hit}"),errors="coerce").dropna()
                rec[f"d{h}_{hit}_rate"] = float(s.mean()) if len(s) else np.nan
        rows.append(rec)
    return pd.DataFrame(rows)


def build_readiness(
    ledger: pd.DataFrame,
    observation: pd.DataFrame,
    source_meta: Dict[str,Any],
    drift_summary: pd.DataFrame,
) -> pd.DataFrame:
    observed_days = int(observation["observation_status"].eq("OBSERVED_READY").sum()) if not observation.empty else 0
    missed_days = int(observation["observation_status"].eq("MISSED_OBSERVATION").sum()) if not observation.empty else 0
    d5_rows = int(pd.to_numeric(ledger.get("d5_complete",pd.Series(dtype=float)),errors="coerce").fillna(0).eq(1).sum()) if not ledger.empty else 0
    d10_rows = int(pd.to_numeric(ledger.get("d10_complete",pd.Series(dtype=float)),errors="coerce").fillna(0).eq(1).sum()) if not ledger.empty else 0

    source_status = str(source_meta.get("status",""))
    if source_status not in VALID_SOURCE_STATUSES:
        status = "INVALID_CURRENT_SOURCE"
    elif observed_days < DESCRIPTIVE_MIN_OBSERVED_DAYS or d5_rows < DESCRIPTIVE_MIN_D5_ROWS:
        status = "WARMUP_PROSPECTIVE"
    else:
        status = "READY_DESCRIPTIVE_ONLY"

    same_day_drift = int(drift_summary.iloc[-1].get("same_day_snapshot_drift",0) or 0) if not drift_summary.empty else 0
    return pd.DataFrame([{
        "audit_id":AUDIT_ID,
        "loader_revision":LOADER_REVISION,
        "authority":AUTHORITY,
        "freeze_date":FREEZE_DATE,
        "prospective_start_date":PROSPECTIVE_START_DATE,
        "source_status":source_status,
        "bridge_capture_status":source_meta.get("bridge_capture_status",""),
        "status":status,
        "observed_ready_days":observed_days,
        "missed_observation_days":missed_days,
        "ledger_rows":int(len(ledger)),
        "d5_mature_rows":d5_rows,
        "d10_mature_rows":d10_rows,
        "descriptive_min_observed_days":DESCRIPTIVE_MIN_OBSERVED_DAYS,
        "descriptive_min_d5_rows":DESCRIPTIVE_MIN_D5_ROWS,
        "same_day_snapshot_drift_detected":same_day_drift,
        "auto_trust_promotion_allowed":0,
        "selection_logic_changed":0,
        "score_rank_changed":0,
        "order_logic_changed":0,
    }])


def run(args: argparse.Namespace) -> int:
    rank_source = Path(args.rank_source)
    source_meta_path = Path(args.source_meta)
    listing_source = Path(args.listing_source) if args.listing_source else None
    final_signal_source = Path(args.final_signal_source) if args.final_signal_source else None
    ledger_dir = Path(args.ledger_dir)
    out_dir = Path(args.output_dir)
    ledger_dir.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    board, source_meta = load_current_rank_board(
        rank_source, source_meta_path, listing_source, final_signal_source
    )

    ledger_path = ledger_dir / "selection_ledger.csv"
    obs_path = ledger_dir / "observation_coverage.csv"
    state_path = ledger_dir / "state.json"

    ledger = _read_csv(ledger_path, dtype={"code":str}) if ledger_path.exists() else _empty_ledger()
    observation = _read_csv(obs_path, dtype={"observation_date":str}) if obs_path.exists() else pd.DataFrame(
        columns=["audit_id","observation_date","observation_status","first_recorded_at_kst",
                 "source_status","capture_slot","candidate_rows","used_as_zero_event_evidence"]
    )

    state = {}
    if state_path.exists():
        try:
            state = json.loads(state_path.read_text(encoding="utf-8"))
        except Exception:
            state = {}
    if not state:
        state = {
            "audit_id":AUDIT_ID,
            "loader_revision":LOADER_REVISION,
            "bootstrap_through":BOOTSTRAP_THROUGH,
            "last_successful_snapshot_date":BOOTSTRAP_THROUGH,
        }

    needed = set(ledger["code"].map(_code)) if not ledger.empty and "code" in ledger.columns else set()
    if not board.empty:
        needed.update(board["code"].map(_code))
    frames, trading_dates, price_meta = load_price_frames(
        Path(args.price_cache_dir), Path(args.amount_cache_dir), Path(args.asof_cache_dir),
        needed_codes=needed or None,
    )
    ledger = update_outcomes(ledger, frames)

    now_kst = datetime.now(ZoneInfo("Asia/Seoul")).isoformat(timespec="seconds")
    today = _kst_today()
    drift_rows: List[Dict[str,Any]] = []
    admitted = 0
    retroactive_rejected = 0
    zero_observed = 0

    source_status = str(source_meta.get("status",""))
    if source_status in VALID_SOURCE_STATUSES:
        source_date = pd.Timestamp(source_meta["signal_date"]).normalize()
        pstart = pd.Timestamp(PROSPECTIVE_START_DATE).normalize()
        last_success = pd.Timestamp(state.get("last_successful_snapshot_date", BOOTSTRAP_THROUGH)).normalize()

        # Trading-day gaps only; never infer zero for a missed observation.
        known_obs = set(observation["observation_date"].astype(str)) if not observation.empty else set()
        for d in [d for d in trading_dates if last_success < d < source_date]:
            ds = d.date().isoformat()
            if ds not in known_obs:
                observation = pd.concat([observation,pd.DataFrame([{
                    "audit_id":AUDIT_ID,
                    "observation_date":ds,
                    "observation_status":"MISSED_OBSERVATION",
                    "first_recorded_at_kst":now_kst,
                    "source_status":"NO_SUCCESSFUL_REAL_FULL_TRUST_CAPTURE",
                    "capture_slot":"",
                    "candidate_rows":pd.NA,
                    "used_as_zero_event_evidence":0,
                }])],ignore_index=True)

        if source_date < pstart or source_date != today:
            retroactive_rejected = int(len(board))
            source_meta["status"] = "STALE_SOURCE_DATE"
        else:
            ds = source_date.date().isoformat()
            already_observed = (
                not observation.empty
                and observation["observation_date"].astype(str).eq(ds).any()
            )
            capture_slot = str(source_meta.get("capture_slot") or "UNKNOWN")

            if not already_observed:
                if source_status == "SOURCE_READY_ZERO":
                    zero_observed = 1
                    observation = pd.concat([observation,pd.DataFrame([{
                        "audit_id":AUDIT_ID,
                        "observation_date":ds,
                        "observation_status":"OBSERVED_READY",
                        "first_recorded_at_kst":now_kst,
                        "source_status":"SOURCE_READY_ZERO",
                        "capture_slot":capture_slot,
                        "candidate_rows":0,
                        "used_as_zero_event_evidence":1,
                    }])],ignore_index=True)
                    state["last_successful_snapshot_date"] = ds
                else:
                    b = board.copy()
                    b["first_observed_at_kst"] = now_kst
                    b["capture_slot"] = capture_slot
                    b["event_key"] = b.apply(lambda r: _event_key(r["snapshot_date"],r["code"]),axis=1)
                    for h in FORWARD_HORIZONS:
                        b[f"d{h}_complete"] = 0
                        b[f"d{h}_close_ret_pct"] = np.nan
                        b[f"d{h}_mfe_pct"] = np.nan
                        b[f"d{h}_mae_pct"] = np.nan
                        b[f"d{h}_hit_plus3"] = 0
                        b[f"d{h}_hit_plus5"] = 0
                    for c in _empty_ledger().columns:
                        if c not in b.columns:
                            b[c] = np.nan
                    b = b[_empty_ledger().columns]
                    ledger = pd.concat([ledger,b],ignore_index=True)
                    admitted = int(len(b))
                    observation = pd.concat([observation,pd.DataFrame([{
                        "audit_id":AUDIT_ID,
                        "observation_date":ds,
                        "observation_status":"OBSERVED_READY",
                        "first_recorded_at_kst":now_kst,
                        "source_status":"SOURCE_READY",
                        "capture_slot":capture_slot,
                        "candidate_rows":int(len(b)),
                        "used_as_zero_event_evidence":0,
                    }])],ignore_index=True)
                    state["last_successful_snapshot_date"] = ds
            elif source_status == "SOURCE_READY" and not board.empty:
                # Same-day rerun can be audited but never rewrite the first snapshot.
                frozen = ledger[pd.to_datetime(ledger["snapshot_date"],errors="coerce").dt.normalize().eq(source_date)].copy()
                if not frozen.empty:
                    f = frozen[["code","rank","score","pattern_combo"]].copy()
                    c = board[["code","rank","score","pattern_combo"]].copy()
                    f["code"] = f["code"].map(_code)
                    c["code"] = c["code"].map(_code)
                    m = f.merge(c,on="code",how="outer",suffixes=("_frozen","_rerun"),indicator=True)
                    for _,r in m.iterrows():
                        changed = (
                            r["_merge"] != "both"
                            or str(r.get("rank_frozen")) != str(r.get("rank_rerun"))
                            or str(r.get("score_frozen")) != str(r.get("score_rerun"))
                            or str(r.get("pattern_combo_frozen")) != str(r.get("pattern_combo_rerun"))
                        )
                        if changed:
                            drift_rows.append({
                                "audit_id":AUDIT_ID,
                                "snapshot_date":ds,
                                "code":_code(r.get("code")),
                                "merge_status":str(r["_merge"]),
                                "rank_frozen":r.get("rank_frozen"),
                                "rank_rerun":r.get("rank_rerun"),
                                "score_frozen":r.get("score_frozen"),
                                "score_rerun":r.get("score_rerun"),
                                "pattern_frozen":r.get("pattern_combo_frozen"),
                                "pattern_rerun":r.get("pattern_combo_rerun"),
                                "membership_or_field_changed":1,
                                "official_snapshot_rewritten":0,
                            })

    ledger = update_outcomes(ledger, frames)

    drift_detail = pd.DataFrame(drift_rows)
    drift_summary = pd.DataFrame([{
        "audit_id":AUDIT_ID,
        "snapshot_date":source_meta.get("signal_date",""),
        "same_day_rerun_drift_rows":int(len(drift_detail)),
        "same_day_snapshot_drift":int(len(drift_detail)>0),
        "official_snapshot_rewritten":0,
    }])

    rank_scorecard = _scorecard(ledger, "rank_bucket")
    score_scorecard = _scorecard(ledger, "audit_score_bucket")
    pattern_scorecard = _scorecard(ledger, "pattern_combo")
    capture_slot_scorecard = _scorecard(ledger, "capture_slot")
    final_signal_scorecard = _scorecard(ledger, "final_signal_same_day")
    readiness = build_readiness(ledger, observation, source_meta, drift_summary)

    ledger.to_csv(ledger_path,index=False,encoding="utf-8-sig")
    observation.to_csv(obs_path,index=False,encoding="utf-8-sig")
    state.update({
        "loader_revision":LOADER_REVISION,
        "last_run_kst":now_kst,
        "last_source_status":source_meta.get("status",""),
        "last_source_date":source_meta.get("signal_date",""),
        "last_bridge_capture_status":source_meta.get("bridge_capture_status",""),
    })
    state_path.write_text(json.dumps(state,ensure_ascii=False,indent=2),encoding="utf-8")

    ledger.to_csv(out_dir/"real_full_trust_selection_ledger.csv",index=False,encoding="utf-8-sig")
    observation.to_csv(out_dir/"real_full_trust_observation_coverage.csv",index=False,encoding="utf-8-sig")
    drift_detail.to_csv(out_dir/"real_full_trust_same_day_drift_detail.csv",index=False,encoding="utf-8-sig")
    drift_summary.to_csv(out_dir/"real_full_trust_same_day_drift_summary.csv",index=False,encoding="utf-8-sig")
    rank_scorecard.to_csv(out_dir/"real_full_trust_rank_bucket_scorecard.csv",index=False,encoding="utf-8-sig")
    score_scorecard.to_csv(out_dir/"real_full_trust_score_bucket_scorecard.csv",index=False,encoding="utf-8-sig")
    pattern_scorecard.to_csv(out_dir/"real_full_trust_pattern_scorecard.csv",index=False,encoding="utf-8-sig")
    capture_slot_scorecard.to_csv(out_dir/"real_full_trust_capture_slot_scorecard.csv",index=False,encoding="utf-8-sig")
    final_signal_scorecard.to_csv(out_dir/"real_full_trust_final_signal_scorecard.csv",index=False,encoding="utf-8-sig")
    readiness.to_csv(out_dir/"real_full_trust_readiness.csv",index=False,encoding="utf-8-sig")

    source_audit = pd.DataFrame([{
        "audit_id":AUDIT_ID,
        "loader_revision":LOADER_REVISION,
        "rank_source_semantics":RANK_SOURCE_SEMANTICS,
        "rank_source":str(rank_source),
        "source_meta":str(source_meta_path),
        "listing_source":str(listing_source or ""),
        "final_signal_source":str(final_signal_source or ""),
        "freeze_date":FREEZE_DATE,
        "prospective_start":PROSPECTIVE_START_DATE,
        "source_status":source_meta.get("status",""),
        "bridge_capture_status":source_meta.get("bridge_capture_status",""),
        "source_signal_date":source_meta.get("signal_date",""),
        "source_candidate_rows":source_meta.get("candidate_rows",0),
        "handoff_rows":source_meta.get("handoff_rows",0),
        "capture_slot":source_meta.get("capture_slot","UNKNOWN"),
        "snapshot_price_ready":source_meta.get("snapshot_price_ready",0),
        "valid_zero_source":int(source_status=="SOURCE_READY_ZERO"),
        "zero_observed_today":zero_observed,
        "admitted_current_snapshot_rows":admitted,
        "retroactive_source_rows_rejected":retroactive_rejected,
        "price_frames_loaded":price_meta.get("price_frames",0),
        "price_load_fail":price_meta.get("price_load_fail",0),
        "selection_logic_changed":0,
        "score_rank_changed":0,
        "order_logic_changed":0,
    }])
    source_audit.to_csv(out_dir/"real_full_trust_source_audit.csv",index=False,encoding="utf-8-sig")

    rr = readiness.iloc[0]
    report = "\n".join([
        "🧪 [REAL_FULL TRUST AUDIT R1 · CURRENT SOURCE BRIDGE]",
        "RESEARCH ONLY · selection/score/rank/order 변경 0",
        f"freeze {FREEZE_DATE} · prospective start {PROSPECTIVE_START_DATE}",
        f"source={RANK_SOURCE_NAME} · semantics={RANK_SOURCE_SEMANTICS} · Top{TOP_N}",
        f"bridge={source_meta.get('bridge_capture_status','-')} · source status={source_meta.get('status','-')}",
        f"source date {source_meta.get('signal_date','-')} · handoff rows {source_meta.get('handoff_rows',0)} · trust rows {source_meta.get('candidate_rows',0)}",
        f"capture slot {source_meta.get('capture_slot','UNKNOWN')}",
        f"today admitted {admitted} · valid-zero {zero_observed} · retroactive rejected {retroactive_rejected}",
        f"ledger rows {len(ledger)} · observed days {int(rr['observed_ready_days'])} · missed {int(rr['missed_observation_days'])}",
        f"D5 mature {int(rr['d5_mature_rows'])} · D10 mature {int(rr['d10_mature_rows'])}",
        f"status={rr['status']} · auto trust promotion=0",
        "※ 당일 REAL_FULL Google-Sheet handoff 원본 순서를 연구용으로 복제한 source만 authority입니다.",
        "※ 같은 날 재실행으로 rank/score/pattern이 바뀌어도 최초 snapshot은 덮어쓰지 않습니다.",
        "※ HOOK_NOT_CALLED/CAPTURE_INVALID는 0-event가 아니며 실패로 취급합니다.",
    ])
    (out_dir/"real_full_trust_report.txt").write_text(report,encoding="utf-8")
    print(report)
    return 0


def self_test() -> int:
    import tempfile
    with tempfile.TemporaryDirectory() as td:
        root=Path(td)
        rank=root/"source.csv"
        meta=root/"meta.json"

        pd.DataFrame([
            {"signal_date":"2026-09-08","rank":1,"code":"000001","name":"A",
             "snapshot_price":100.0,"pattern_combo":"P1","overlap":1,"score":800,
             "score_bucket":"SOURCE_NATIVE","ai_pick_label":"X","evidence":"{}"},
            {"signal_date":"2026-09-08","rank":2,"code":"000002","name":"B",
             "snapshot_price":200.0,"pattern_combo":"P2","overlap":0,"score":300,
             "score_bucket":"SOURCE_NATIVE","ai_pick_label":"X","evidence":"{}"},
        ]).to_csv(rank,index=False)
        meta.write_text(json.dumps({
            "capture_status":"CAPTURED_NONEMPTY","signal_date":"2026-09-08",
            "capture_slot":"PRIMARY_1440","selected_handoff_rows":20
        }),encoding="utf-8")
        b,m=load_current_rank_board(rank,meta,None,None)
        assert m["status"]=="SOURCE_READY"
        assert len(b)==2
        assert list(b["rank_bucket"])==["RANK_1_3","RANK_1_3"]
        assert list(b["audit_score_bucket"])==["SCORE_GE700","SCORE_LT400"]

        # Valid zero is distinct from missing/failed capture.
        rank.unlink()
        meta.write_text(json.dumps({
            "capture_status":"CAPTURED_ZERO","signal_date":"2026-09-08",
            "capture_slot":"PRIMARY_1440","selected_handoff_rows":0
        }),encoding="utf-8")
        bz,mz=load_current_rank_board(rank,meta,None,None)
        assert bz.empty and mz["status"]=="SOURCE_READY_ZERO"
        assert int(mz["zero_event_interpretable"])==1

        meta.write_text(json.dumps({
            "capture_status":"HOOK_NOT_CALLED","signal_date":"2026-09-08",
            "capture_slot":"PRIMARY_1440","selected_handoff_rows":0
        }),encoding="utf-8")
        bf,mf=load_current_rank_board(rank,meta,None,None)
        assert bf.empty and mf["status"]=="INVALID_BRIDGE_CAPTURE"

        led=_empty_ledger()
        row={c:np.nan for c in led.columns}
        row.update({"snapshot_date":"2026-09-08","code":"000001","snapshot_price":100.0})
        led=pd.concat([led,pd.DataFrame([row])],ignore_index=True)
        frame=pd.DataFrame([
            {"date":"2026-09-09","open":101,"high":106,"low":99,"close":104},
            {"date":"2026-09-10","open":104,"high":108,"low":102,"close":107},
            {"date":"2026-09-11","open":107,"high":109,"low":103,"close":105},
            {"date":"2026-09-14","open":105,"high":110,"low":104,"close":109},
            {"date":"2026-09-15","open":109,"high":111,"low":107,"close":110},
        ])
        out=update_outcomes(led,{"000001":frame})
        assert int(out.iloc[0]["d1_complete"])==1
        assert round(float(out.iloc[0]["d1_close_ret_pct"]),2)==4.0
        assert int(out.iloc[0]["d5_hit_plus5"])==1
    print("REAL_FULL_TRUST_R1_CURRENT_SOURCE_SELF_TEST PASS")
    return 0


def main() -> int:
    ap=argparse.ArgumentParser()
    ap.add_argument("--rank-source",default="reports/real_full_trust_source.csv")
    ap.add_argument("--source-meta",default="reports/real_full_trust_source_meta.json")
    ap.add_argument("--listing-source",default="reports/v73_listing_cache.csv")
    ap.add_argument("--final-signal-source",default="reports/v1080_stockhunter_signals.csv")
    ap.add_argument("--price-cache-dir",default="reports/.cache/v20_price_history")
    ap.add_argument("--amount-cache-dir",default="reports/.cache/v25_actual_amount_history")
    ap.add_argument("--asof-cache-dir",default="reports/.cache/v20_asof_snapshots")
    ap.add_argument("--ledger-dir",default="reports/.cache/real_full_trust_r1")
    ap.add_argument("--output-dir",default="reports/real_full_trust_r1")
    ap.add_argument("--self-test",action="store_true")
    args=ap.parse_args()
    return self_test() if args.self_test else run(args)


if __name__=="__main__":
    raise SystemExit(main())
