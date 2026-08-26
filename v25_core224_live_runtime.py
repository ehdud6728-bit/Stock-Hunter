from __future__ import annotations

"""V25.4.10 CORE224 LIVE/PAPER runtime bridge.

This module is a presentation/execution-audit bridge for the already locked CORE224 policy.
It does NOT create a new strategy, score, rank, live order, or return-tuned threshold.

Authority model
---------------
* Weekly watch-list comes only from the latest WEEKLY_BACKTEST snapshot persisted by
  ``v25_core224_daily_episode_replay.py``.  The snapshot expires if it becomes stale.
* Daily CORE224 state uses the same ``original_thesis_reconstruction.evaluate_core224``
  evaluator and verified trading value only.  Close*Volume is never used as Amount.
* Signal-date universe membership is reconstructed with the existing
  ``historical_asof_universe.HistoricalUniverseRuntime`` in an isolated child process,
  using D-1 and earlier data only.
* Before 15:40 KST only completed daily bars are evaluated (INTRADAY_WATCH).
* At/after 15:40 KST today's bar must be available with actual Amount before EOD_FINAL.
* D+1 execution is PAPER audit only.  No order API is called.
"""

import hashlib
import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

import original_thesis_reconstruction as thesis
import v25_core224_daily_episode_replay as daily

VERSION = "V73.3.6.6.25.4.10"
RESEARCH_ONLY = True
LIVE_LOGIC_CHANGED = False
REAL_ORDER_CHANGED = False
HEADER = "🚦 [CORE224 LIVE BOARD · RESEARCH/PAPER]"

CACHE_SUBDIR = ".cache/v25_core224_live"
SEED_SNAPSHOT_FILE = "weekly_seed_snapshot.csv"
SEED_UNIVERSE_FILE = "seed_universe.csv"
SEED_META_FILE = "seed_meta.json"
EOD_SNAPSHOT_FILE = "live_eod_snapshot_ledger.csv"
RESTART_LEDGER_FILE = "live_restart_ledger.csv"
EXECUTION_LEDGER_FILE = "live_d1_execution_ledger.csv"
RUNTIME_AUDIT_FILE = "v73_v25_core224_live_runtime_audit.csv"
RUNTIME_REPORT_FILE = "v73_v25_core224_live_runtime_report.txt"
LIVE_BOARD_FILE = "v73_v25_core224_live_runtime_board.csv"
D1_BOARD_FILE = "v73_v25_core224_live_runtime_d1_board.csv"

KST = timezone(timedelta(hours=9))


def _out(output_dir: str | Path) -> Path:
    p = Path(output_dir or "reports")
    p.mkdir(parents=True, exist_ok=True)
    return p


def _cache(output_dir: str | Path) -> Path:
    p = _out(output_dir) / CACHE_SUBDIR
    p.mkdir(parents=True, exist_ok=True)
    return p


def _norm_code(v: Any) -> str:
    return thesis._norm_code(v)


def _fmt_date(v: Any) -> str:
    return thesis._fmt_date(v)


def _num(v: Any, default: float = np.nan) -> float:
    try:
        x = float(v)
        return x if np.isfinite(x) else default
    except Exception:
        return default


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, dtype={"code": str, "Code": str})
    except Exception:
        return pd.DataFrame()


def _atomic_csv(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + f".{os.getpid()}.tmp")
    df.to_csv(tmp, index=False, encoding="utf-8-sig")
    os.replace(tmp, path)


def _atomic_json(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + f".{os.getpid()}.tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, sort_keys=True, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def _append_dedup(path: Path, new: pd.DataFrame, keys: List[str]) -> pd.DataFrame:
    if new is None or new.empty:
        return _read_csv(path)
    old = _read_csv(path)
    q = pd.concat([old, new], ignore_index=True, sort=False) if not old.empty else new.copy()
    present = [k for k in keys if k in q.columns]
    if present:
        q = q.drop_duplicates(present, keep="last")
    _atomic_csv(path, q)
    return q


def _load_seed(output_dir: str | Path) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    c = _cache(output_dir)
    wa = _read_csv(c / SEED_SNAPSHOT_FILE)
    su = _read_csv(c / SEED_UNIVERSE_FILE)
    meta: Dict[str, Any] = {}
    try:
        meta = json.loads((c / SEED_META_FILE).read_text(encoding="utf-8"))
    except Exception:
        meta = {}
    if not wa.empty:
        wa["code"] = wa.get("code", pd.Series("", index=wa.index)).map(_norm_code)
        wa["signal_date"] = pd.to_datetime(wa.get("signal_date"), errors="coerce").dt.normalize()
        wa["watch_start_date"] = pd.to_datetime(wa.get("watch_start_date"), errors="coerce").dt.normalize()
        wa["watch_end_exclusive"] = pd.NaT  # latest-known snapshot remains open until refresh
        wa = wa[wa["code"].ne("")].drop_duplicates("code", keep="last")
    if not su.empty:
        su["code"] = su.get("code", pd.Series("", index=su.index)).map(_norm_code)
        su = su[su["code"].ne("")].drop_duplicates("code", keep="last")
    return wa, su, meta


def seed_codes(output_dir: str | Path = "reports") -> List[str]:
    wa, su, _ = _load_seed(output_dir)
    if not wa.empty:
        return sorted(set(wa["code"].astype(str)))
    if not su.empty:
        return sorted(set(su["code"].astype(str)))
    return []


def _seed_name_map(wa: pd.DataFrame, su: pd.DataFrame) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for q in (su, wa):
        if q is None or q.empty:
            continue
        for _, r in q.iterrows():
            code = _norm_code(r.get("code", ""))
            name = str(r.get("name", "") or "").strip()
            if code and name:
                out[code] = name
    return out


def _seed_age(meta: Dict[str, Any], now_date: pd.Timestamp) -> Tuple[Optional[pd.Timestamp], int, bool]:
    d = pd.to_datetime(meta.get("weekly_snapshot_date"), errors="coerce")
    if pd.isna(d):
        return None, 9999, True
    d = pd.Timestamp(d).normalize()
    age = max(0, int((now_date.normalize() - d).days))
    max_age = max(3, int(float(os.getenv("V25_CORE224_LIVE_SEED_MAX_AGE_DAYS", "10"))))
    return d, age, age > max_age


def _latest_base_date(output_dir: str | Path, codes: List[str], meta: Dict[str, Any], before: Optional[pd.Timestamp] = None) -> Optional[pd.Timestamp]:
    cand: List[pd.Timestamp] = []
    bt = pd.to_datetime(meta.get("backtest_end_date"), errors="coerce")
    if pd.notna(bt):
        cand.append(pd.Timestamp(bt).normalize())
    live = _read_csv(_cache(output_dir) / EOD_SNAPSHOT_FILE)
    if not live.empty:
        d = pd.to_datetime(live.get("date"), errors="coerce").dropna().dt.normalize()
        if before is not None:
            d = d[d < before.normalize()]
        if len(d):
            cand.append(pd.Timestamp(d.max()).normalize())
    # Defensive cache check for a few codes; do not scan every file just to resolve the date.
    for code in codes[:5]:
        try:
            px, _ = thesis._read_price_cache_for_code(output_dir, code)
            if px is not None and not px.empty:
                d = pd.to_datetime(px.index, errors="coerce")
                d = d[pd.notna(d)]
                if before is not None:
                    d = d[d.normalize() < before.normalize()]
                if len(d): cand.append(pd.Timestamp(d.max()).normalize())
        except Exception:
            pass
    return max(cand) if cand else None


def _pick_col(df: pd.DataFrame, names: Iterable[str]) -> Optional[str]:
    by = {str(c).strip().lower(): c for c in df.columns}
    for n in names:
        if str(n).strip().lower() in by:
            return by[str(n).strip().lower()]
    return None


def _normalize_eod_raw(ohlcv: pd.DataFrame, cap: pd.DataFrame, asof: pd.Timestamp) -> pd.DataFrame:
    if ohlcv is None or ohlcv.empty:
        return pd.DataFrame()
    q = ohlcv.copy()
    cc = _pick_col(q, ["Code", "code", "티커", "Ticker", "종목코드", "index"])
    if cc is None:
        return pd.DataFrame()
    mapping = {
        "Open": ["Open", "open", "시가"], "High": ["High", "high", "고가"],
        "Low": ["Low", "low", "저가"], "Close": ["Close", "close", "종가", "현재가"],
        "Volume": ["Volume", "volume", "거래량"], "Amount": ["Amount", "amount", "거래대금", "Turnover"],
    }
    out = pd.DataFrame({"code": q[cc].map(_norm_code)})
    for dst, names in mapping.items():
        c = _pick_col(q, names)
        out[dst] = pd.to_numeric(q[c], errors="coerce") if c else np.nan
    if (out["Amount"].isna() | out["Amount"].le(0)).any() and cap is not None and not cap.empty:
        c = cap.copy(); ccc = _pick_col(c, ["Code", "code", "티커", "Ticker", "종목코드", "index"]); ac = _pick_col(c, ["Amount", "amount", "거래대금", "Turnover"])
        if ccc and ac:
            am = pd.DataFrame({"code": c[ccc].map(_norm_code), "_cap_amount": pd.to_numeric(c[ac], errors="coerce")}).drop_duplicates("code", keep="last")
            out = out.merge(am, on="code", how="left")
            out["Amount"] = out["Amount"].where(out["Amount"].gt(0), out["_cap_amount"])
            out = out.drop(columns=["_cap_amount"], errors="ignore")
    out["date"] = asof.strftime("%Y-%m-%d")
    out["amount_is_actual"] = (pd.to_numeric(out["Amount"], errors="coerce").gt(0)).astype(int)
    out["amount_source"] = np.where(out["amount_is_actual"].eq(1), "PYKRX_DAILY_CROSS_SECTION", "MISSING")
    req = out[["Open", "High", "Low", "Close", "Volume"]].apply(pd.to_numeric, errors="coerce")
    ok = req.notna().all(axis=1) & req[["Open", "High", "Low", "Close"]].gt(0).all(axis=1)
    out = out[ok & out["code"].ne("")].copy()
    return out.drop_duplicates("code", keep="last")


def _fetch_eod_snapshot_child(asof: pd.Timestamp, output_dir: str | Path) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Fetch one completed all-market daily bar in a clean interpreter.

    main7 may intentionally install a pykrx stub after an import/auth failure.  A clean child
    avoids mutating that process and makes the CORE224 authority failure explicit.
    """
    c = _cache(output_dir)
    ymd = asof.strftime("%Y%m%d")
    with tempfile.TemporaryDirectory(prefix="v2549_eod_", dir=str(c)) as td:
        td = Path(td); op = td / "ohlcv.csv"; cp = td / "cap.csv"; dp = td / "diag.json"
        code = r'''
import json, sys
from pathlib import Path
import pandas as pd
from pykrx import stock
ymd, op, cp, dp = sys.argv[1:5]
errs=[]; of=[]; cf=[]
def fetch(kind, market):
    names = ["get_market_ohlcv", "get_market_ohlcv_by_ticker"] if kind=="ohlcv" else ["get_market_cap", "get_market_cap_by_ticker"]
    for n in names:
        fn=getattr(stock,n,None)
        if not callable(fn): continue
        for args,kw in [((ymd,),{"market":market}),((ymd,market),{}),((ymd,),{}) if market=="ALL" else ((),{})]:
            if not args: continue
            try:
                z=fn(*args,**kw)
                if isinstance(z,pd.DataFrame) and not z.empty:
                    z=z.reset_index(); z["_market_hint"]=market; return z,n
            except Exception as e: errs.append(f"{kind}:{market}:{n}:{type(e).__name__}:{e}"[:240])
    return pd.DataFrame(),""
for market in ["KOSPI","KOSDAQ"]:
    z,n=fetch("ohlcv",market)
    if not z.empty: of.append(z)
    z,n=fetch("cap",market)
    if not z.empty: cf.append(z)
o=pd.concat(of,ignore_index=True,sort=False) if of else pd.DataFrame()
c=pd.concat(cf,ignore_index=True,sort=False) if cf else pd.DataFrame()
if not o.empty: o.to_csv(op,index=False)
if not c.empty: c.to_csv(cp,index=False)
Path(dp).write_text(json.dumps({"ohlcv_rows":len(o),"cap_rows":len(c),"errors":errs[:20]},ensure_ascii=False),encoding="utf-8")
'''
        try:
            r = subprocess.run([sys.executable, "-c", code, ymd, str(op), str(cp), str(dp)], cwd=str(Path(__file__).resolve().parent), capture_output=True, text=True, timeout=max(30, int(float(os.getenv("V25_CORE224_LIVE_EOD_FETCH_TIMEOUT_SEC", "120")))))
            diag = json.loads(dp.read_text(encoding="utf-8")) if dp.exists() else {}
            diag.update({"returncode": r.returncode, "stderr": (r.stderr or "")[-500:]})
            o = pd.read_csv(op, dtype=str) if op.exists() else pd.DataFrame()
            cap = pd.read_csv(cp, dtype=str) if cp.exists() else pd.DataFrame()
            z = _normalize_eod_raw(o, cap, asof)
            diag["normalized_rows"] = len(z)
            diag["actual_amount_rows"] = int(pd.to_numeric(z.get("amount_is_actual", pd.Series(dtype=float)), errors="coerce").fillna(0).eq(1).sum()) if not z.empty else 0
            return z, diag
        except Exception as exc:
            return pd.DataFrame(), {"returncode": -1, "error": f"{type(exc).__name__}:{exc}"}


def _build_universe_child(asof: pd.Timestamp, output_dir: str | Path) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    c = _cache(output_dir)
    with tempfile.TemporaryDirectory(prefix="v2549_univ_", dir=str(c)) as td:
        td = Path(td); mp = td / "membership.csv"; ap = td / "availability.csv"; dp = td / "diag.json"
        code = r'''
import json, sys
from pathlib import Path
import pandas as pd
import FinanceDataReader as fdr
from pykrx import stock
import historical_asof_universe as h
asof, out, mp, ap, dp = sys.argv[1:6]
try:
    rt=h.HistoricalUniverseRuntime(stock_module=stock, listing_loader=lambda: fdr.StockListing("KRX"), fdr_reader=fdr.DataReader)
    mem, stats, avail=rt.build(asof, output_dir=out)
    if isinstance(mem,pd.DataFrame): mem.to_csv(mp,index=False)
    if isinstance(avail,pd.DataFrame): avail.to_csv(ap,index=False)
    Path(dp).write_text(json.dumps({"membership_rows":len(mem),"availability_rows":len(avail)},ensure_ascii=False),encoding="utf-8")
except Exception as e:
    Path(dp).write_text(json.dumps({"error":f"{type(e).__name__}:{e}"},ensure_ascii=False),encoding="utf-8")
    raise
'''
        try:
            r = subprocess.run([sys.executable, "-c", code, asof.strftime("%Y-%m-%d"), str(_out(output_dir)), str(mp), str(ap), str(dp)], cwd=str(Path(__file__).resolve().parent), capture_output=True, text=True, timeout=max(60, int(float(os.getenv("V25_CORE224_LIVE_UNIVERSE_TIMEOUT_SEC", "210")))))
            diag = json.loads(dp.read_text(encoding="utf-8")) if dp.exists() else {}
            diag.update({"returncode": r.returncode, "stderr": (r.stderr or "")[-500:]})
            mem = pd.read_csv(mp, dtype={"code": str}) if mp.exists() else pd.DataFrame()
            av = pd.read_csv(ap) if ap.exists() else pd.DataFrame()
            return mem, av, diag
        except Exception as exc:
            return pd.DataFrame(), pd.DataFrame(), {"returncode": -1, "error": f"{type(exc).__name__}:{exc}"}


def _load_live_eod(output_dir: str | Path) -> pd.DataFrame:
    q = _read_csv(_cache(output_dir) / EOD_SNAPSHOT_FILE)
    if not q.empty:
        q["code"] = q.get("code", pd.Series("", index=q.index)).map(_norm_code)
        q["date"] = pd.to_datetime(q.get("date"), errors="coerce").dt.normalize()
    return q


def _merge_code_history(output_dir: str | Path, code: str, live_eod: pd.DataFrame, global_amount: pd.DataFrame) -> pd.DataFrame:
    px_raw, _ = thesis._read_price_cache_for_code(output_dir, code)
    if px_raw is None or px_raw.empty:
        return pd.DataFrame()
    try:
        auth = daily._merge_amount_authority(_out(output_dir), code, global_amount)
        q = thesis._overlay_actual_amount(px_raw, code, auth)
    except Exception:
        q = px_raw.copy()
    q.index = pd.to_datetime(q.index, errors="coerce")
    q = q[q.index.notna()].sort_index()
    if live_eod is not None and not live_eod.empty:
        z = live_eod[live_eod["code"].eq(code)].copy()
        if not z.empty:
            z.index = pd.to_datetime(z["date"], errors="coerce")
            zz = pd.DataFrame(index=z.index)
            for c in ["Open", "High", "Low", "Close", "Volume", "Amount", "amount_is_actual", "amount_source"]:
                if c in z.columns: zz[c] = z[c].values
            q = pd.concat([q, zz], axis=0, sort=False)
            q = q[~q.index.duplicated(keep="last")].sort_index()
    return q


def _universe_authority_for_code(code: str, name: str, membership: pd.DataFrame, availability: pd.DataFrame) -> Tuple[int, str]:
    status = ""
    complete = 0
    if availability is not None and not availability.empty:
        r = availability.iloc[-1]
        status = str(r.get("status", "") or "")
        complete = int(float(r.get("complete", 0) or 0))
    member = False
    if membership is not None and not membership.empty:
        cc = membership.get("code", pd.Series("", index=membership.index)).map(_norm_code)
        member = bool(cc.eq(code).any())
    known_good_name = bool(str(name or "").strip()) and not bool(pd.Series([str(name)]).str.contains(r"ETF|ETN|스팩|제[0-9]+호|우$|우A$|우B$|우C$|우선주", regex=True, na=False).iloc[0])
    if member and complete == 1:
        return 1, "LIVE_EXACT_CAUSAL_ASOF_PROVEN"
    if member and status.startswith("VALID_CAUSAL_ASOF") and known_good_name:
        return 1, "LIVE_CONSERVATIVE_POSITIVE_PROOF"
    if complete == 1 and not member:
        return 0, "NOT_IN_CAUSAL_UNIVERSE"
    return 0, status or "LIVE_UNIVERSE_AUTHORITY_PENDING"


def _evaluate(
    output_dir: str | Path,
    asof: pd.Timestamp,
    wa_for_gate: pd.DataFrame,
    su: pd.DataFrame,
    membership: pd.DataFrame,
    availability: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, Dict[str, pd.DataFrame], Dict[str, Any]]:
    codes = sorted(set(wa_for_gate.get("code", pd.Series(dtype=str)).map(_norm_code))) if not wa_for_gate.empty else []
    names = _seed_name_map(wa_for_gate, su)
    live_eod = _load_live_eod(output_dir)
    try:
        global_amount = thesis.load_cached_amount_panel(_out(output_dir), asof, codes, max_files=max(200, int(os.getenv("V25_DAILY_AMOUNT_CACHE_MAX_FILES", "800")))) if codes else pd.DataFrame()
    except Exception:
        global_amount = pd.DataFrame()
    state_rows: List[pd.DataFrame] = []
    restarts: List[Dict[str, Any]] = []
    seed_rows: List[Dict[str, Any]] = []
    px_by_code: Dict[str, pd.DataFrame] = {}
    inv_fail = 0; eval_ok = 0; price_missing = 0; asof_missing = 0
    min_amt_days = thesis.Core224Config().actual_amount_min_history_days
    for code in codes:
        q = _merge_code_history(output_dir, code, live_eod, global_amount)
        if q.empty:
            price_missing += 1; seed_rows.append({"code": code, "name": names.get(code, ""), "daily_eval_status": "PRICE_CACHE_MISSING", "actual_amount_days": 0}); continue
        q = q[pd.to_datetime(q.index).normalize() <= asof].copy()
        if q.empty:
            price_missing += 1; continue
        actual_days = int(pd.to_numeric(q.get("amount_is_actual", pd.Series(0, index=q.index)), errors="coerce").fillna(0).eq(1).sum())
        try:
            d, ev, inv = thesis.evaluate_core224(q)
        except Exception as exc:
            seed_rows.append({"code": code, "name": names.get(code, ""), "daily_eval_status": f"EVAL_ERROR:{type(exc).__name__}", "actual_amount_days": actual_days}); continue
        eval_ok += 1; inv_fail += len(inv) if isinstance(inv, pd.DataFrame) else 0
        px_by_code[code] = thesis._normalize_lifecycle_price(q)
        seed_rows.append({"code": code, "name": names.get(code, ""), "daily_eval_status": "PASS", "actual_amount_days": actual_days})
        if d is None or d.empty: continue
        d["date"] = pd.to_datetime(d.get("date"), errors="coerce").dt.normalize()
        z = d[d["date"].eq(asof)].copy()
        if z.empty:
            asof_missing += 1; continue
        z.insert(0, "version", VERSION); z.insert(1, "code", code); z.insert(2, "name", names.get(code, "")); z["research_only"] = True
        state_rows.append(z)
        if ev is not None and not ev.empty:
            ev = ev.copy(); ev["date"] = pd.to_datetime(ev.get("date"), errors="coerce").dt.normalize()
            rz = ev[ev["date"].eq(asof) & ev.get("to_state", pd.Series("", index=ev.index)).astype(str).eq("CORE224_RESTART")]
            for _, rr in rz.iterrows():
                rec = rr.to_dict(); proven, auth = _universe_authority_for_code(code, names.get(code, ""), membership, availability)
                rec.update({
                    "version": VERSION, "event_id": f"{code}|{asof.strftime('%Y-%m-%d')}", "cycle_id": rec.get("cycle_id", ""),
                    "code": code, "name": names.get(code, ""), "restart_date": asof.strftime("%Y-%m-%d"),
                    "weekly_seed_causal_eligible": 1, "weekly_seed_reason": "LATEST_KNOWN_WEEKLY_SEED_ACTIVE",
                    "daily_universe_membership_proven": proven, "daily_universe_authority": auth,
                    "policy_training_eligible": 0, "research_only": True,
                })
                restarts.append(rec)
    state_df = pd.concat(state_rows, ignore_index=True, sort=False) if state_rows else pd.DataFrame()
    restart_df = pd.DataFrame(restarts)
    seed_runtime = pd.DataFrame(seed_rows)
    meta = {"codes": len(codes), "evaluated": eval_ok, "price_missing": price_missing, "asof_missing": asof_missing, "invariant_fail": inv_fail, "amount_ready": int(pd.to_numeric(seed_runtime.get("actual_amount_days", pd.Series(dtype=float)), errors="coerce").fillna(0).ge(min_amt_days).sum()) if not seed_runtime.empty else 0}
    return state_df, restart_df, seed_runtime, px_by_code, meta


def _restart_ledger_from_board(asof: pd.Timestamp, board: pd.DataFrame) -> pd.DataFrame:
    if board is None or board.empty:
        return pd.DataFrame()
    z = board[board.get("daily_state", pd.Series("", index=board.index)).astype(str).eq("CORE224_RESTART")].copy()
    if z.empty: return pd.DataFrame()
    z["restart_date"] = asof.strftime("%Y-%m-%d")
    z["event_id"] = z["code"].map(lambda c: f"{_norm_code(c)}|{asof.strftime('%Y-%m-%d')}")
    z["captured_at_kst"] = datetime.now(KST).isoformat()
    return z


def _normalize_intraday_snapshot(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame()
    q = df.copy(); cc = _pick_col(q, ["Code", "code", "itemCode", "종목코드"])
    oc = _pick_col(q, ["Open", "open", "시가"])
    if not cc or not oc: return pd.DataFrame()
    z = pd.DataFrame({"code": q[cc].map(_norm_code), "d1_open": pd.to_numeric(q[oc], errors="coerce")})
    z = z[z["code"].ne("") & z["d1_open"].gt(0)].drop_duplicates("code", keep="last")
    return z


def _build_d1_board(output_dir: str | Path, today: pd.Timestamp, prior_asof: Optional[pd.Timestamp], intraday_snapshot: Optional[pd.DataFrame], eod_today: Optional[pd.DataFrame]) -> pd.DataFrame:
    cols = ["version","asof_date","code","name","restart_date","weekly_seed_active","daily_universe_membership_proven","amount_authority_pass","fib61_8_stop","d1_open","paper_action","target_plus5","execution_authority","research_only","paper_only"]
    if prior_asof is None: return pd.DataFrame(columns=cols)
    led = _read_csv(_cache(output_dir) / RESTART_LEDGER_FILE)
    if led.empty: return pd.DataFrame(columns=cols)
    rd = pd.to_datetime(led.get("restart_date"), errors="coerce").dt.normalize()
    led = led[rd.eq(prior_asof.normalize())].copy()
    if led.empty: return pd.DataFrame(columns=cols)
    snap = _normalize_intraday_snapshot(intraday_snapshot)
    authority = "NAVER_CURRENT_SESSION_OPEN"
    if snap.empty and eod_today is not None and not eod_today.empty:
        snap = pd.DataFrame({"code": eod_today["code"].map(_norm_code), "d1_open": pd.to_numeric(eod_today["Open"], errors="coerce")})
        snap = snap[snap["d1_open"].gt(0)].drop_duplicates("code", keep="last")
        authority = "PYKRX_COMPLETED_DAILY_OPEN"
    sm = dict(zip(snap.get("code", pd.Series(dtype=str)), snap.get("d1_open", pd.Series(dtype=float)))) if not snap.empty else {}
    rows = []
    for _, r in led.iterrows():
        code = _norm_code(r.get("code", "")); op = _num(sm.get(code)); stop = _num(r.get("fib61_8_stop"))
        weekly = int(float(r.get("weekly_seed_active", 0) or 0) == 1); proven = int(float(r.get("daily_universe_membership_proven", 0) or 0) == 1); amt = int(float(r.get("amount_authority_pass", 0) or 0) == 1)
        if not np.isfinite(op): action = "WAIT_CURRENT_OPEN"
        elif not (weekly and proven and amt and np.isfinite(stop)): action = "PAPER_ONLY_AUTHORITY_FAIL"
        elif op <= stop: action = "ENTRY_CANCEL_OPEN_AT_OR_BELOW_STOP"
        else: action = "PAPER_ENTRY_AT_D1_OPEN"
        rows.append({"version": VERSION, "asof_date": today.strftime("%Y-%m-%d"), "code": code, "name": str(r.get("name", "") or ""), "restart_date": prior_asof.strftime("%Y-%m-%d"),
                     "weekly_seed_active": weekly, "daily_universe_membership_proven": proven, "amount_authority_pass": amt, "fib61_8_stop": stop, "d1_open": op,
                     "paper_action": action, "target_plus5": op*1.05 if action == "PAPER_ENTRY_AT_D1_OPEN" else np.nan, "execution_authority": authority if np.isfinite(op) else "CURRENT_OPEN_NOT_AVAILABLE",
                     "research_only": True, "paper_only": True})
    return pd.DataFrame(rows, columns=cols)


def _render_runtime_report(mode: str, status: str, today: pd.Timestamp, asof: Optional[pd.Timestamp], seed_meta: Dict[str, Any], seed_age_days: int, seed_stale: bool, eval_meta: Dict[str, Any], universe_status: str, board: pd.DataFrame, d1: pd.DataFrame) -> str:
    lines = [
        HEADER,
        f"📌 {VERSION} · mode={mode} · status={status} · 자동주문 0 · LIVE점수/랭크 변경 0",
        f"🛰️ weekly seed: {seed_meta.get('weekly_snapshot_date','-')} · age {seed_age_days}일 · active-cache {int(seed_meta.get('seed_count',0) or 0)} · calendar {'PASS' if int(seed_meta.get('snapshot_calendar_complete',0) or 0)==1 else 'UNPROVEN'} · {'STALE/BLOCK' if seed_stale else 'FRESH'}",
        f"📅 평가 completed-bar: {_fmt_date(asof) if asof is not None else '-'} · today {today.strftime('%Y-%m-%d')} · universe {universe_status or 'NOT_REQUIRED'}",
        f"🧪 evaluator: codes {eval_meta.get('codes',0)} · pass {eval_meta.get('evaluated',0)} · Amount20-ready {eval_meta.get('amount_ready',0)} · price-missing {eval_meta.get('price_missing',0)} · invariant {eval_meta.get('invariant_fail',0)}",
    ]
    if int(seed_meta.get('snapshot_calendar_complete',0) or 0) != 1:
        lines.append("⛔ Weekly snapshot calendar authority가 불완전하여 신규 진입 gate를 차단합니다. COMPLETE_HANDOFF selected_dates가 필요합니다.")
    if seed_stale:
        lines.append("⛔ 최신 Weekly Seed snapshot이 오래되어 신규 RESTART 진입 gate를 차단합니다. WEEKLY_BACKTEST로 seed snapshot을 갱신하세요.")
    if int(seed_meta.get('snapshot_calendar_complete',0) or 0) == 1 and int(seed_meta.get('seed_count',0) or 0) == 0:
        lines.append("ℹ️ 최신 scheduled Weekly snapshot의 seed가 0개입니다. 이전 주 watch-list를 이월하지 않으며 D+1 기존신호 감사만 계속합니다.")
    if board is None or board.empty:
        lines.extend(["", "🟢/🟡/🔵 CORE224 상태 후보: 없음 또는 평가자료 미완성"])
    else:
        txt = daily._render_live_board(board, d1, asof if asof is not None else today)
        if mode == "INTRADAY_WATCH":
            txt = txt.replace("🟢 [내일 진입 검토 · RESTART 확정]", "🟢 [전일 RESTART · 오늘 D+1 실행 확인]")
            txt = txt.replace("D+1 OPEN 대기 · 시가≤stop이면 취소 · 목표=실제 D+1 체결가×1.05", "오늘 D+1 OPEN은 아래 EXECUTION BOARD 확인")
        lines.extend(["", txt])
    if (board is None or board.empty) and d1 is not None and not d1.empty:
        lines.append("\n🌅 [D+1 EXECUTION BOARD]")
        for i, (_, r) in enumerate(d1.iterrows(), 1):
            lines.append(f"{i}. {r.get('name')} ({r.get('code')}) · {r.get('paper_action')} · open {daily._fmt_money(r.get('d1_open'))} · stop {daily._fmt_money(r.get('fib61_8_stop'))} · target {daily._fmt_money(r.get('target_plus5'))}")
    lines.append("🔒 PRIMARY는 D+1 OPEN → SINGLE → Fib61.8 구조손절 → +5% 전량익절 → 20bp로 고정. 본 런타임은 PAPER 가시화만 수행합니다.")
    return "\n".join(lines)


def run_core224_live_runtime(
    output_dir: str | Path = "reports",
    now_kst: Optional[datetime] = None,
    intraday_snapshot: Optional[pd.DataFrame] = None,
    eod_snapshot_override: Optional[pd.DataFrame] = None,
    universe_membership_override: Optional[pd.DataFrame] = None,
    universe_availability_override: Optional[pd.DataFrame] = None,
) -> Dict[str, Any]:
    out = _out(output_dir); cache = _cache(out)
    now = now_kst.astimezone(KST) if isinstance(now_kst, datetime) and now_kst.tzinfo else (now_kst.replace(tzinfo=KST) if isinstance(now_kst, datetime) else datetime.now(KST))
    today = pd.Timestamp(now.date()).normalize()
    wa, su, seed_meta = _load_seed(out)
    if not seed_meta:
        report = "\n".join([HEADER, f"📌 {VERSION} · status=NEEDS_WEEKLY_BACKTEST_SEED", "⛔ CORE224 LIVE seed cache가 없습니다. V25 WEEKLY_BACKTEST를 1회 완료한 뒤 LIVE를 실행하세요.", "🔒 기존 LIVE 점수·랭크·주문에는 영향 없음"])
        (out / RUNTIME_REPORT_FILE).write_text(report + "\n", encoding="utf-8")
        return {"status": "NEEDS_WEEKLY_BACKTEST_SEED", "report": report, "board": pd.DataFrame(), "d1": pd.DataFrame()}

    seed_date, age_days, stale = _seed_age(seed_meta, today)
    calendar_complete = int(seed_meta.get("snapshot_calendar_complete", 0) or 0) == 1
    codes = sorted(set(wa.get("code", pd.Series(dtype=str)).astype(str))) if not wa.empty else []
    prior_asof = _latest_base_date(out, codes, seed_meta, before=today)
    cutoff_h = int(float(os.getenv("V25_CORE224_LIVE_EOD_FINAL_HOUR", "15"))); cutoff_m = int(float(os.getenv("V25_CORE224_LIVE_EOD_FINAL_MINUTE", "40")))
    after_close = (now.hour, now.minute) >= (cutoff_h, cutoff_m)
    mode = "EOD_FINAL" if after_close else "INTRADAY_WATCH"
    eod_today = pd.DataFrame(); eod_diag: Dict[str, Any] = {}
    asof = prior_asof
    if after_close:
        if eod_snapshot_override is not None:
            eod_today = eod_snapshot_override.copy()
            if "date" not in eod_today.columns: eod_today["date"] = today.strftime("%Y-%m-%d")
            if "amount_is_actual" not in eod_today.columns: eod_today["amount_is_actual"] = pd.to_numeric(eod_today.get("Amount"), errors="coerce").fillna(0).gt(0).astype(int)
            if "amount_source" not in eod_today.columns: eod_today["amount_source"] = np.where(eod_today["amount_is_actual"].eq(1), "PYKRX_DAILY_CROSS_SECTION", "MISSING")
            eod_today["code"] = eod_today.get("code", eod_today.get("Code", pd.Series("", index=eod_today.index))).map(_norm_code)
            eod_diag = {"override": 1, "normalized_rows": len(eod_today), "actual_amount_rows": int(pd.to_numeric(eod_today.get("amount_is_actual"), errors="coerce").fillna(0).eq(1).sum())}
        else:
            eod_today, eod_diag = _fetch_eod_snapshot_child(today, out)
        min_rows = max(100, int(float(os.getenv("V25_CORE224_LIVE_EOD_MIN_MARKET_ROWS", "500"))))
        actual_rows = int(pd.to_numeric(eod_today.get("amount_is_actual", pd.Series(dtype=float)), errors="coerce").fillna(0).eq(1).sum()) if not eod_today.empty else 0
        if len(eod_today) >= min_rows and actual_rows >= min_rows:
            _append_dedup(cache / EOD_SNAPSHOT_FILE, eod_today, ["date", "code"])
            asof = today
        else:
            mode = "EOD_DATA_PENDING"
            asof = prior_asof

    membership = pd.DataFrame(); availability = pd.DataFrame(); universe_diag: Dict[str, Any] = {}
    universe_status = "NOT_FETCHED"
    # Signal-date universe is needed only for a fresh EOD signal date.  It is D-1 causal.
    if asof is not None and pd.Timestamp(asof).normalize() == today and mode == "EOD_FINAL":
        if universe_membership_override is not None:
            membership = universe_membership_override.copy(); availability = universe_availability_override.copy() if isinstance(universe_availability_override, pd.DataFrame) else pd.DataFrame([{"status":"VALID_CAUSAL_ASOF","complete":1}]); universe_diag = {"override": 1}
        else:
            membership, availability, universe_diag = _build_universe_child(asof, out)
        if not availability.empty: universe_status = str(availability.iloc[-1].get("status", "") or "")
        else: universe_status = "LIVE_UNIVERSE_AUTHORITY_PENDING"
    elif asof is not None:
        # Use authority already frozen in the live restart ledger for old RESTARTs; watch states do not need membership.
        universe_status = "PREVIOUS_COMPLETED_BAR_WATCH"

    wa_gate = wa.copy() if (not stale and calendar_complete) else wa.iloc[0:0].copy()
    eval_meta: Dict[str, Any] = {"codes": len(codes), "evaluated": 0, "amount_ready": 0, "price_missing": 0, "invariant_fail": 0}
    board = pd.DataFrame(); restart_df = pd.DataFrame(); seed_runtime = pd.DataFrame(); px_by_code: Dict[str, pd.DataFrame] = {}
    if asof is not None:
        # For intraday watch, no new universe classification is needed.  Persisted RESTART authority is merged below.
        state_df, restart_df, seed_runtime, px_by_code, eval_meta = _evaluate(out, asof, wa, su, membership, availability)
        if mode != "EOD_FINAL":
            old = _read_csv(cache / RESTART_LEDGER_FILE)
            if not old.empty:
                old["restart_date"] = pd.to_datetime(old.get("restart_date"), errors="coerce").dt.normalize()
                old = old[old["restart_date"].eq(asof)].copy()
                if not old.empty:
                    rr = old.rename(columns={"restart_date":"_rd"}).copy(); rr["restart_date"] = asof.strftime("%Y-%m-%d")
                    # Prefer persisted causal authority over a watch-only recomputation.
                    keep = [c for c in ["event_id","cycle_id","code","name","restart_date","weekly_seed_causal_eligible","weekly_seed_reason","daily_universe_membership_proven","daily_universe_authority"] if c in rr.columns]
                    restart_df = rr[keep].copy()
        cfg = thesis.Core224LifecycleConfig(max_follow_days=max(20, int(float(os.getenv("V25_LIFECYCLE_MAX_DAYS", "60")))))
        board = daily._core224_live_board(asof, state_df, restart_df, wa_gate, seed_runtime, px_by_code, cfg)
        if mode == "EOD_FINAL" and not board.empty:
            lednew = _restart_ledger_from_board(asof, board)
            if not lednew.empty: _append_dedup(cache / RESTART_LEDGER_FILE, lednew, ["event_id"])

    d1 = _build_d1_board(out, today, prior_asof, intraday_snapshot, eod_today)
    if not d1.empty:
        _append_dedup(cache / EXECUTION_LEDGER_FILE, d1, ["code", "restart_date", "asof_date"])

    if mode == "EOD_DATA_PENDING": status = "EOD_DATA_PENDING_NO_SIGNAL_FINALIZE"
    elif not calendar_complete: status = "INVALID_WEEKLY_SNAPSHOT_CALENDAR_AUTHORITY"
    elif stale: status = "SEED_STALE_ENTRY_BLOCKED"
    elif asof is None: status = "NO_COMPLETED_BAR_AUTHORITY"
    elif int(eval_meta.get("invariant_fail", 0) or 0) > 0: status = "INVALID_CORE224_INVARIANT"
    elif len(codes) == 0: status = "PASS_NO_ACTIVE_WEEKLY_SEED"
    else: status = "PASS_PAPER_ONLY"
    report = _render_runtime_report(mode, status, today, asof, seed_meta, age_days, stale, eval_meta, universe_status, board, d1)
    (out / RUNTIME_REPORT_FILE).write_text(report + "\n", encoding="utf-8")
    _atomic_csv(out / LIVE_BOARD_FILE, board if isinstance(board, pd.DataFrame) else pd.DataFrame())
    _atomic_csv(out / D1_BOARD_FILE, d1 if isinstance(d1, pd.DataFrame) else pd.DataFrame())
    audit = pd.DataFrame([{
        "version": VERSION, "ts_kst": now.isoformat(), "mode": mode, "status": status, "today": today.strftime("%Y-%m-%d"), "asof_date": _fmt_date(asof),
        "seed_snapshot_date": seed_meta.get("weekly_snapshot_date", ""), "seed_age_days": age_days, "seed_stale": int(stale), "seed_codes": len(codes),
        "snapshot_calendar_complete": int(calendar_complete), "snapshot_calendar_source": str(seed_meta.get("snapshot_calendar_source", "")),
        "evaluated": eval_meta.get("evaluated",0), "amount_ready": eval_meta.get("amount_ready",0), "price_missing": eval_meta.get("price_missing",0), "invariant_fail": eval_meta.get("invariant_fail",0),
        "universe_status": universe_status, "universe_rows": len(membership), "board_rows": len(board), "entry_review": int(board.get("section",pd.Series(dtype=str)).astype(str).eq("ENTRY_REVIEW").sum()) if not board.empty else 0,
        "restart_wait": int(board.get("section",pd.Series(dtype=str)).astype(str).eq("RESTART_WAIT").sum()) if not board.empty else 0, "excluded_restart": int(board.get("section",pd.Series(dtype=str)).astype(str).eq("EXCLUDED_RESTART").sum()) if not board.empty else 0,
        "d1_rows": len(d1), "d1_paper_entry": int(d1.get("paper_action",pd.Series(dtype=str)).astype(str).eq("PAPER_ENTRY_AT_D1_OPEN").sum()) if not d1.empty else 0,
        "eod_fetch_diag": json.dumps(eod_diag, ensure_ascii=False)[:1000], "universe_diag": json.dumps(universe_diag, ensure_ascii=False)[:1000],
        "research_only": True, "live_score_rank_changed": False, "real_order_changed": False,
    }])
    _append_dedup(out / RUNTIME_AUDIT_FILE, audit, ["ts_kst"])
    return {"status": status, "mode": mode, "asof": asof, "board": board, "d1": d1, "report": report, "audit": audit, "seed_meta": seed_meta}


if __name__ == "__main__":
    r = run_core224_live_runtime(os.getenv("V1080_BACKTEST_OUTPUT_DIR", "reports"))
    print(r.get("report", ""))
