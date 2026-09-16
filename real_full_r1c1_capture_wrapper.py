#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import json, math, traceback
from pathlib import Path
from typing import Any
import pandas as pd
import real_full_capture_runner as base

REVISION = "R1_4_1_RUNTIME_V72_EXACT_SIDECAR_20260916"
SIDE_CSV = Path("reports/real_full_r1c1_v72_runtime_sidecar.csv")
ANCHOR_CSV = Path("reports/v72_formula_selector_anchor_serialization.csv")
META_JSON = Path("reports/real_full_r1c1_v72_runtime_sidecar_meta.json")
REPORT_TXT = Path("reports/real_full_r1c1_v72_runtime_sidecar_report.txt")
TOP_N = 15
ANCHOR_METHOD = "CAUSAL_LOW_HIGH_PULLBACK_V1"
ANCHOR_PROVENANCE = "EXACT_ASOF_HISTORY_SHADOW_SERIALIZER"

def _norm_code(v: Any) -> str:
    s = str(v or "").strip().upper()
    if s.endswith(".0") and s[:-2].isdigit(): s = s[:-2]
    for suffix in (".KS", ".KQ", ".KRX"):
        if s.endswith(suffix):
            s = s[:-len(suffix)]
            break
    s = "".join(ch for ch in s if ch.isalnum())
    if len(s) == 7 and s.startswith("A"): s = s[1:]
    if s.isdigit() and len(s) <= 6: return s.zfill(6)
    return s[-6:] if len(s) >= 6 else s

def _first(row, names, default=""):
    for c in names:
        if c in row.index:
            v = row.get(c)
            try:
                if pd.isna(v): continue
            except Exception:
                pass
            if str(v).strip() != "": return v
    return default

def _num(v, default=float("nan")):
    try:
        x=float(str(v).replace(",","").replace("%","").strip())
        return x if math.isfinite(x) else default
    except Exception:
        return default

def _walk_frames(caller):
    out=[]
    f=caller
    while f is not None:
        out.append(f); f=f.f_back
    return out

def _find_ai(frames):
    for f in frames:
        for scope in (f.f_locals,f.f_globals):
            x=scope.get("ai_candidates")
            if isinstance(x,pd.DataFrame): return x.copy()
            if isinstance(x,list) and (not x or all(isinstance(i,dict) for i in x)):
                return pd.DataFrame(x)
    return None

def _find_api(frames):
    for f in frames:
        g=f.f_globals
        det=g.get("_v1107_4_5_72_detect_pullback_restart")
        fetch=g.get("fdr_cached")
        if callable(det) and callable(fetch):
            return {"detect":det,"fetch":fetch,"module_name":str(g.get("__name__",""))}
    return {}

def _history(fetch, code):
    fr=fetch(code,days=220)
    if not isinstance(fr,pd.DataFrame): return pd.DataFrame()
    q=fr.copy()
    if not isinstance(q.index,pd.DatetimeIndex):
        q.index=pd.to_datetime(q.index,errors="coerce")
    q=q[q.index.notna()].sort_index()
    for c in ["Open","High","Low","Close","Volume"]:
        if c in q.columns: q[c]=pd.to_numeric(q[c],errors="coerce")
    return q

def _top15(ai):
    if ai is None or ai.empty: return pd.DataFrame()
    rows=[]; seen=set()
    for _,r in ai.reset_index(drop=True).iterrows():
        code=_norm_code(_first(r,["종목코드","code","Code","ticker","Ticker","stock_code"]))
        if not code or code in seen: continue
        seen.add(code)
        rows.append({
            "signal_date":base._signal_date(),
            "rank":len(rows)+1,
            "code":code,
            "name":str(_first(r,["종목명","name","Name","stock_name"],"")),
            "snapshot_price":_num(_first(r,["현재가","종가","Close","close","price","Price","entry_price"],float("nan")))
        })
        if len(rows)>=TOP_N: break
    return pd.DataFrame(rows)

def _fail(reason, extra=None):
    SIDE_CSV.parent.mkdir(parents=True,exist_ok=True)
    meta={
        "revision":REVISION,"status":"FAIL_CLOSED","reason":reason,
        "signal_date":base._signal_date(),"research_only":True,
        "selection_logic_changed":False,"score_rank_changed":False,
        "order_logic_changed":False,"production_eligible":False,
    }
    if extra: meta.update(extra)
    META_JSON.write_text(json.dumps(meta,ensure_ascii=False,indent=2,default=str),encoding="utf-8")
    REPORT_TXT.write_text(
        "🧪 [REAL_FULL R1C1 R1.4.1 RUNTIME SIDECAR]\n"
        f"status=FAIL_CLOSED\nreason={reason}\n"
        "production/search/score/rank/order changes=0\n",encoding="utf-8")

def build_runtime_sidecar(caller_frame):
    frames=_walk_frames(caller_frame)
    ai=_find_ai(frames); api=_find_api(frames)
    if ai is None or ai.empty:
        _fail("AI_CANDIDATES_RUNTIME_FRAME_MISSING"); return False
    if not api:
        _fail("EXACT_V72_RUNTIME_API_MISSING"); return False
    top=_top15(ai)
    if top.empty:
        _fail("TOP15_CANONICAL_EMPTY"); return False

    try:
        import search_formula_complete_pipeline as sfcp
        anchor_fn=getattr(sfcp,"causal_anchor_v1",None)
    except Exception as e:
        _fail(f"ANCHOR_MODULE_IMPORT_FAIL:{type(e).__name__}:{e}"); return False
    if not callable(anchor_fn):
        _fail("CAUSAL_ANCHOR_V1_MISSING"); return False

    side_rows=[]; anchor_rows=[]; errors=[]
    for _,r in top.iterrows():
        code=r["code"]
        try:
            hist=_history(api["fetch"],code)
            if hist.empty or not {"High","Low","Close","Volume"}.issubset(hist.columns):
                raise RuntimeError("OHLCV_HISTORY_UNAVAILABLE")
            prc=api["detect"](hist.copy())
            if not isinstance(prc,dict):
                raise RuntimeError("V72_DETECTOR_NON_DICT")
            score=_num(prc.get("score"))
            raw=_num(prc.get("raw_score",prc.get("score_raw")))
            side_rows.append({
                "revision":REVISION,"signal_date":r["signal_date"],"rank":int(r["rank"]),
                "code":code,"name":r["name"],"snapshot_price":r["snapshot_price"],
                "v72_pullback_restart_score":score,
                "v72_pullback_restart_score_raw":raw,
                "v72_pullback_restart_grade":str(prc.get("grade","") or ""),
                "v72_pullback_restart_ok":bool(prc.get("ok",False)),
                "v72_pullback_restart_reason":str(prc.get("reason","") or ""),
                "v72_pullback_restart_debug":str(prc.get("debug","") or ""),
                "v72_impulse_pct":_num(prc.get("impulse_pct")),
                "v72_pullback_days":_num(prc.get("pullback_days")),
                "v72_support_count":_num(prc.get("support_count")),
                "v72_volume_ratio20":_num(prc.get("volume_ratio20")),
                "v72_headroom_pct":_num(prc.get("headroom_pct")),
                "v72_entry_line":_num(prc.get("entry_line")),
                "v72_stop_line":_num(prc.get("stop_line")),
                "runtime_v72_source":api.get("module_name",""),
                "runtime_capture_semantics":"EXACT_EXISTING_TOP15_ORDER_AT_REAL_FULL_GRACEFUL_SHUTDOWN",
                "selection_logic_changed":False,"score_rank_changed":False,"order_logic_changed":False,
            })
            a=anchor_fn(hist.copy(),r["signal_date"])
            if not isinstance(a,dict): raise RuntimeError("ANCHOR_NON_DICT")
            a=dict(a)
            a.update({
                "version":REVISION,"code":code,"name":r["name"],
                "anchor_method":a.get("anchor_method",ANCHOR_METHOD),
                "anchor_provenance":ANCHOR_PROVENANCE,
                "selector_logic_changed":False,
            })
            anchor_rows.append(a)
        except Exception as e:
            errors.append(f"{code}:{type(e).__name__}:{e}")

    side=pd.DataFrame(side_rows); anchors=pd.DataFrame(anchor_rows)
    if len(side)!=len(top) or len(anchors)!=len(top):
        _fail("PARTIAL_SERIALIZATION",{
            "top15_rows":len(top),"sidecar_rows":len(side),
            "anchor_rows":len(anchors),"errors":errors[:20]})
        return False

    for c in ["signal_date","code","anchor_status","anchor_method",
              "wave1_low_date","wave1_high_date","pullback_low_date",
              "temporal_invariant","anchor_provenance","selector_logic_changed"]:
        if c not in anchors.columns: anchors[c]=""

    SIDE_CSV.parent.mkdir(parents=True,exist_ok=True)
    side.to_csv(SIDE_CSV,index=False,encoding="utf-8-sig")
    anchors.to_csv(ANCHOR_CSV,index=False,encoding="utf-8-sig")
    score100=int(pd.to_numeric(side["v72_pullback_restart_score"],errors="coerce").eq(100).sum())
    avail=int(anchors["anchor_status"].astype(str).eq("AVAILABLE").sum())
    tpass=int(anchors["temporal_invariant"].astype(str).eq("PASS").sum())
    meta={
        "revision":REVISION,"status":"PASS","signal_date":base._signal_date(),
        "top15_rows":len(top),"sidecar_rows":len(side),"score100_rows":score100,
        "anchor_rows":len(anchors),"anchor_available_rows":avail,"temporal_pass_rows":tpass,
        "anchor_method":ANCHOR_METHOD,"anchor_provenance":ANCHOR_PROVENANCE,
        "runtime_v72_source":api.get("module_name",""),"research_only":True,
        "selection_logic_changed":False,"score_rank_changed":False,
        "order_logic_changed":False,"production_eligible":False,"errors":errors,
    }
    META_JSON.write_text(json.dumps(meta,ensure_ascii=False,indent=2,default=str),encoding="utf-8")
    REPORT_TXT.write_text(
        "🧪 [REAL_FULL R1C1 R1.4.1 RUNTIME SIDECAR]\n"
        f"status=PASS\ndate={base._signal_date()} top15={len(top)} score100={score100}\n"
        f"anchors={avail}/{len(anchors)} temporal_pass={tpass}/{len(anchors)}\n"
        f"v72_source={api.get('module_name','')}\n"
        f"anchor={ANCHOR_METHOD} / {ANCHOR_PROVENANCE}\n"
        "production/search/score/rank/order changes=0\n",encoding="utf-8")
    print(REPORT_TXT.read_text(encoding="utf-8"))
    return True

_ORIGINAL=base.BridgeState.capture_runtime_frames
def _patched(self,caller_frame):
    ok=_ORIGINAL(self,caller_frame)
    try: build_runtime_sidecar(caller_frame)
    except Exception as e:
        traceback.print_exc(); _fail(f"UNHANDLED:{type(e).__name__}:{e}")
    return ok

def self_test():
    assert hasattr(base,"BridgeState")
    assert callable(getattr(base.BridgeState,"capture_runtime_frames",None))
    assert callable(getattr(base,"main",None))
    import search_formula_complete_pipeline as sfcp
    assert callable(getattr(sfcp,"causal_anchor_v1",None))
    print("REAL_FULL_R141_RUNTIME_SIDECAR_SELF_TEST_PASS")
    return 0

def main():
    import sys
    if "--self-test" in sys.argv: return self_test()
    base.BridgeState.capture_runtime_frames=_patched
    return base.main()

if __name__=="__main__":
    raise SystemExit(main())
