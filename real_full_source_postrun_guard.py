#!/usr/bin/env python3
from __future__ import annotations
import argparse, json, re
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
import numpy as np
import pandas as pd

GUARD_ID="REAL_FULL_SOURCE_POSTRUN_GUARD_R1"
GUARD_REVISION="REAL_FULL_SOURCE_POSTRUN_GUARD_R1_3_DURABLE_TODAY_CANDIDATES"
VALID={"CAPTURED_NONEMPTY","CAPTURED_ZERO"}
EMPTY_MARKERS=("종목 리스트 로드 완전 실패","V73 SAFE STOP용 빈 DataFrame 반환")
STUB_MARKERS=("STOCKHUNTER_PYKRX_FORCE_STUB=1","RuntimeError: STOCKHUNTER_PYKRX_FORCE_STUB=1")
ZERO_MARKERS=("today_candidates.json 후보 0개","1단계 TOP15 후보가 없어","ai_candidates.empty after build_and_sort_candidates")
LEADER_MARKER="Leader-Scanner] 국내 대장주 스캔 우회 중"

CODE=("종목코드","code","Code","ticker","Ticker","stock_code")
NAME=("종목명","name","Name","stock_name")
PRICE=("현재가","종가","Close","close","price","Price","entry_price")
SCORE=("안전점수","safe_score","N점수","n_score","최종점수","총점","점수","score","Score","total_score")
PATTERN_RX=re.compile(r"(pattern|패턴|수박|blue|돌반지|삼각|구조|저항|정제|stage|단계|signal|신호|gate|td_label)",re.I)

def now(): return datetime.now(ZoneInfo("Asia/Seoul"))
def readj(p):
    try: return json.loads(p.read_text(encoding="utf-8"))
    except Exception: return {}
def writej(p,d):
    p.parent.mkdir(parents=True,exist_ok=True)
    p.write_text(json.dumps(d,ensure_ascii=False,indent=2),encoding="utf-8")
def code(v):
    s=re.sub(r"\D","",str(v or ""))
    return s[-6:].zfill(6) if s else ""
def pick(d,keys):
    for k in keys:
        if k in d:return d.get(k)
    return None
def text(v):
    if v is None:return ""
    s=str(v).strip()
    return "" if s.lower() in ("","nan","none","nat") else s

def extract(obj):
    if isinstance(obj,list) and all(isinstance(x,dict) for x in obj): return obj
    if isinstance(obj,dict):
        for k in ("candidates","today_candidates","ai_candidates","items","rows","data","results","stocks","top15"):
            if k in obj:
                x=extract(obj[k])
                if x is not None:return x
        if any(k in obj for k in CODE): return [obj]
        for v in obj.values():
            if isinstance(v,(list,dict)):
                x=extract(v)
                if x is not None:return x
    return None

def find_candidates(explicit):
    candidates=[]
    for s in (explicit,"today_candidates.json","reports/today_candidates.json","data/today_candidates.json","output/today_candidates.json"):
        if s and Path(s) not in candidates:candidates.append(Path(s))
    for p in Path(".").glob("**/today_candidates.json"):
        if ".cache" not in str(p) and p not in candidates:candidates.append(p)
    errors=[]
    for p in candidates:
        if not p.exists():continue
        try:
            rows=extract(json.loads(p.read_text(encoding="utf-8")))
            if rows is not None:return p,rows,errors
            errors.append(f"{p}:unsupported_schema")
        except Exception as e:
            errors.append(f"{p}:{type(e).__name__}:{e}")
    return None,None,errors

def canonical(rows,signal_date,slot):
    out=[]; seen=set()
    for raw in rows:
        c=code(pick(raw,CODE))
        if not c or c in seen:continue
        seen.add(c)
        price=pd.to_numeric(pd.Series([pick(raw,PRICE)]),errors="coerce").iloc[0]
        scol=next((k for k in SCORE if k in raw),"")
        score=pd.to_numeric(pd.Series([raw.get(scol) if scol else np.nan]),errors="coerce").iloc[0]
        pcols=[str(k) for k in raw if PATTERN_RX.search(str(k))]
        parts=[]; ev={}
        for k in pcols[:32]:
            v=text(raw.get(k))
            if v:
                ev[k]=raw.get(k)
                if v not in parts:parts.append(v)
        out.append({
            "signal_date":signal_date,"rank":len(out)+1,"code":c,
            "name":text(pick(raw,NAME)),
            "snapshot_price":float(price) if pd.notna(price) and float(price)>0 else np.nan,
            "pattern_combo":" | ".join(parts[:10]) if parts else "UNCLASSIFIED",
            "overlap":len(parts),"score":float(score) if pd.notna(score) else np.nan,
            "score_bucket":"SOURCE_NATIVE","ai_pick_label":"NOT_AVAILABLE",
            "evidence":json.dumps(ev,ensure_ascii=False,sort_keys=True,default=str),
            "bridge_capture_slot":slot,"bridge_source_variable":"today_candidates.json",
            "bridge_rank_semantics":"REAL_FULL_TODAY_CANDIDATES_DURABLE_FILE_ORDER",
            "bridge_score_source_col":scol,"bridge_pattern_source_cols":"|".join(pcols[:32]),
            "bridge_source_rows":len(rows),
        })
        if len(out)>=15:break
    return pd.DataFrame(out)

def run(a):
    log=Path(a.run_log).read_text(encoding="utf-8",errors="ignore") if Path(a.run_log).exists() else ""
    meta_p=Path(a.meta); source_p=Path(a.source); full_p=Path(a.full_source)
    empty=any(x in log for x in EMPTY_MARKERS)
    stub=any(x in log for x in STUB_MARKERS)
    zero_log=any(x in log for x in ZERO_MARKERS)
    leader=LEADER_MARKER in log
    sig=a.signal_date if re.fullmatch(r"\d{4}-\d{2}-\d{2}",a.signal_date or "") else now().date().isoformat()
    p,rows,errs=find_candidates(a.candidate_json)
    meta=readj(meta_p) if meta_p.exists() else {}
    prior=str(meta.get("capture_status") or "")
    meta.update({
        "postrun_guard_id":GUARD_ID,"postrun_guard_revision":GUARD_REVISION,
        "signal_date":meta.get("signal_date") or sig,"capture_slot":meta.get("capture_slot") or a.capture_slot,
        "runner_rc":int(a.runner_rc),"empty_universe_detected":int(empty),
        "forced_stub_detected_in_main_log":int(stub),"explicit_zero_marker_detected":int(zero_log),
        "leader_scanner_fallback_detected":int(leader),
        "source_quality":"DEGRADED_LEADER_SCANNER_FALLBACK" if leader else "READY",
        "durable_candidate_json":str(p or ""),"durable_candidate_rows":len(rows) if rows is not None else None,
        "durable_candidate_parse_errors":errs[:10],
        "selection_logic_changed":0,"score_rank_changed":0,"order_logic_changed":0,
    })
    rc=0
    if empty:
        status="INVALID_EMPTY_UNIVERSE"; rc=73; reason="universe load failed"
    elif stub:
        status="INVALID_FORCED_STUB"; rc=79; reason="forced pykrx stub still active"
    elif int(a.runner_rc)!=0:
        status="RUNNER_NONZERO"; rc=74; reason=f"runner rc={a.runner_rc}"
    elif prior in VALID:
        status=prior; reason=f"existing bridge authority retained: {prior}"
    elif rows is not None:
        if len(rows)==0:
            status="CAPTURED_ZERO"; reason="recovered empty today_candidates.json"
            meta.update({"selected_handoff_rows":0,"canonical_top_rows":0,"zero_event_interpretable":1,
                         "capture_source_variable":"today_candidates.json"})
        else:
            df=canonical(rows,sig,a.capture_slot)
            if df.empty:
                status="INVALID_DURABLE_CANDIDATES_NO_TICKER"; rc=80; reason="nonempty json cannot canonicalize ticker"
            else:
                status="CAPTURED_NONEMPTY"; reason=f"recovered {len(rows)} durable candidates"
                df.to_csv(source_p,index=False,encoding="utf-8-sig")
                pd.DataFrame(rows).to_csv(full_p,index=False,encoding="utf-8-sig")
                meta.update({"selected_handoff_rows":len(rows),"canonical_top_rows":len(df),"zero_event_interpretable":0,
                             "capture_source_variable":"today_candidates.json",
                             "rank_semantics":"REAL_FULL_TODAY_CANDIDATES_DURABLE_FILE_ORDER"})
    elif zero_log:
        status="CAPTURED_ZERO"; reason="recovered zero from explicit main-log marker"
        meta.update({"selected_handoff_rows":0,"canonical_top_rows":0,"zero_event_interpretable":1,
                     "capture_source_variable":"explicit_zero_log"})
    else:
        status="RUNNER_EXIT_WITHOUT_RECOVERABLE_SOURCE"; rc=75
        reason="no bridge meta, durable today_candidates.json, or explicit zero marker"

    meta["capture_status"]=status
    meta["trust_eligible"]=int(rc==0 and status in VALID)
    meta["postrun_guard_pass"]=int(rc==0)
    meta["postrun_guard_reason"]=reason
    if rc!=0 or status=="CAPTURED_ZERO":
        for q in (source_p,full_p):
            try:q.unlink()
            except FileNotFoundError:pass
    writej(meta_p,meta)
    g={"guard_id":GUARD_ID,"status":status,"pass":int(rc==0),"trust_eligible":meta["trust_eligible"],
       "source_quality":meta["source_quality"],"reason":reason,"durable_candidate_rows":meta["durable_candidate_rows"],
       "leader_scanner_fallback_detected":int(leader),"signal_date":sig}
    writej(Path(a.guard_output),g)
    print("REAL_FULL_SOURCE_POSTRUN_GUARD",f"status={status}",f"pass={int(rc==0)}",
          f"trust_eligible={meta['trust_eligible']}",f"quality={meta['source_quality']}",
          f"durable_rows={meta['durable_candidate_rows']}",f"explicit_zero={int(zero_log)}")
    print("reason=",reason)
    return rc

def self_test():
    import tempfile
    class A:pass
    with tempfile.TemporaryDirectory() as td:
        r=Path(td); a=A()
        a.run_log=str(r/"run.log"); a.meta=str(r/"meta.json"); a.guard_output=str(r/"guard.json")
        a.source=str(r/"source.csv"); a.full_source=str(r/"full.csv"); a.candidate_json=str(r/"today_candidates.json")
        a.capture_slot="TEST"; a.signal_date="2026-09-10"; a.runner_rc=0
        Path(a.run_log).write_text("normal universe\n",encoding="utf-8")
        Path(a.candidate_json).write_text(json.dumps([{"종목코드":"005930","종목명":"삼성전자","현재가":70000,"점수":800,"검색패턴":"BLUE"}],ensure_ascii=False),encoding="utf-8")
        assert run(a)==0 and readj(Path(a.meta))["capture_status"]=="CAPTURED_NONEMPTY"
        for q in (Path(a.meta),Path(a.source),Path(a.full_source),Path(a.guard_output)):
            try:q.unlink()
            except:pass
        Path(a.candidate_json).write_text("[]",encoding="utf-8")
        Path(a.run_log).write_text("today_candidates.json 후보 0개\n",encoding="utf-8")
        assert run(a)==0
        d=readj(Path(a.meta)); assert d["capture_status"]=="CAPTURED_ZERO" and d["zero_event_interpretable"]==1
        Path(a.meta).unlink()
        Path(a.run_log).write_text("Leader-Scanner] 국내 대장주 스캔 우회 중: HTTP Error 404: Not Found\ntoday_candidates.json 후보 0개\n",encoding="utf-8")
        assert run(a)==0
        assert readj(Path(a.meta))["source_quality"]=="DEGRADED_LEADER_SCANNER_FALLBACK"
    print("REAL_FULL_DURABLE_TODAY_CANDIDATES_SELF_TEST PASS")
    return 0

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--run-log",default="reports/real_full_main_run.log")
    ap.add_argument("--meta",default="reports/real_full_trust_source_meta.json")
    ap.add_argument("--guard-output",default="reports/real_full_source_postrun_guard.json")
    ap.add_argument("--source",default="reports/real_full_trust_source.csv")
    ap.add_argument("--full-source",default="reports/real_full_current_universe.csv")
    ap.add_argument("--candidate-json",default="today_candidates.json")
    ap.add_argument("--capture-slot",default="UNKNOWN")
    ap.add_argument("--signal-date",default="")
    ap.add_argument("--runner-rc",type=int,default=0)
    ap.add_argument("--self-test",action="store_true")
    a=ap.parse_args()
    return self_test() if a.self_test else run(a)
if __name__=="__main__": raise SystemExit(main())
