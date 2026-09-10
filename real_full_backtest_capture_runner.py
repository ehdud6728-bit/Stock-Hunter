#!/usr/bin/env python3
from __future__ import annotations
import argparse, inspect, json, os, runpy, sys, threading, traceback
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
import pandas as pd

CAPTURE_ID="REAL_FULL_HISTORICAL_BUILDER_CAPTURE_R1"
CAPTURE_REVISION="REAL_FULL_HISTORICAL_BUILDER_CAPTURE_R1_1_PROFILE_RETURN"
DATE_KEYS=("signal_date","날짜","date","asof_date","asof","기준일","target_date","evaluation_date")

class ForcedExit(BaseException):
    def __init__(self,code=0): self.code=int(code or 0)

def date_text(v):
    try:
        t=pd.to_datetime(v,errors="coerce")
        if pd.notna(t): return pd.Timestamp(t).date().isoformat()
    except:pass
    return ""

def to_df(v):
    if isinstance(v,pd.DataFrame): return v.copy()
    if isinstance(v,list) and all(isinstance(x,dict) for x in v): return pd.DataFrame(v)
    if isinstance(v,tuple):
        for x in v:
            d=to_df(x)
            if d is not None:return d
    return None

def detect_date(df,frame):
    if df is not None and not df.empty:
        for k in DATE_KEYS:
            if k in df.columns:
                for v in df[k].tolist()[:5]:
                    d=date_text(v)
                    if d:return d
    f=frame
    depth=0
    while f is not None and depth<8:
        for k in DATE_KEYS:
            if k in f.f_locals:
                d=date_text(f.f_locals.get(k))
                if d:return d
        for k,v in list(f.f_locals.items()):
            lk=str(k).lower()
            if ("date" in lk or "asof" in lk or "기준" in str(k)) and not isinstance(v,(pd.DataFrame,list,dict,tuple,set)):
                d=date_text(v)
                if d:return d
        f=f.f_back; depth+=1
    return ""

class Capture:
    def __init__(self,out_dir):
        self.out=Path(out_dir); self.out.mkdir(parents=True,exist_ok=True)
        self.calls=[]; self.rows=[]; self.call_id=0; self.busy=False
    def profile(self,frame,event,arg):
        if self.busy or event!="return": return
        if "build_and_sort_candidates" not in frame.f_code.co_name: return
        self.busy=True
        try:
            df=to_df(arg)
            if df is None:return
            self.call_id+=1
            d=detect_date(df,frame)
            cols=[str(c) for c in df.columns]
            code_col=next((c for c in ("code","종목코드","Code") if c in df.columns),None)
            call={
                "capture_id":CAPTURE_ID,"capture_revision":CAPTURE_REVISION,"call_id":self.call_id,
                "snapshot_date":d,"rows":int(len(df)),"columns":len(cols),
                "code_ready":int(code_col is not None),"function_name":frame.f_code.co_name,
                "captured_at_kst":datetime.now(ZoneInfo("Asia/Seoul")).isoformat(timespec="seconds"),
            }
            self.calls.append(call)
            if len(df):
                for order,(_,r) in enumerate(df.reset_index(drop=True).iterrows(),1):
                    rec={"_capture_call_id":self.call_id,"_snapshot_date":d,"_source_order":order}
                    for k,v in r.to_dict().items():
                        try:
                            if pd.isna(v): rec[str(k)]=None
                            elif isinstance(v,(str,int,float,bool)): rec[str(k)]=v
                            else: rec[str(k)]=str(v)
                        except: rec[str(k)]=str(v)
                    self.rows.append(rec)
        except Exception:
            traceback.print_exc()
        finally:
            self.busy=False
    def finish(self,exit_code):
        pd.DataFrame(self.calls).to_csv(self.out/"capture_manifest.csv",index=False,encoding="utf-8-sig")
        with (self.out/"candidate_snapshots.jsonl").open("w",encoding="utf-8") as f:
            for r in self.rows:f.write(json.dumps(r,ensure_ascii=False,default=str)+"\n")
        resolved=sum(1 for c in self.calls if c["snapshot_date"])
        rep="\n".join([
            "🧪 [REAL_FULL HISTORICAL BUILDER CAPTURE]",
            f"calls={len(self.calls)} · resolved_dates={resolved} · rows={len(self.rows)} · exit={exit_code}",
            "source=existing build_and_sort_candidates return order · no rerank/no strategy mutation",
        ])
        (self.out/"capture_report.txt").write_text(rep,encoding="utf-8")
        print(rep)

def self_test():
    import tempfile, subprocess
    with tempfile.TemporaryDirectory() as td:
        td=Path(td); fake=td/"fake.py"
        fake.write_text(
            "import pandas as pd\n"
            "def build_and_sort_candidates(rows,asof_date=None): return pd.DataFrame(rows)\n"
            "for d in ['2026-08-28','2026-09-04']:\n"
            " x=build_and_sort_candidates([{'code':'005930','종목명':'A','현재가':100,'날짜':d}],asof_date=d)\n",
            encoding="utf-8")
        r=subprocess.run([sys.executable,__file__,"--script",str(fake),"--output-dir",str(td/"o")],capture_output=True,text=True)
        assert r.returncode==0,(r.stdout,r.stderr)
        m=pd.read_csv(td/"o/capture_manifest.csv")
        assert len(m)==2 and m["snapshot_date"].notna().all()
        print("REAL_FULL_HISTORICAL_BUILDER_CAPTURE_SELF_TEST PASS")
        return 0

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--script")
    ap.add_argument("--output-dir",default="reports/real_full_validation_backtest")
    ap.add_argument("--self-test",action="store_true")
    a,unknown=ap.parse_known_args()
    if a.self_test:return self_test()
    if not a.script or not Path(a.script).exists(): raise SystemExit("capture script missing")
    cap=Capture(a.output_dir)
    old_prof=sys.getprofile(); old_thread=None
    old_exit=os._exit
    def fake_exit(code=0): raise ForcedExit(code)
    rc=0; old_argv=sys.argv[:]
    try:
        os._exit=fake_exit
        sys.setprofile(cap.profile); threading.setprofile(cap.profile)
        sys.argv=[a.script]+unknown
        runpy.run_path(a.script,run_name="__main__")
    except ForcedExit as e: rc=e.code
    except SystemExit as e:
        try:rc=int(e.code or 0)
        except:rc=1
    except BaseException:
        rc=1; traceback.print_exc()
    finally:
        sys.setprofile(old_prof); threading.setprofile(None); os._exit=old_exit; sys.argv=old_argv
        cap.finish(rc)
    return rc
if __name__=="__main__": raise SystemExit(main())
