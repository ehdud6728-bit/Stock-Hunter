#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import argparse, gzip, json, pickle, re
from pathlib import Path
from typing import Any, Dict, List, Tuple
import pandas as pd
import numpy as np

ADAPTER_ID="REAL_FULL_V23_MATERIALIZED_SOURCE_ADAPTER_R1"
ADAPTER_REVISION="REAL_FULL_V23_MATERIALIZED_SOURCE_ADAPTER_R1_1_FAIL_CLOSED_SCHEMA_AUDIT"

CODE_COLS=("code","종목코드","Code","ticker","Ticker","stock_code")
DATE_COLS=("signal_date","날짜","date","asof_date","asof","기준일")
CONTEXT_HINTS=(
    "검색식대표","검색패턴","검색식매칭","파동타점상태","수박상태명","수박최종상태",
    "저항구름상태","추천단계","단계상태","🚨손절가","파란점선기준가",
    "소파동박스방향","중파동박스방향"
)
KEY_HINT_RE=re.compile(r"(candidate|signal|hits|rank|final|search|formula)",re.I)

def _date_from_filename(p:Path)->str:
    s=p.name
    m=re.search(r"(20\d{2})[-_]?(\d{2})[-_]?(\d{2})",s)
    if m:return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return ""

def _to_df(obj:Any):
    if isinstance(obj,pd.DataFrame):return obj.copy()
    if isinstance(obj,list) and (not obj or all(isinstance(x,dict) for x in obj)):
        return pd.DataFrame(obj)
    if isinstance(obj,tuple) and obj and all(isinstance(x,dict) for x in obj):
        return pd.DataFrame(list(obj))
    return None

def _walk(obj:Any,path:str="root",depth:int=0):
    if depth>7:return
    d=_to_df(obj)
    if d is not None:
        yield path,d
        return
    if isinstance(obj,dict):
        for k,v in obj.items():
            yield from _walk(v,f"{path}.{k}",depth+1)
    elif isinstance(obj,(list,tuple)):
        for i,v in enumerate(obj[:50]):
            if isinstance(v,(dict,list,tuple,pd.DataFrame)):
                yield from _walk(v,f"{path}[{i}]",depth+1)

def _score(path:str,df:pd.DataFrame)->Tuple[int,Dict[str,int]]:
    cols=set(map(str,df.columns))
    code_ok=int(any(c in cols for c in CODE_COLS))
    context=sum(1 for c in CONTEXT_HINTS if c in cols)
    key_hint=int(bool(KEY_HINT_RE.search(path)))
    rank_hint=int(any(c in cols for c in ("rank","순위","TOP15순위","추천정렬점수","TOP15정렬점수")))
    score=code_ok*100 + min(context,12)*10 + key_hint*8 + rank_hint*5
    return score,{"code_ok":code_ok,"context_hits":context,"key_hint":key_hint,"rank_hint":rank_hint}

def _load_pickle(p:Path):
    with gzip.open(p,"rb") as f:
        return pickle.load(f)

def extract(materialized_dir:Path,output_dir:Path)->int:
    output_dir.mkdir(parents=True,exist_ok=True)
    audits=[]; rows=[]; selected_summary=[]
    files=sorted(materialized_dir.glob("date_*.pkl.gz"))
    for p in files:
        date=_date_from_filename(p)
        try:
            obj=_load_pickle(p)
        except Exception as e:
            audits.append({"file":p.name,"snapshot_date":date,"path":"","rows":-1,"cols":0,
                           "score":-1,"selected":0,"status":f"LOAD_ERROR:{type(e).__name__}:{e}"})
            continue
        candidates=[]
        for path,df in _walk(obj):
            sc,meta=_score(path,df)
            rec={"file":p.name,"snapshot_date":date,"path":path,"rows":len(df),"cols":len(df.columns),
                 "score":sc,"selected":0,"status":"DISCOVERED",**meta}
            audits.append(rec)
            if meta["code_ok"] and (meta["context_hits"]>=1 or meta["key_hint"]):
                candidates.append((sc,path,df,meta))
        if not candidates:
            selected_summary.append({"snapshot_date":date,"file":p.name,"status":"NO_CANDIDATE_TABLE",
                                     "selected_path":"","rows":0,"candidate_tables":0})
            continue
        candidates.sort(key=lambda x:(x[0],len(x[2])),reverse=True)
        best=candidates[0]
        # Fail closed only on a genuine score tie among different candidate tables.
        tied=[x for x in candidates if x[0]==best[0] and len(x[2])==len(best[2])]
        if len(tied)>1:
            selected_summary.append({"snapshot_date":date,"file":p.name,"status":"AMBIGUOUS_CANDIDATE_TABLE",
                                     "selected_path":"","rows":0,"candidate_tables":len(candidates)})
            continue
        sc,path,df,meta=best
        for a in audits:
            if a["file"]==p.name and a["path"]==path:
                a["selected"]=1
        selected_summary.append({"snapshot_date":date,"file":p.name,"status":"SELECTED",
                                 "selected_path":path,"rows":len(df),"candidate_tables":len(candidates)})
        d=df.reset_index(drop=True).copy()
        for i,r in d.iterrows():
            rec={"_materialized_file":p.name,"_snapshot_date":date,"_source_path":path,"_source_order":i+1}
            for k,v in r.to_dict().items():
                try:
                    if pd.isna(v):rec[str(k)]=None
                    elif isinstance(v,(str,int,float,bool)):rec[str(k)]=v
                    else:rec[str(k)]=str(v)
                except Exception:
                    rec[str(k)]=str(v)
            rows.append(rec)
    pd.DataFrame(audits).to_csv(output_dir/"materialized_schema_audit.csv",index=False,encoding="utf-8-sig")
    pd.DataFrame(selected_summary).to_csv(output_dir/"materialized_source_selection.csv",index=False,encoding="utf-8-sig")
    with (output_dir/"materialized_candidate_snapshots.jsonl").open("w",encoding="utf-8") as f:
        for r in rows:f.write(json.dumps(r,ensure_ascii=False,default=str)+"\n")
    selected=sum(1 for r in selected_summary if r["status"]=="SELECTED")
    ambiguous=sum(1 for r in selected_summary if r["status"]=="AMBIGUOUS_CANDIDATE_TABLE")
    missing=sum(1 for r in selected_summary if r["status"]=="NO_CANDIDATE_TABLE")
    report="\n".join([
        "🧩 [V23 MATERIALIZED SOURCE ADAPTER]",
        f"files={len(files)} · selected_dates={selected} · ambiguous={ambiguous} · no_candidate_table={missing} · rows={len(rows)}",
        "authority=research adapter only · source order preserved · no rerank",
        "fail-closed: same-score same-size candidate-table ambiguity is never guessed",
    ])
    (output_dir/"materialized_source_adapter_report.txt").write_text(report,encoding="utf-8")
    print(report)
    # Extraction itself is diagnostic. The downstream analyzer decides readiness.
    return 0

def self_test()->int:
    import tempfile
    with tempfile.TemporaryDirectory() as td:
        root=Path(td); mat=root/"mat"; out=root/"out"; mat.mkdir()
        obj={"payload":{"candidate_rows":[
            {"code":"005930","종목명":"A","현재가":100,"🚨손절가":95,"파동타점상태":"1차눌림","검색식대표":"P"},
            {"code":"000660","종목명":"B","현재가":200,"🚨손절가":190,"파동타점상태":"수렴","검색식대표":"Q"},
        ]},"aux":{"rows":[{"x":1}]}}
        with gzip.open(mat/"date_2026-09-04.pkl.gz","wb") as f:pickle.dump(obj,f)
        assert extract(mat,out)==0
        sel=pd.read_csv(out/"materialized_source_selection.csv")
        assert sel.iloc[0]["status"]=="SELECTED"
        lines=(out/"materialized_candidate_snapshots.jsonl").read_text(encoding="utf-8").splitlines()
        assert len(lines)==2
    print("REAL_FULL_V23_MATERIALIZED_SOURCE_ADAPTER_SELF_TEST PASS")
    return 0

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--materialized-dir",default="reports/v23_materialized")
    ap.add_argument("--output-dir",default="reports/real_full_validation_backtest/materialized_adapter")
    ap.add_argument("--self-test",action="store_true")
    a=ap.parse_args()
    return self_test() if a.self_test else extract(Path(a.materialized_dir),Path(a.output_dir))
if __name__=="__main__":raise SystemExit(main())
