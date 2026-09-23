#!/usr/bin/env python3
from __future__ import annotations
import argparse, json, math, re, hashlib
from pathlib import Path
import pandas as pd
import numpy as np

RESEARCH_ID="REAL_FULL_RESEARCH_OVERLAY_R1"
REVISION="R1_SIMPLE_FLOW_RESEARCH_EXPECTATION_20260923"

def code(v):
    s=re.sub(r"\D","",str(v or ""))
    return s[-6:].zfill(6) if s else ""

def num(v):
    try:
        x=float(v); return x if math.isfinite(x) else np.nan
    except: return np.nan

def phase(fr, col, asof):
    h=fr[fr["date"].le(asof)].copy()
    if len(h)<25 or col not in h:return "UNKNOWN"
    v=pd.to_numeric(h[col],errors="coerce")
    c=pd.to_numeric(h["close"],errors="coerce")
    curv=num(v.iloc[-1]); curc=num(c.iloc[-1])
    if not math.isfinite(curv) or not math.isfinite(curc) or curv<=0:return "UNKNOWN"
    cross=((c.shift(1)<v.shift(1))&(c>=v)).tail(20).any()
    if cross:return "RECLAIM_20"
    if curc<curv:return "BELOW"
    return "ABOVE_STABLE"

def load_price(c, asof):
    import FinanceDataReader as fdr
    start=(asof-pd.Timedelta(days=700)).strftime("%Y-%m-%d")
    end=(asof+pd.Timedelta(days=2)).strftime("%Y-%m-%d")
    q=fdr.DataReader(c,start,end)
    if q is None or q.empty:return pd.DataFrame()
    q=q.copy()
    q["date"]=pd.to_datetime(q.index,errors="coerce").normalize()
    ren={k:str(k).lower() for k in q.columns if str(k).lower() in {"open","high","low","close","volume"}}
    q=q.rename(columns=ren)
    for x in ["open","high","low","close","volume"]:
        if x in q:q[x]=pd.to_numeric(q[x],errors="coerce")
    q["amount"]=q["close"]*q["volume"] # research-only proxy
    for n in [5,20,60,112,224,448]:
        q[f"ma{n}"]=q["close"].rolling(n,min_periods=n).mean()
    q["ret1"]=q["close"].pct_change()*100
    q["range_pct"]=(q["high"]/q["low"]-1)*100
    return q.reset_index(drop=True)

def infer(fr, asof):
    h=fr[fr.date.le(asof)].copy()
    if h.empty or not h.date.eq(asof).any():return {}
    cur=h[h.date.eq(asof)].iloc[-1]
    entry=num(cur.close)
    p5=h.iloc[:-1].tail(5); p20=h.iloc[:-1].tail(20); p40=h.iloc[:-1].tail(40); p60=h.iloc[:-1].tail(60)
    out={}
    for n in [112,224,448]:
        mv=num(cur.get(f"ma{n}")); out[f"close_vs_ma{n}_pct"]=(entry/mv-1)*100 if mv>0 else np.nan
        out[f"ma{n}_phase"]=phase(h,f"ma{n}",asof)
    vals=[num(cur.get(f"ma{n}")) for n in [20,60,112,224] if math.isfinite(num(cur.get(f"ma{n}"))) and num(cur.get(f"ma{n}"))>0]
    width=(max(vals)/min(vals)-1)*100 if len(vals)>=3 else np.nan
    out["ma_compression"]=bool(math.isfinite(width) and width<=12)
    med60=pd.to_numeric(p60.amount,errors="coerce").median()
    med20=pd.to_numeric(p20.amount,errors="coerce").median()
    med5=pd.to_numeric(p5.amount,errors="coerce").median()
    r20=pd.to_numeric(p20.amount,errors="coerce")/med60 if med60 and math.isfinite(med60) else pd.Series(dtype=float)
    out["gradual_accumulation"]=bool(len(r20) and ((r20>=1.2)&(r20<2.0)).sum()>=3 and (r20>=2).sum()<=3)
    ratio=med5/med20 if med20 and math.isfinite(med20) else np.nan
    out["liquidity_retained"]=bool(math.isfinite(ratio) and .35<=ratio<=1.2)
    out["supply_drying"]=bool(math.isfinite(ratio) and ratio<.8)
    # causal wave/pullback
    out["wave1_pct"]=np.nan; out["pullback_pct"]=np.nan
    if len(p40)>=10:
        li=pd.to_numeric(p40.low,errors="coerce").idxmin()
        after=p40.loc[li:]
        if len(after):
            hi=pd.to_numeric(after.high,errors="coerce").idxmax()
            lo=num(p40.loc[li,"low"]); high=num(p40.loc[hi,"high"])
            if lo>0 and high>=lo:
                out["wave1_pct"]=(high/lo-1)*100
                out["pullback_pct"]=(entry/high-1)*100
    out["strong_wave1"]=bool(math.isfinite(num(out["wave1_pct"])) and out["wave1_pct"]>=15)
    out["shallow_pullback"]=bool(math.isfinite(num(out["pullback_pct"])) and -10<=out["pullback_pct"]<=0)
    return out

def match_registry(reg, state, p112,p224,p448):
    hits=[]
    if reg is None or reg.empty:return hits
    for _,r in reg.iterrows():
        axis=str(r.get("axis",""))
        ok=str(r.get("wm_modern_state",""))==str(state)
        if axis=="MODERN_STATE_X_MA224_X_MA448":
            ok=ok and str(r.get("ma224_phase",""))==p224 and str(r.get("ma448_phase",""))==p448
        elif axis=="MODERN_STATE_X_ALL_LONGMA_PHASES":
            ok=ok and str(r.get("ma112_phase",""))==p112 and str(r.get("ma224_phase",""))==p224 and str(r.get("ma448_phase",""))==p448
        else: continue
        if ok:hits.append(r.to_dict())
    return hits

def modern_state_from_source(r):
    for k in ["수박최종상태","wm_modern_state","Watermelon_State","watermelon_state","blue_line_state"]:
        v=str(r.get(k,"") or "").strip()
        if v and v.lower() not in {"nan","none"}: return v
    return "미확인"

def overlay_level(f, hits):
    good=0; risk=0
    if f.get("ma224_phase")=="BELOW":good+=1
    if f.get("ma448_phase")=="RECLAIM_20":good+=1
    if f.get("gradual_accumulation"):good+=1
    if f.get("liquidity_retained"):good+=1
    if f.get("ma_compression"):good+=1
    if f.get("ma224_phase")=="ABOVE_STABLE":risk+=1
    for h in hits:
        n=num(h.get("events")); sr=num(h.get("success_rate")); med=num(h.get("d20_close_median"))
        if n>=5 and math.isfinite(sr):
            if sr>=.55 and med>0:good+=1
            if sr<=.35 and med<0:risk+=1
    # Research display only, not a score/rank/gate.
    if good>=3 and risk==0:return "🟢 연구흐름 좋음"
    if risk>=2:return "🔴 연구상 주의"
    return "🟡 연구관찰"

def flow_lines(f,state):
    xs=[]
    p224=f.get("ma224_phase","UNKNOWN"); p448=f.get("ma448_phase","UNKNOWN")
    if p224=="BELOW": xs.append("224일선 아래/바닥권에서 움직임 관찰")
    elif p224=="RECLAIM_20": xs.append("224일선을 최근 회복하는 과정")
    else: xs.append("224일선 위에서 진행 중")
    if f.get("strong_wave1") and f.get("shallow_pullback"): xs.append("1차 파동 뒤 얕은 눌림 형태")
    elif f.get("strong_wave1"): xs.append("1차 파동 흔적은 있으나 눌림 깊이 확인 필요")
    if f.get("gradual_accumulation"):xs.append("거래 유입이 한 번 폭발보다 점진적으로 들어온 편")
    if f.get("liquidity_retained"):xs.append("눌림에서도 거래 유동성이 완전히 꺼지지 않음")
    if state!="미확인":xs.append(f"수박 상태: {state}")
    return xs[:3]

def research_line(f,hits):
    if hits:
        hs=sorted(hits,key=lambda x:num(x.get("events")),reverse=True)
        h=hs[0]; n=int(num(h.get("events")) or 0); sr=num(h.get("success_rate")); med=num(h.get("d20_close_median"))
        if math.isfinite(sr) and math.isfinite(med):
            return f"동일 frozen 구조는 Discovery n={n}, 성공 {sr*100:.0f}%, D20 중앙값 {med:+.1f}%였음(참고용)"
    if f.get("ma224_phase")=="BELOW":
        return "연구에서는 MA224 아래 초기 구조가 상대적으로 더 나았고, 완전 돌파 후 구조는 혼재했음"
    return "현재 구조는 기존 연구에서 우열이 아직 선명하지 않아 추가 관찰 필요"

def expectation(f,hits):
    if f.get("ma224_phase")=="BELOW" and f.get("liquidity_retained"):
        return "급등 여부보다 눌림에서 거래가 유지되고 다시 재시동하는지 확인"
    if f.get("ma224_phase")=="RECLAIM_20" and f.get("ma448_phase")=="ABOVE_STABLE":
        return "장기선 회복 뒤 바로 밀리는 Giveback이 나오는지 우선 확인"
    if f.get("ma448_phase")=="RECLAIM_20":
        return "MA448 회복을 지키면서 MA224 아래에서 힘을 모으는지 확인"
    return "가격이 눌릴 때 거래량이 줄고, 이후 거래 증가와 함께 재상승하는지 확인"

def run(a):
    src=pd.read_csv(a.source,dtype=str,low_memory=False)
    reg=pd.read_csv(a.registry,dtype=str,low_memory=False) if Path(a.registry).exists() else pd.DataFrame()
    datecol=next((c for c in ["signal_date","date","날짜","snapshot_date"] if c in src.columns),None)
    codecol=next((c for c in ["code","종목코드","Code"] if c in src.columns),None)
    namecol=next((c for c in ["name","종목명","Name"] if c in src.columns),None)
    rankcol=next((c for c in ["rank","origin_rank","순위","cross_top15_rank"] if c in src.columns),None)
    if not codecol:raise SystemExit("SOURCE_CODE_COLUMN_MISSING")
    rows=[]; cards=[]
    for ix,r in src.head(15).iterrows():
        c=code(r.get(codecol)); nm=str(r.get(namecol,"")) if namecol else c
        sd=pd.to_datetime(r.get(datecol),errors="coerce").normalize() if datecol else pd.Timestamp.now(tz="Asia/Seoul").tz_localize(None).normalize()
        fr=load_price(c,sd)
        f=infer(fr,sd) if not fr.empty else {}
        state=modern_state_from_source(r)
        hits=match_registry(reg,state,f.get("ma112_phase","UNKNOWN"),f.get("ma224_phase","UNKNOWN"),f.get("ma448_phase","UNKNOWN"))
        level=overlay_level(f,hits)
        flow=flow_lines(f,state)
        res=research_line(f,hits)
        exp=expectation(f,hits)
        rank=str(r.get(rankcol,ix+1)) if rankcol else str(ix+1)
        cards += [f"{rank}. {nm} ({c})  {level}",
                  "   지금까지: " + " / ".join(flow) if flow else "   지금까지: 구조 계산 자료 부족",
                  "   연구: " + res,
                  "   예상: " + exp,
                  ""]
        rows.append({"rank":rank,"code":c,"name":nm,"research_level":level,
                     "flow":" / ".join(flow),"research_reference":res,"expectation":exp,
                     "wm_state":state,**f,
                     "frozen_signature_matches":"|".join([str(x.get("signature_id","")) for x in hits]),
                     "research_only":1,"production_changed":0})
    out=Path(a.output_dir);out.mkdir(parents=True,exist_ok=True)
    pd.DataFrame(rows).to_csv(out/"real_full_research_overlay.csv",index=False,encoding="utf-8-sig")
    header=[
      "🔬 REAL_FULL Research Overlay R1 · RESEARCH_ONLY",
      "※ Production 후보/점수/순위 변경 없음",
      "※ 🟢🟡🔴은 매수추천이 아니라 연구 흐름을 쉽게 보기 위한 관찰표시",
      ""
    ]
    (out/"real_full_research_overlay.txt").write_text("\n".join(header+cards),encoding="utf-8")
    meta={"research_id":RESEARCH_ID,"revision":REVISION,"rows":len(rows),
          "production_search_changed":False,"production_score_changed":False,
          "production_rank_changed":False,"production_order_changed":False,
          "same_sample_retuning":False,"research_only":True,
          "amount_note":"FDR close*volume proxy is descriptive only; not authoritative KRX trading value"}
    (out/"real_full_research_overlay_meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")
    print((out/"real_full_research_overlay.txt").read_text())
    return 0

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--source",required=True)
    ap.add_argument("--registry",required=True)
    ap.add_argument("--output-dir",default="reports/real_full_research_overlay_r1")
    a=ap.parse_args();raise SystemExit(run(a))
if __name__=="__main__":main()
