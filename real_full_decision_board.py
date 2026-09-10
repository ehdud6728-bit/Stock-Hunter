#!/usr/bin/env python3
from __future__ import annotations
import argparse, json, math
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
import numpy as np
import pandas as pd
from real_full_decision_engine import (
    POLICY_ID,POLICY_REVISION,TERMINAL,code,text,num,price_fmt,
    extract_row,make_event,evaluate,condition_summary,mature_from_ready
)
from real_full_trust_audit import load_price_frames

TRACKER_ID="REAL_FULL_CANDIDATE_TRACKER_R1"
TRACKER_REVISION="REAL_FULL_CANDIDATE_TRACKER_R1_1_READY_NEAR_AUTO_TRACK"

def readcsv(p,**kw):
    p=Path(p)
    if not p.exists(): return pd.DataFrame()
    try:return pd.read_csv(p,**kw)
    except pd.errors.EmptyDataError:return pd.DataFrame()

def kstnow(): return datetime.now(ZoneInfo("Asia/Seoul")).isoformat(timespec="seconds")

def listing_map(path):
    d=readcsv(path,dtype={"Code":str,"code":str})
    out={}
    if d.empty:return out
    cc="Code" if "Code" in d.columns else "code" if "code" in d.columns else None
    pc="Close" if "Close" in d.columns else "close" if "close" in d.columns else None
    if not cc or not pc:return out
    for _,r in d.iterrows():
        c=code(r.get(cc)); p=num(r.get(pc))
        if c and math.isfinite(p) and p>0: out[c]=p
    return out

def current_raw_map(raw):
    if raw.empty:return {}
    d=raw.copy()
    if "code" not in d.columns:
        d["code"]=d.get("종목코드","")
    d["code"]=d["code"].map(code)
    return {r["code"]:r for _,r in d.iterrows() if r["code"]}

def scorecard(events,group):
    if events.empty or group not in events.columns:return pd.DataFrame()
    rows=[]
    for k,g in events.groupby(group,dropna=False):
        rec={"tracker_id":TRACKER_ID,"group_type":group,"group":str(k),"events":len(g)}
        rec["ready"]=int(g["state"].eq("READY").sum())
        rec["ready_rate"]=float(g["state"].eq("READY").mean()) if len(g) else np.nan
        rec["expired_rate"]=float(g["state"].eq("EXPIRED").mean()) if len(g) else np.nan
        rec["invalidated_rate"]=float(g["state"].eq("INVALIDATED").mean()) if len(g) else np.nan
        ready=g[g["state"].eq("READY")].copy()
        ages=pd.to_numeric(ready.get("wait_age"),errors="coerce").dropna()
        rec["median_bars_to_ready"]=float(ages.median()) if len(ages) else np.nan
        for h in (1,3,5,10):
            comp=pd.to_numeric(ready.get(f"ready_d{h}_complete"),errors="coerce").fillna(0).eq(1)
            z=ready[comp]
            rec[f"ready_d{h}_mature"]=len(z)
            for m in ("close_ret_pct","mfe_pct","mae_pct"):
                s=pd.to_numeric(z.get(f"ready_d{h}_{m}"),errors="coerce").dropna()
                rec[f"ready_d{h}_{m}_median"]=float(s.median()) if len(s) else np.nan
            for m in ("hit_plus3","hit_plus5"):
                s=pd.to_numeric(z.get(f"ready_d{h}_{m}"),errors="coerce").dropna()
                rec[f"ready_d{h}_{m}_rate"]=float(s.mean()) if len(s) else np.nan
        rows.append(rec)
    return pd.DataFrame(rows)

def brief(alerts,events,current_date):
    alerts=alerts.copy()
    ready=alerts[alerts["state"].eq("READY")] if not alerts.empty else pd.DataFrame()
    near=alerts[alerts["state"].eq("NEAR_READY")] if not alerts.empty else pd.DataFrame()
    active=events[events["state"].isin(["WAIT","NEAR_READY"])] if not events.empty else pd.DataFrame()
    today_term=events[events["terminal_date"].astype(str).eq(current_date)] if not events.empty and "terminal_date" in events else pd.DataFrame()
    lines=[
        "🧭 [REAL_FULL 실전 추적 R1]",
        f"📅 {current_date} | 🟢READY {len(ready)} | 🟡NEAR {len(near)} | 내부추적 {len(active)} | 오늘종료 {len(today_term)}",
        "원칙: WAIT는 시스템이 내부 추적 · NEAR_READY=1조건 남음 · READY만 수동 실전검토 추천 · 자동주문 0",
    ]
    if len(ready):
        lines+=["","🟢 [READY · 실전 검토 추천]"]
        for _,r in ready.sort_values(["origin_rank","code"]).head(5).iterrows():
            lines.append(f"- {r['name']}({r['code']}) | 현재 {price_fmt(r['last_price'])} | 손절 {price_fmt(r['frozen_stop'])}")
            lines.append(f"  완료: {r.get('met_conditions','-')} | READY일 {r.get('ready_date','-')}")
            lines.append("  대응: 오늘 차트/시장/수급 재검토 후 사용자 수동 결정")
    if len(near):
        lines+=["","🟡 [NEAR_READY · 진입 근접]"]
        for _,r in near.sort_values(["days_left","origin_rank"]).head(7).iterrows():
            lines.append(f"- {r['name']}({r['code']}) | 1조건 남음: {r.get('missing_conditions','-')}")
            lines.append(f"  현재 {price_fmt(r['last_price'])} | 손절 {price_fmt(r['frozen_stop'])} | 남은기한 D+{int(r.get('days_left',0))}")
    if not len(ready) and not len(near):
        lines+=["","- 오늘 사용자에게 볼 READY/NEAR_READY 없음. WAIT 후보는 내부에서 계속 추적합니다."]
    lines+=["","📊 판정 검증은 event/transition/daily ledger에 누적하고 D+1/3/5/10 READY 이후 성과로 비교합니다."]
    return "\n".join(lines)

def self_test():
    import tempfile
    raw=pd.DataFrame([{
        "code":"005930","종목명":"A","현재가":100,"🚨손절가":95,"파란점선기준가":101,"파란점선상태":"아래",
        "파동최적하단":96,"파동최적상단":104,"파동타점상태":"눌림대기형",
        "5일재안착":False,"수박정제_vol_ok":False,"수박정제_candle_ok":True
    }])
    e=make_event(raw.iloc[0],"2026-09-10",1)
    assert e["origin_action"]=="WAIT_RECLAIM" and e["state"]=="WAIT"
    r2=raw.iloc[0].copy(); r2["현재가"]=102; r2["파란점선상태"]="위안착"; r2["5일재안착"]=True; r2["수박정제_vol_ok"]=False
    x=extract_row(r2); e=evaluate(e,r2,x["current_price"],1,"2026-09-11")
    assert e["state"]=="NEAR_READY" and "거래량보강" in e["missing_conditions"]
    r3=r2.copy(); r3["수박정제_vol_ok"]=True
    x=extract_row(r3); e=evaluate(e,r3,x["current_price"],2,"2026-09-14")
    assert e["state"]=="READY" and e["manual_review_required"]==1
    print("REAL_FULL_CANDIDATE_TRACKER_SELF_TEST PASS")
    return 0

def run(a):
    raw=readcsv(a.current_universe,dtype={"code":str})
    source=readcsv(a.trust_source,dtype={"code":str})
    listing=listing_map(a.listing_source)
    obs=readcsv(a.observation_coverage,dtype=str)
    out=Path(a.output_dir); out.mkdir(parents=True,exist_ok=True)
    state_dir=Path(a.state_dir); state_dir.mkdir(parents=True,exist_ok=True)
    events_p=state_dir/"candidate_state.csv"
    daily_p=state_dir/"candidate_daily_ledger.csv"
    trans_p=state_dir/"candidate_transition_ledger.csv"
    meta_p=state_dir/"tracker_meta.json"
    events=readcsv(events_p,dtype={"code":str})
    daily=readcsv(daily_p,dtype={"code":str})
    trans=readcsv(trans_p,dtype={"code":str})
    if source.empty:
        # valid zero day: advance active clocks with no new origin rows.
        current_date=""
        if not obs.empty and "observation_date" in obs.columns:
            z=obs[obs.get("observation_status","").astype(str).eq("OBSERVED_READY")]
            if len(z): current_date=str(z.iloc[-1]["observation_date"])
        if not current_date:
            print("REAL_FULL_TRACKER no source/no observation date; skip")
            return 0
    else:
        current_date=str(source.iloc[0]["signal_date"])
    meta={}
    if meta_p.exists():
        try:meta=json.loads(meta_p.read_text(encoding="utf-8"))
        except:meta={}
    if meta.get("last_processed_date")==current_date:
        print("REAL_FULL_TRACKER idempotent same-day skip",current_date)
        # emit last alert board if present
        return 0

    rmap=current_raw_map(raw)
    # current pool membership for source Top15
    if not source.empty:
        source=source.copy(); source["code"]=source["code"].map(code)
        source["rank"]=pd.to_numeric(source["rank"],errors="coerce")
    if events.empty:
        events=pd.DataFrame()
    # update existing active events first
    updated=[]
    transition_rows=[]
    daily_rows=[]
    active_codes=set()
    if not events.empty:
        for _,er in events.iterrows():
            e=er.to_dict()
            st=text(e.get("state"))
            if st not in {"WAIT","NEAR_READY"}:
                updated.append(e); continue
            c=code(e.get("code")); active_codes.add(c)
            crow=rmap.get(c)
            cur=extract_row(crow) if crow is not None else None
            p=cur["current_price"] if cur and math.isfinite(cur["current_price"]) else listing.get(c,np.nan)
            age=int(num(e.get("wait_age")) if math.isfinite(num(e.get("wait_age"))) else 0)+1
            old=st
            e2=evaluate(e,crow,p,age,current_date)
            updated.append(e2)
            daily_rows.append({**e2,"daily_date":current_date,"daily_recorded_at_kst":kstnow()})
            if e2["state"]!=old:
                transition_rows.append({
                    "tracker_id":TRACKER_ID,"event_id":e2["event_id"],"code":c,"name":e2["name"],
                    "transition_date":current_date,"from_state":old,"to_state":e2["state"],
                    "wait_age":e2["wait_age"],"missing_conditions":e2["missing_conditions"],
                    "last_price":e2["last_price"],"frozen_stop":e2["frozen_stop"],
                    "auto_order_allowed":0,
                })
    events=pd.DataFrame(updated) if updated else pd.DataFrame()

    # new Top15 events only if no active event for code and no terminal event created today
    if not source.empty:
        for _,sr in source.sort_values("rank").iterrows():
            c=code(sr["code"])
            if c in active_codes: continue
            if not events.empty:
                same_today=events[(events["code"].map(code).eq(c)) & (events.get("terminal_date","").astype(str).eq(current_date))]
                if len(same_today): continue
            crow=rmap.get(c)
            if crow is None: continue
            e=make_event(crow,current_date,int(sr["rank"]))
            # carry source name if raw name missing
            if not e["name"]: e["name"]=text(sr.get("name"))
            events=pd.concat([events,pd.DataFrame([e])],ignore_index=True)
            daily_rows.append({**e,"daily_date":current_date,"daily_recorded_at_kst":kstnow()})
            transition_rows.append({
                "tracker_id":TRACKER_ID,"event_id":e["event_id"],"code":c,"name":e["name"],
                "transition_date":current_date,"from_state":"NEW","to_state":e["state"],
                "wait_age":e["wait_age"],"missing_conditions":e["missing_conditions"],
                "last_price":e["last_price"],"frozen_stop":e["frozen_stop"],"auto_order_allowed":0,
            })

    if daily_rows:
        add=pd.DataFrame(daily_rows)
        if not daily.empty:
            keys=set((daily["event_id"].astype(str)+"|"+daily["daily_date"].astype(str)).tolist())
            add=add[~(add["event_id"].astype(str)+"|"+add["daily_date"].astype(str)).isin(keys)]
        daily=pd.concat([daily,add],ignore_index=True) if not add.empty else daily
    if transition_rows:
        trans=pd.concat([trans,pd.DataFrame(transition_rows)],ignore_index=True)

    # Mature READY outcomes from causal price cache.
    needed=set(events["code"].map(code)) if not events.empty else set()
    frames={}
    if needed:
        try:
            frames,_,_=load_price_frames(Path(a.price_cache_dir),Path(a.amount_cache_dir),Path(a.asof_cache_dir),needed_codes=needed)
        except Exception as ex:
            print("REAL_FULL_TRACKER price cache warning",type(ex).__name__,ex)
    if not events.empty:
        for i in events.index:
            vals=mature_from_ready(events.loc[i].to_dict(),frames.get(code(events.at[i,"code"])))
            for k,v in vals.items(): events.at[i,k]=v

    events.to_csv(events_p,index=False,encoding="utf-8-sig")
    daily.to_csv(daily_p,index=False,encoding="utf-8-sig")
    trans.to_csv(trans_p,index=False,encoding="utf-8-sig")
    meta={"tracker_id":TRACKER_ID,"tracker_revision":TRACKER_REVISION,"last_processed_date":current_date,
          "processed_at_kst":kstnow(),"auto_order_allowed":0}
    meta_p.write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")

    alerts=events[
        ((events["state"].eq("READY")) & (events["ready_date"].astype(str).eq(current_date))) |
        (events["state"].eq("NEAR_READY"))
    ].copy() if not events.empty else pd.DataFrame()
    active=events[events["state"].isin(["WAIT","NEAR_READY"])].copy() if not events.empty else pd.DataFrame()
    manual=alerts[alerts["state"].eq("READY")].copy() if not alerts.empty else pd.DataFrame()

    events.to_csv(out/"real_full_decision_event_ledger.csv",index=False,encoding="utf-8-sig")
    daily.to_csv(out/"real_full_decision_daily_ledger.csv",index=False,encoding="utf-8-sig")
    trans.to_csv(out/"real_full_decision_transition_ledger.csv",index=False,encoding="utf-8-sig")
    alerts.to_csv(out/"real_full_decision_alert_board.csv",index=False,encoding="utf-8-sig")
    active.to_csv(out/"real_full_decision_tracking_board.csv",index=False,encoding="utf-8-sig")
    manual.to_csv(out/"real_full_manual_review_queue.csv",index=False,encoding="utf-8-sig")
    # compatibility names
    alerts.to_csv(out/"real_full_decision_current.csv",index=False,encoding="utf-8-sig")
    events.to_csv(out/"real_full_decision_ledger.csv",index=False,encoding="utf-8-sig")

    for group in ["origin_action","origin_wave_state","origin_watermelon_state","origin_recommendation_stage",
                  "origin_stage_status","origin_cloud_state","origin_refine_state","origin_search_pattern"]:
        scorecard(events,group).to_csv(out/f"real_full_decision_scorecard_{group}.csv",index=False,encoding="utf-8-sig")

    b=brief(alerts,events,current_date)
    (out/"real_full_decision_brief.txt").write_text(b,encoding="utf-8")
    print(b)
    return 0

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--current-universe",default="reports/real_full_current_universe.csv")
    ap.add_argument("--trust-source",default="reports/real_full_trust_source.csv")
    ap.add_argument("--listing-source",default="reports/v73_listing_cache.csv")
    ap.add_argument("--observation-coverage",default="reports/.cache/real_full_trust_r1/observation_coverage.csv")
    ap.add_argument("--state-dir",default="reports/.cache/real_full_trust_r1/decision")
    ap.add_argument("--output-dir",default="reports/real_full_decision_r1")
    ap.add_argument("--price-cache-dir",default="reports/.cache/v20_price_history")
    ap.add_argument("--amount-cache-dir",default="reports/.cache/v25_actual_amount_history")
    ap.add_argument("--asof-cache-dir",default="reports/.cache/v20_asof_snapshots")
    ap.add_argument("--self-test",action="store_true")
    a=ap.parse_args()
    return self_test() if a.self_test else run(a)
if __name__=="__main__": raise SystemExit(main())
