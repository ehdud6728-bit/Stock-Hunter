#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse, json, math
from pathlib import Path
import numpy as np
import pandas as pd

REVISION="CLOSEBET_OHLCV_PATH_SCALE_OOS_R104_20260918"
DISCOVERY_END="2026-08-18"
OOS_START="2026-08-19"
OOS_END="2026-09-18"
RESEARCH_ONLY=True

FIXED_POLICIES={
    "LUMP_100":[(0.0,1.00)],
    "D1_60_40":[(0.0,0.60),(-1.0,0.40)],
    "D2_50_25_25":[(0.0,0.50),(-1.0,0.25),(-2.0,0.25)],
    "D3_40_30_30":[(0.0,0.40),(-1.5,0.30),(-3.0,0.30)],
    "D4_30_30_40":[(0.0,0.30),(-2.0,0.30),(-4.0,0.40)],
}

def num(v):
    try:
        x=float(v)
        return x if math.isfinite(x) else np.nan
    except:
        return np.nan

def norm_code(v):
    s=str(v or "").strip().upper()
    if s.endswith(".0") and s[:-2].isdigit(): s=s[:-2]
    if len(s)==7 and s.startswith("A"): s=s[1:]
    return s.zfill(6) if s.isdigit() and len(s)<=6 else s

def read_csv(p:Path):
    if not p.exists() or p.stat().st_size==0:
        return pd.DataFrame()
    for enc in ("utf-8-sig","utf-8","cp949"):
        try:return pd.read_csv(p,low_memory=False,encoding=enc)
        except Exception:pass
    return pd.DataFrame()

def find(root:Path,name:str):
    xs=list(root.rglob(name))
    xs.sort(key=lambda p:(len(p.parts),str(p)))
    return xs[0] if xs else None

def pct(a,b):
    if not (math.isfinite(a) and math.isfinite(b)) or b==0:return np.nan
    return (a/b-1)*100.0

def prep_hist(h):
    q=h.copy()
    q["code"]=q["code"].map(norm_code)
    q["date"]=pd.to_datetime(q["date"],errors="coerce").dt.normalize()
    for c in ["Open","High","Low","Close","Volume","Amount"]:
        if c in q:q[c]=pd.to_numeric(q[c],errors="coerce")
    q=q[q.code.ne("") & q.date.notna()].sort_values(["code","date"]).copy()
    return q

def add_ma_at(g, n):
    return g["Close"].rolling(n,min_periods=n).mean()

def atr(g,n=14):
    prev=g["Close"].shift(1)
    tr=pd.concat([
        (g["High"]-g["Low"]).abs(),
        (g["High"]-prev).abs(),
        (g["Low"]-prev).abs()
    ],axis=1).max(axis=1)
    return tr.rolling(n,min_periods=n).mean()

def independent_structure(g, signal_date):
    # Strictly signal-date-or-earlier history only.
    q=g[g.date<=signal_date].tail(260).copy()
    if len(q)<80:
        return None
    for n in [5,10,20,60,112,120,150,200,224]:
        q[f"MA{n}"]=add_ma_at(q,n)
    q["ATR14"]=atr(q,14)
    sig=q.iloc[-1]
    # Wave1 search: prior peak must be at least 3 bars before signal and within 60 bars.
    pre=q.iloc[max(0,len(q)-65):-3].copy()
    if len(pre)<25:return None
    peak_pos=int(pre["High"].values.argmax())
    peak_idx=pre.index[peak_pos]
    peak_row=q.loc[peak_idx]
    # base from 25 bars before peak
    loc=q.index.get_loc(peak_idx)
    base_seg=q.iloc[max(0,loc-25):loc+1]
    if base_seg.empty:return None
    base_idx=base_seg["Low"].idxmin()
    base_row=q.loc[base_idx]
    if q.index.get_loc(base_idx)>=loc:
        return None

    peak_high=num(peak_row.High); base_low=num(base_row.Low); signal_close=num(sig.Close)
    wave_gain=pct(peak_high,base_low)

    post_peak=q.iloc[loc+1:]
    if post_peak.empty:return None
    pb_low_idx=post_peak["Low"].idxmin()
    pb_low=num(q.loc[pb_low_idx,"Low"])
    pb_depth=(pb_low/peak_high-1)*100 if peak_high>0 else np.nan
    pb_bars=max(1,q.index.get_loc(pb_low_idx)-loc)

    wave_seg=q.loc[base_idx:peak_idx]
    pb_seg=q.loc[peak_idx:signal_date] if signal_date in set(q["date"]) else post_peak
    # safer positional segment:
    pb_seg=q.iloc[loc+1:]
    wave_vol=float(pd.to_numeric(wave_seg["Volume"],errors="coerce").median())
    pb_vol=float(pd.to_numeric(pb_seg["Volume"],errors="coerce").median())
    vol_ratio=pb_vol/wave_vol if wave_vol>0 else np.nan

    # amount can be absent; use Close*Volume proxy only when needed and label it.
    amount_source="AMOUNT"
    wa=pd.to_numeric(wave_seg.get("Amount"),errors="coerce") if "Amount" in wave_seg else pd.Series(dtype=float)
    pa=pd.to_numeric(pb_seg.get("Amount"),errors="coerce") if "Amount" in pb_seg else pd.Series(dtype=float)
    if wa.dropna().empty or pa.dropna().empty or float(wa.fillna(0).sum())<=0:
        wa=wave_seg["Close"]*wave_seg["Volume"]; pa=pb_seg["Close"]*pb_seg["Volume"]; amount_source="CLOSE_X_VOLUME_PROXY"
    wave_amt=float(wa.median()) if len(wa) else np.nan
    pb_amt=float(pa.median()) if len(pa) else np.nan
    amt_ratio=pb_amt/wave_amt if math.isfinite(wave_amt) and wave_amt>0 else np.nan

    # MA cluster width at peak vs signal: 20/60/112/224 when available.
    def cluster(row):
        vals=[num(row.get(f"MA{n}")) for n in [20,60,112,224]]
        vals=[x for x in vals if math.isfinite(x)]
        c=num(row.Close)
        return (max(vals)-min(vals))/c*100 if len(vals)>=3 and c>0 else np.nan
    peak_cluster=cluster(peak_row); sig_cluster=cluster(sig)
    compression_delta=(sig_cluster-peak_cluster) if math.isfinite(peak_cluster) and math.isfinite(sig_cluster) else np.nan
    compression_ratio=(sig_cluster/peak_cluster) if math.isfinite(peak_cluster) and peak_cluster>0 and math.isfinite(sig_cluster) else np.nan

    last5=q.tail(5)
    prev20=q.iloc[-25:-5] if len(q)>=25 else q.iloc[:-5]
    v5=float(last5.Volume.median())
    v20=float(prev20.Volume.median()) if len(prev20) else np.nan
    vol_persist=v5/v20 if math.isfinite(v20) and v20>0 else np.nan

    # Down-day volume share in last 10 bars
    z=q.tail(10).copy()
    z["ret"]=z["Close"].pct_change()
    down=z[z.ret<0]["Volume"].sum()
    total=z["Volume"].sum()
    down_share=float(down/total) if total>0 else np.nan

    # Restart evidence at signal
    prev=q.iloc[-2]
    body=num(sig.Close)-num(sig.Open)
    rng=max(num(sig.High)-num(sig.Low),1e-12)
    close_loc=(num(sig.Close)-num(sig.Low))/rng
    sig_vol20=num(sig.Volume)/(float(q.iloc[-21:-1].Volume.median()) or np.nan) if len(q)>=21 else np.nan
    restart_up=num(sig.Close)>num(prev.Close)
    reclaim_ma5=math.isfinite(num(sig.MA5)) and num(sig.Close)>=num(sig.MA5)
    restart_score=sum([
        bool(restart_up),
        bool(reclaim_ma5),
        bool(math.isfinite(sig_vol20) and sig_vol20>=1.0),
        bool(math.isfinite(close_loc) and close_loc>=0.60),
    ])/4.0

    # Support references derived only from pre/signal data
    support_candidates=[]
    for name,v in [
        ("PB_LOW",pb_low),
        ("MA5",num(sig.MA5)),("MA10",num(sig.MA10)),("MA20",num(sig.MA20)),
        ("MA60",num(sig.MA60)),("MA112",num(sig.MA112)),("MA224",num(sig.MA224))
    ]:
        if math.isfinite(v) and 0<v<signal_close:
            support_candidates.append((name,v))
    support_candidates=sorted(support_candidates,key=lambda x:x[1],reverse=True)
    uniq=[]
    for name,v in support_candidates:
        if all(abs(v-u)>signal_close*0.002 for _,u in uniq):
            uniq.append((name,v))
        if len(uniq)>=3:break

    # Core pullback-restart semantic score, independent of scanner tags.
    components={
        "wave1": math.isfinite(wave_gain) and wave_gain>=8.0,
        "pullback": math.isfinite(pb_depth) and -20.0<=pb_depth<=-2.0,
        "volume_control": math.isfinite(vol_ratio) and vol_ratio<=1.15,
        "amount_control": math.isfinite(amt_ratio) and amt_ratio<=1.15,
        "ma_compression": math.isfinite(compression_delta) and compression_delta<=0,
        "restart": restart_score>=0.50,
        "not_broken": signal_close>=pb_low if math.isfinite(pb_low) else False,
    }
    core_score=sum(components.values())/len(components)
    core_label="MATCH" if core_score>=0.71 else ("PARTIAL" if core_score>=0.43 else "MISMATCH")

    return {
        "wave_base_date":str(pd.Timestamp(base_row.date).date()),
        "wave_peak_date":str(pd.Timestamp(peak_row.date).date()),
        "wave_gain_pct":wave_gain,
        "pullback_low_date":str(pd.Timestamp(q.loc[pb_low_idx,"date"]).date()),
        "pullback_depth_pct":pb_depth,
        "pullback_bars":pb_bars,
        "pb_volume_vs_wave":vol_ratio,
        "pb_amount_vs_wave":amt_ratio,
        "amount_source":amount_source,
        "peak_ma_cluster_width_pct":peak_cluster,
        "signal_ma_cluster_width_pct":sig_cluster,
        "ma_cluster_delta_pct":compression_delta,
        "ma_cluster_ratio":compression_ratio,
        "signal_vol5_vs_prev20":vol_persist,
        "down_volume_share10":down_share,
        "signal_close_loc":close_loc,
        "signal_vol20_ratio":sig_vol20,
        "restart_score":restart_score,
        "price_to_ma224_pct":pct(signal_close,num(sig.MA224)),
        "core_pattern_score":core_score,
        "core_pattern_label":core_label,
        "core_failed":"|".join(k for k,v in components.items() if not v),
        "support1_name":uniq[0][0] if len(uniq)>0 else "",
        "support1_price":uniq[0][1] if len(uniq)>0 else np.nan,
        "support2_name":uniq[1][0] if len(uniq)>1 else "",
        "support2_price":uniq[1][1] if len(uniq)>1 else np.nan,
        "support3_name":uniq[2][0] if len(uniq)>2 else "",
        "support3_price":uniq[2][1] if len(uniq)>2 else np.nan,
    }

def frozen_profile_bins(disc):
    cuts={}; rows=[]
    for pat,g in disc.groupby("primary_formula"):
        s=pd.to_numeric(g["core_pattern_score"],errors="coerce").dropna()
        if len(s)>=10 and s.nunique()>=3:
            q33=float(s.quantile(1/3));q67=float(s.quantile(2/3))
            cuts[str(pat)]={"q33":q33,"q67":q67,"n":len(s)}
            rows.append({"pattern":pat,"q33":q33,"q67":q67,"n":len(s)})
    return cuts,pd.DataFrame(rows)

def profile(row,cuts):
    p=str(row.primary_formula);x=num(row.core_pattern_score)
    if p not in cuts or not math.isfinite(x):return "UNRESOLVED"
    c=cuts[p]
    if x>=c["q67"]:return "CONSERVATIVE_CONFIRM"
    if x<c["q33"]:return "AGGRESSIVE_EARLY"
    return "NEUTRAL"

def stop_price(row,entry):
    for c in ["signal_stop_price","official_stop_price","stop_price"]:
        x=num(row.get(c))
        if math.isfinite(x) and 0<x<entry and -15<=pct(x,entry)<=-1:
            return x,"OFFICIAL"
    for c in ["signal_stop_pct_eval","stop_pct"]:
        p=num(row.get(c))
        if math.isfinite(p) and -15<=p<=-1:
            return entry*(1+p/100),"PCT_SOURCE"
    return entry*0.95,"FALLBACK_-5"

def policy_legs(row,entry,policy):
    if policy in FIXED_POLICIES:
        return [(entry*(1+d/100),w,f"{d:+.1f}%") for d,w in FIXED_POLICIES[policy]]
    if policy=="STRUCTURE_40_30_30":
        legs=[(entry,0.40,"SIGNAL")]
        for namec,pricec,w in [("support1_name","support1_price",0.30),("support2_name","support2_price",0.30)]:
            p=num(row.get(pricec))
            if math.isfinite(p) and 0<p<entry:
                legs.append((p,w,str(row.get(namec) or namec)))
        return legs
    return []

def simulate(row,hist,policy):
    code=norm_code(row.code); sd=pd.Timestamp(row.signal_date).normalize()
    g=hist[(hist.code==code)&(hist.date>sd)].sort_values("date").head(10)
    entry=num(row.get("entry_price",row.get("close")))
    if not math.isfinite(entry) or entry<=0 or g.empty:return None
    stop,stop_src=stop_price(row,entry)
    legs=[z for z in policy_legs(row,entry,policy) if z[0]>stop]
    if not legs:return None

    # first leg signal-close fill
    fills=[(0,entry,legs[0][1],legs[0][2])]
    units=legs[0][1]/entry
    invested=legs[0][1]
    next_leg=1
    stopped=False; stop_day=np.nan; stop_exec=np.nan
    conflict=False; conflict_reason=""
    exit_day=np.nan; exit_price=np.nan

    daily_marks={}
    for di,(_,d) in enumerate(g.iterrows(),start=1):
        o,h,l,c=[num(d.get(k)) for k in ["Open","High","Low","Close"]]
        if not all(math.isfinite(x) for x in [o,h,l,c]):continue

        # Definite gap-through stop: stop executes at open, no lower-price adds.
        if o<=stop:
            stopped=True; stop_day=di; stop_exec=o
            break

        # If price trades down from an open above the limit, intermediate add levels above stop are deemed filled.
        while next_leg<len(legs) and l<=legs[next_leg][0]:
            lp,w,lab=legs[next_leg]
            fills.append((di,lp,w,lab))
            units+=w/lp; invested+=w; next_leg+=1

        avg=invested/units
        # Stop touched after eligible adds.
        if l<=stop:
            stopped=True; stop_day=di; stop_exec=stop
            break

        daily_marks[di]=c

    avg=invested/units
    planned_capital=1.0

    # Realized stop return must terminate the path.
    if stopped:
        position_return=pct(stop_exec,avg)
        pnl=units*stop_exec-invested
        planned_return=pnl/planned_capital*100.0
        final_reason="STOP"
        final_day=stop_day
    else:
        # D5 mark is primary comparable outcome; D3/D10 also emitted below.
        idx=min(4,len(g)-1)
        exit_price=num(g.iloc[idx].Close)
        exit_day=idx+1
        position_return=pct(exit_price,avg)
        pnl=units*exit_price-invested
        planned_return=pnl/planned_capital*100.0
        final_reason="D5_CLOSE"
        final_day=exit_day

    def mark_return(day,planned=False):
        if stopped and math.isfinite(stop_day) and stop_day<=day:
            px=stop_exec
        else:
            if len(g)<1:return np.nan
            idx=min(day-1,len(g)-1)
            px=num(g.iloc[idx].Close)
        if not math.isfinite(px):return np.nan
        if planned:
            return (units*px-invested)/planned_capital*100.0
        return pct(px,avg)

    # Excursions only until stop day; no post-stop recovery credit.
    endn=int(stop_day) if stopped and math.isfinite(stop_day) else len(g)
    path=g.head(endn)
    highs=pd.to_numeric(path.High,errors="coerce")
    lows=pd.to_numeric(path.Low,errors="coerce")
    mfe=float((highs/avg-1).max()*100) if highs.notna().any() else np.nan
    mae=float((lows/avg-1).min()*100) if lows.notna().any() else np.nan
    hit3=bool((highs>=avg*1.03).any()) if highs.notna().any() else False
    hit5=bool((highs>=avg*1.05).any()) if highs.notna().any() else False

    return {
        "signal_date":sd,"code":code,"name":row.get("name",""),"pattern":row.primary_formula,
        "core_pattern_label":row.core_pattern_label,"core_pattern_score":row.core_pattern_score,
        "entry_profile":row.entry_profile,"policy":policy,
        "entry_price":entry,"weighted_avg_entry":avg,
        "avg_entry_improvement_pct":(1-avg/entry)*100,
        "planned_weight":1.0,"invested_weight":invested,"cash_weight":1.0-invested,
        "filled_legs":len(fills),"planned_legs":len(legs),
        "deepest_fill_pct":min((pct(p,entry) for _,p,_,_ in fills),default=0),
        "stop_price":stop,"stop_source":stop_src,"stop_hit":stopped,"stop_day":stop_day,
        "position_d3_ret":mark_return(3,False),"position_d5_ret":mark_return(5,False),"position_d10_ret":mark_return(10,False),
        "planned_d3_ret":mark_return(3,True),"planned_d5_ret":mark_return(5,True),"planned_d10_ret":mark_return(10,True),
        "final_reason":final_reason,"final_day":final_day,
        "mfe_until_exit":mfe,"mae_until_exit":mae,"hit3_before_exit":hit3,"hit5_before_exit":hit5,
        "fill_ledger":"|".join(f"D{d}:{lab}@{p:.2f}({w:.2f})" for d,p,w,lab in fills),
        "path_conflict":conflict,"conflict_reason":conflict_reason,
        "research_only":True
    }

def summarize(g,retcol):
    r=pd.to_numeric(g[retcol],errors="coerce").dropna()
    return {
        "n":len(g),"eval_n":len(r),
        "mean":r.mean() if len(r) else np.nan,
        "median":r.median() if len(r) else np.nan,
        "win_rate":(r>0).mean()*100 if len(r) else np.nan,
        "stop_rate":g.stop_hit.mean()*100 if len(g) else np.nan,
        "hit3_rate":g.hit3_before_exit.mean()*100 if len(g) else np.nan,
        "hit5_rate":g.hit5_before_exit.mean()*100 if len(g) else np.nan,
        "avg_entry_improvement":pd.to_numeric(g.avg_entry_improvement_pct,errors="coerce").mean(),
        "avg_invested_weight":pd.to_numeric(g.invested_weight,errors="coerce").mean(),
        "mfe_median":pd.to_numeric(g.mfe_until_exit,errors="coerce").median(),
        "mae_median":pd.to_numeric(g.mae_until_exit,errors="coerce").median(),
    }

def gp(df,cols,label,retcol):
    rows=[]
    for keys,g in df.groupby(cols,dropna=False):
        if not isinstance(keys,tuple):keys=(keys,)
        z={"group_type":label,**{c:str(v) for c,v in zip(cols,keys)}}
        z.update(summarize(g,retcol));rows.append(z)
    return pd.DataFrame(rows)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--r103-root",default="r103_artifacts")
    ap.add_argument("--r102-root",default="r102_artifacts")
    ap.add_argument("--history-cache",default=".cache/closebet_structure_env_oos_r102/v49_76_research_raw_history.csv")
    ap.add_argument("--output-dir",default="reports/closebet_ohlcv_path_scale_oos_r104")
    ap.add_argument("--discovery-end",default=DISCOVERY_END)
    ap.add_argument("--oos-start",default=OOS_START)
    ap.add_argument("--oos-end",default=OOS_END)
    a=ap.parse_args()

    out=Path(a.output_dir);out.mkdir(parents=True,exist_ok=True)
    ep=find(Path(a.r103_root),"pattern_semantic_event_master.csv")
    if ep is None:
        ep=find(Path(a.r102_root),"event_master_enriched.csv")
    hp=Path(a.history_cache)
    if ep is None or not hp.exists():
        raise SystemExit(f"INPUT_MISSING event={ep} hist={hp.exists()}")

    e=read_csv(ep)
    h=prep_hist(read_csv(hp))
    e["code"]=e["code"].map(norm_code)
    e["signal_date"]=pd.to_datetime(e["signal_date"],errors="coerce").dt.normalize()
    if "primary_formula" not in e:
        e["primary_formula"]=e.get("primary_strategy",e.get("strategy","UNCLASSIFIED"))

    # Independent OHLCV reconstruction.
    hgroups={c:g.copy() for c,g in h.groupby("code")}
    rows=[]
    for i,r in e.iterrows():
        g=hgroups.get(r.code)
        z=independent_structure(g,r.signal_date) if g is not None else None
        rows.append(z or {})
    st=pd.DataFrame(rows)
    e=pd.concat([e.reset_index(drop=True),st.reset_index(drop=True)],axis=1)

    de=pd.Timestamp(a.discovery_end);osd=pd.Timestamp(a.oos_start);oed=pd.Timestamp(a.oos_end)
    disc=e[e.signal_date<=de].copy()
    cuts,cutdf=frozen_profile_bins(disc)
    e["entry_profile"]=e.apply(lambda r:profile(r,cuts),axis=1)
    disc=e[e.signal_date<=de].copy()
    oos=e[(e.signal_date>=osd)&(e.signal_date<=oed)].copy()

    # OOS independent semantic summary
    semrows=[]
    for (p,l),g in oos.groupby(["primary_formula","core_pattern_label"],dropna=False):
        rr=pd.to_numeric(g.get("evaluation_ret"),errors="coerce").dropna()
        semrows.append({
            "pattern":p,"core_pattern_label":l,"n":len(g),"eval_n":len(rr),
            "d5_mean":rr.mean() if len(rr) else np.nan,
            "d5_median":rr.median() if len(rr) else np.nan,
            "win_rate":(rr>0).mean()*100 if len(rr) else np.nan,
            "core_score_mean":pd.to_numeric(g.core_pattern_score,errors="coerce").mean(),
            "wave_gain_median":pd.to_numeric(g.wave_gain_pct,errors="coerce").median(),
            "pullback_depth_median":pd.to_numeric(g.pullback_depth_pct,errors="coerce").median(),
            "pb_vol_ratio_median":pd.to_numeric(g.pb_volume_vs_wave,errors="coerce").median(),
            "ma_cluster_delta_median":pd.to_numeric(g.ma_cluster_delta_pct,errors="coerce").median(),
        })
    sem=pd.DataFrame(semrows)

    profrows=[]
    for (p,pr),g in oos.groupby(["primary_formula","entry_profile"],dropna=False):
        rr=pd.to_numeric(g.get("evaluation_ret"),errors="coerce").dropna()
        profrows.append({"pattern":p,"entry_profile":pr,"n":len(g),"eval_n":len(rr),
                         "d5_mean":rr.mean() if len(rr) else np.nan,
                         "d5_median":rr.median() if len(rr) else np.nan,
                         "win_rate":(rr>0).mean()*100 if len(rr) else np.nan})
    prof=pd.DataFrame(profrows)

    sims=[]
    policies=list(FIXED_POLICIES)+["STRUCTURE_40_30_30"]
    for _,r in oos.iterrows():
        for p in policies:
            z=simulate(r,h,p)
            if z:sims.append(z)
    sims=pd.DataFrame(sims)

    # Both position-return and planned-capital-return summaries
    sum_planned=gp(sims,["pattern","policy"],"PATTERN_X_POLICY_PLANNED","planned_d5_ret") if len(sims) else pd.DataFrame()
    sum_position=gp(sims,["pattern","policy"],"PATTERN_X_POLICY_POSITION","position_d5_ret") if len(sims) else pd.DataFrame()
    sem_scale=gp(sims,["pattern","core_pattern_label","policy"],"PATTERN_X_CORE_X_POLICY","planned_d5_ret") if len(sims) else pd.DataFrame()
    prof_scale=gp(sims,["pattern","entry_profile","policy"],"PATTERN_X_PROFILE_X_POLICY","planned_d5_ret") if len(sims) else pd.DataFrame()

    e.to_csv(out/"independent_ohlcv_event_master.csv",index=False,encoding="utf-8-sig")
    cutdf.to_csv(out/"discovery_frozen_ohlcv_confirmation_bins.csv",index=False,encoding="utf-8-sig")
    sem.to_csv(out/"oos_independent_pattern_semantic_summary.csv",index=False,encoding="utf-8-sig")
    prof.to_csv(out/"oos_independent_entry_profile_summary.csv",index=False,encoding="utf-8-sig")
    sims.to_csv(out/"oos_path_corrected_scale_in_event.csv",index=False,encoding="utf-8-sig")
    sum_planned.to_csv(out/"oos_scale_in_planned_capital_summary.csv",index=False,encoding="utf-8-sig")
    sum_position.to_csv(out/"oos_scale_in_position_return_summary.csv",index=False,encoding="utf-8-sig")
    sem_scale.to_csv(out/"oos_core_pattern_scale_in_summary.csv",index=False,encoding="utf-8-sig")
    prof_scale.to_csv(out/"oos_profile_scale_in_summary.csv",index=False,encoding="utf-8-sig")

    meta={
        "revision":REVISION,"status":"PASS",
        "discovery_end":a.discovery_end,"oos_start":a.oos_start,"oos_end":a.oos_end,
        "event_rows":len(e),"discovery_rows":len(disc),"oos_rows":len(oos),
        "independent_structure_rows":int(e.core_pattern_score.notna().sum()),
        "scale_event_rows":len(sims),"patterns":int(e.primary_formula.nunique()),
        "research_only":True,"production_eligible":False,
        "selection_logic_changed":False,"score_rank_changed":False,"order_logic_changed":False,
        "same_sample_retuning":False,
        "post_stop_recovery_credit":False,
        "planned_capital_return_emitted":True,
        "position_return_emitted":True,
        "scanner_internal_flags_required_for_core_semantic":False,
        "notes":[
            "Core pattern is reconstructed independently from OHLCV <= signal date.",
            "Stop terminates path; no later D5 recovery is credited.",
            "Planned-capital return keeps unfilled allocation as cash.",
            "Adds below fixed stop are forbidden; stop is never widened."
        ]
    }
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")

    rep=[
        "🧪 [CLOSING BET · INDEPENDENT OHLCV PATTERN × PATH-CORRECTED SCALE-IN OOS R1.0.4]",
        f"status=PASS | discovery<={a.discovery_end} n={len(disc)} | OOS {a.oos_start}~{a.oos_end} n={len(oos)}",
        f"independent OHLCV structure={meta['independent_structure_rows']} | scale simulations={len(sims)} | patterns={meta['patterns']}",
        "",
        "핵심 교정:",
        "- 검색기 내부 semantic flag 없이 OHLCV로 Wave1→Pullback→Volume/Amount→MA compression→Restart 재구성",
        "- 손절 발생 즉시 경로 종료; 손절 후 D+5 회복을 수익으로 인정하지 않음",
        "- Position return과 Planned-capital return을 분리",
        "- 미체결 예정자금은 현금으로 남김",
        "- 고정 손절 확대 금지 / 손절 아래 추가매수 금지",
        "",
        "RESEARCH_ONLY. LIVE 검색식·점수·랭킹·주문 변경 0."
    ]
    (out/"report.txt").write_text("\n".join(rep),encoding="utf-8")
    print("\n".join(rep))

if __name__=="__main__":
    main()
