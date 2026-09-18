#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import argparse, json, math
from pathlib import Path
import numpy as np
import pandas as pd

REVISION="CLOSEBET_PAIRED_ANATOMY_R1062_CHECKPOINT_LOOP_FIX_20260918"
DISCOVERY_END="2026-08-18"
OOS_START="2026-08-19"
OOS_END="2026-09-18"

FOCUS_PATTERNS={"C","B1","B2","I"}
MATCH_FEATURES=["wave_gain_pct","pullback_depth_pct","price_to_ma224_pct","entry_stock_ret_20d","marcap"]
TRAJ_DAYS=list(range(-10,1))

def read_csv(p):
    p=Path(p)
    if not p.exists() or p.stat().st_size==0:return pd.DataFrame()
    for enc in ("utf-8-sig","utf-8","cp949"):
        try:return pd.read_csv(p,low_memory=False,encoding=enc)
        except Exception:pass
    return pd.DataFrame()

def find(root,name):
    xs=list(Path(root).rglob(name));xs.sort(key=lambda p:(len(p.parts),str(p)))
    return xs[0] if xs else None

def norm_code(v):
    s=str(v or "").strip().upper()
    if s.endswith(".0") and s[:-2].isdigit():s=s[:-2]
    if len(s)==7 and s.startswith("A"):s=s[1:]
    return s.zfill(6) if s.isdigit() and len(s)<=6 else s

def num(v):
    try:
        x=float(v);return x if math.isfinite(x) else np.nan
    except:return np.nan

def prep_hist(h):
    q=h.copy()
    q["code"]=q["code"].map(norm_code)
    q["date"]=pd.to_datetime(q["date"],errors="coerce").dt.normalize()
    for c in ["Open","High","Low","Close","Volume","Amount"]:
        if c in q:q[c]=pd.to_numeric(q[c],errors="coerce")
    q=q[q.code.ne("")&q.date.notna()].sort_values(["code","date"]).copy()
    return q

def robust_scale(disc):
    rows=[];scales={}
    for f in MATCH_FEATURES:
        if f not in disc:continue
        s=pd.to_numeric(disc[f],errors="coerce").dropna()
        if len(s)<30:continue
        med=float(s.median())
        mad=float((s-med).abs().median())
        scale=mad*1.4826
        if not math.isfinite(scale) or scale<=1e-12:
            scale=float(s.std()) if len(s)>1 else 1.0
        if not math.isfinite(scale) or scale<=1e-12:scale=1.0
        scales[f]={"median":med,"scale":scale,"n":len(s)}
        rows.append({"feature":f,"median":med,"robust_scale":scale,"n":len(s)})
    return scales,pd.DataFrame(rows)

def dist(a,b,scales):
    ds=[]
    for f,s in scales.items():
        av=num(a.get(f));bv=num(b.get(f))
        if math.isfinite(av) and math.isfinite(bv):
            ds.append(((av-bv)/s["scale"])**2)
    if len(ds)<2:return np.inf
    return float(math.sqrt(sum(ds)/len(ds)))

def make_pairs(oos,scales):
    rows=[]
    # Winner classes intentionally broad; STOP_FIRST is the target failure mechanism.
    winners=oos[oos["outcome_class"].isin(["BIG_WIN","NORMAL_WIN","QUICK_WIN","SHAKEOUT_WIN"])].copy()
    stops=oos[oos["outcome_class"].eq("STOP_FIRST")].copy()
    for pat in sorted(set(oos.primary_formula.astype(str)) & FOCUS_PATTERNS):
        w=winners[winners.primary_formula.astype(str).eq(pat)].copy()
        s=stops[stops.primary_formula.astype(str).eq(pat)].copy()
        used=set()
        # pair more structurally specific winners first
        w=w.sort_values(["core_pattern_label","signal_date","code"],kind="stable")
        for wi,wr in w.iterrows():
            cand=s[~s.index.isin(used)].copy()
            # first preference: same core label
            same=cand[cand.core_pattern_label.astype(str).eq(str(wr.core_pattern_label))]
            if len(same):cand=same
            best=None;bestd=np.inf
            for si,sr in cand.iterrows():
                d=dist(wr,sr,scales)
                if d<bestd:
                    bestd=d;best=(si,sr)
            if best is None or not math.isfinite(bestd):continue
            si,sr=best;used.add(si)
            row={
                "pattern":pat,
                "winner_date":wr.signal_date,"winner_code":wr.code,"winner_name":wr.get("name",""),
                "winner_core":wr.core_pattern_label,"winner_outcome":wr.outcome_class,
                "stop_date":sr.signal_date,"stop_code":sr.code,"stop_name":sr.get("name",""),
                "stop_core":sr.core_pattern_label,"stop_outcome":sr.outcome_class,
                "match_distance":bestd,
            }
            for f in scales:
                row["winner_"+f]=wr.get(f,np.nan);row["stop_"+f]=sr.get(f,np.nan)
            # snapshot context for descriptive contrast
            for f in ["market_ret_5d_t1","sector_peer_positive_pct","sector_peer_mean_ret_1d",
                      "USDKRW_ret5_pct","VIX_ret5_pct","US10Y_ret5_pct","SOX_ret5_pct","NASDAQ_ret5_pct"]:
                if f in oos:
                    row["winner_"+f]=wr.get(f,np.nan);row["stop_"+f]=sr.get(f,np.nan)
            rows.append(row)
    return pd.DataFrame(rows)

def trajectory(g,signal_date):
    # IMPORTANT: compute long MAs on the full causal lookback first,
    # then slice D-10..D0. This fixes the R1.0.6 short-window MA60/MA224 bug.
    q=g[g.date<=signal_date].sort_values("date").tail(280).copy()
    if len(q)<30:return pd.DataFrame()

    for n in [5,10,20,60,112,224]:
        q[f"MA{n}"]=q.Close.rolling(n,min_periods=n).mean()

    prev=q.Close.shift(1)
    tr=pd.concat([
        (q.High-q.Low).abs(),
        (q.High-prev).abs(),
        (q.Low-prev).abs()
    ],axis=1).max(axis=1)
    q["ATR14"]=tr.rolling(14,min_periods=14).mean()

    day_range=(q.High-q.Low).replace(0,np.nan)
    q["close_loc"]=(q.Close-q.Low)/day_range
    q["upper_wick"]=(q.High-q[["Open","Close"]].max(axis=1))/day_range

    q["vol20_med"]=q.Volume.rolling(20,min_periods=20).median().shift(1)
    q["vol20_ratio"]=q.Volume/q.vol20_med

    if "Amount" in q and q.Amount.notna().any():
        amount=q.Amount
    else:
        amount=q.Close*q.Volume
    q["_amount_series"]=amount
    q["amt20_med"]=q["_amount_series"].rolling(20,min_periods=20).median().shift(1)
    q["amt20_ratio"]=q["_amount_series"]/q.amt20_med

    sig=q.iloc[-1]
    sig_close=num(sig.Close)
    z=q.tail(11).copy()
    if len(z)<11:return pd.DataFrame()
    z["rel_day"]=range(-10,1)
    z["close_vs_d0_pct"]=(z.Close/sig_close-1)*100
    z["ma20_dist_pct"]=(z.Close/z.MA20-1)*100
    z["ma60_dist_pct"]=(z.Close/z.MA60-1)*100
    z["ma224_dist_pct"]=(z.Close/z.MA224-1)*100
    z["atr_pct"]=z.ATR14/z.Close*100

    return z[[
        "rel_day","date","close_vs_d0_pct","vol20_ratio","amt20_ratio",
        "close_loc","upper_wick","ma20_dist_pct","ma60_dist_pct",
        "ma224_dist_pct","atr_pct"
    ]]

def checkpoint_table(tr):
    if tr.empty:return pd.DataFrame()
    checkpoints=[-10,-5,-3,-2,-1,0]
    metrics=[
        "close_vs_d0_pct","vol20_ratio","amt20_ratio","close_loc","upper_wick",
        "ma20_dist_pct","ma60_dist_pct","ma224_dist_pct","atr_pct"
    ]
    z=tr[tr.rel_day.isin(checkpoints)].copy()
    rows=[]
    for (pid,p,side),g in z.groupby(["pair_id","pattern","side"]):
        row={"pair_id":pid,"pattern":p,"side":side}
        for d in checkpoints:
            gd=g[g.rel_day.eq(d)]
            for m in metrics:
                row[f"{m}_D{d:+d}"]=pd.to_numeric(gd[m],errors="coerce").iloc[0] if len(gd) else np.nan
        # useful explicit changes into signal day
        for m in ["vol20_ratio","amt20_ratio","close_loc","ma20_dist_pct","ma60_dist_pct","ma224_dist_pct","atr_pct"]:
            d5=row.get(f"{m}_D-5",np.nan);d1=row.get(f"{m}_D-1",np.nan);d0=row.get(f"{m}_D+0",np.nan)
            row[f"{m}_chg_D5_to_D0"]=d0-d5 if math.isfinite(num(d5)) and math.isfinite(num(d0)) else np.nan
            row[f"{m}_chg_D1_to_D0"]=d0-d1 if math.isfinite(num(d1)) and math.isfinite(num(d0)) else np.nan
        rows.append(row)
    return pd.DataFrame(rows)

def checkpoint_contrast(cp):
    if cp.empty:return pd.DataFrame()
    rows=[]
    feature_cols=[c for c in cp.columns if c not in {"pair_id","pattern","side"}]
    for p,g in cp.groupby("pattern"):
        for f in feature_cols:
            w=pd.to_numeric(g[g.side.eq("winner")][f],errors="coerce").dropna()
            s=pd.to_numeric(g[g.side.eq("stop")][f],errors="coerce").dropna()
            rows.append({
                "pattern":p,"feature":f,
                "n_win":len(w),"n_stop":len(s),
                "winner_median":w.median() if len(w) else np.nan,
                "stop_median":s.median() if len(s) else np.nan,
                "median_delta":w.median()-s.median() if len(w) and len(s) else np.nan
            })
    return pd.DataFrame(rows)

def build_pair_trajectories(pairs,hist):
    hgroups={c:g.copy() for c,g in hist.groupby("code")}
    rows=[]
    for pid,r in pairs.reset_index(drop=True).iterrows():
        for side in ["winner","stop"]:
            code=norm_code(r[f"{side}_code"])
            dt=pd.Timestamp(r[f"{side}_date"]).normalize()
            g=hgroups.get(code)
            if g is None:continue
            z=trajectory(g,dt)
            if z.empty:continue
            z=z.copy()
            z["pair_id"]=pid;z["pattern"]=r.pattern;z["side"]=side;z["code"]=code
            rows.append(z)
    return pd.concat(rows,ignore_index=True) if rows else pd.DataFrame()

def trajectory_contrast(tr):
    if tr.empty:return pd.DataFrame()
    metrics=["close_vs_d0_pct","vol20_ratio","amt20_ratio","close_loc","upper_wick",
             "ma20_dist_pct","ma60_dist_pct","ma224_dist_pct","atr_pct"]
    rows=[]
    for (p,d),g in tr.groupby(["pattern","rel_day"]):
        for m in metrics:
            w=pd.to_numeric(g[g.side.eq("winner")][m],errors="coerce").dropna()
            l=pd.to_numeric(g[g.side.eq("stop")][m],errors="coerce").dropna()
            rows.append({"pattern":p,"rel_day":d,"metric":m,"n_win":len(w),"n_stop":len(l),
                         "winner_median":w.median() if len(w) else np.nan,
                         "stop_median":l.median() if len(l) else np.nan,
                         "median_delta":w.median()-l.median() if len(w) and len(l) else np.nan})
    return pd.DataFrame(rows)

def pre_signal_slopes(tr):
    if tr.empty:return pd.DataFrame()
    rows=[]
    metrics=["vol20_ratio","amt20_ratio","close_loc","ma20_dist_pct","ma60_dist_pct","ma224_dist_pct","atr_pct"]
    for (pid,p,side),g in tr.groupby(["pair_id","pattern","side"]):
        g=g.sort_values("rel_day")
        for m in metrics:
            z=g[["rel_day",m]].dropna()
            if len(z)>=4:
                slope=float(np.polyfit(z.rel_day.astype(float),z[m].astype(float),1)[0])
                rows.append({"pair_id":pid,"pattern":p,"side":side,"metric":m,"slope_d10_to_d0":slope})
    return pd.DataFrame(rows)

def slope_contrast(slopes):
    if slopes.empty:return pd.DataFrame()
    rows=[]
    for (p,m),g in slopes.groupby(["pattern","metric"]):
        w=g[g.side.eq("winner")].slope_d10_to_d0.dropna()
        l=g[g.side.eq("stop")].slope_d10_to_d0.dropna()
        rows.append({"pattern":p,"metric":m,"n_win":len(w),"n_stop":len(l),
                     "winner_slope_median":w.median() if len(w) else np.nan,
                     "stop_slope_median":l.median() if len(l) else np.nan,
                     "slope_delta":w.median()-l.median() if len(w) and len(l) else np.nan})
    return pd.DataFrame(rows)

def context_contrast(pairs):
    rows=[]
    features=["market_ret_5d_t1","sector_peer_positive_pct","sector_peer_mean_ret_1d",
              "USDKRW_ret5_pct","VIX_ret5_pct","US10Y_ret5_pct","SOX_ret5_pct","NASDAQ_ret5_pct"]
    for p,g in pairs.groupby("pattern"):
        for f in features:
            wc="winner_"+f;lc="stop_"+f
            if wc not in g or lc not in g:continue
            w=pd.to_numeric(g[wc],errors="coerce").dropna()
            l=pd.to_numeric(g[lc],errors="coerce").dropna()
            rows.append({"pattern":p,"feature":f,"scope":"EXPLORATORY_CONTEXT" if f!="market_ret_5d_t1" else "CAUSAL_STRICT",
                         "n_win":len(w),"n_stop":len(l),
                         "winner_median":w.median() if len(w) else np.nan,
                         "stop_median":l.median() if len(l) else np.nan,
                         "median_delta":w.median()-l.median() if len(w) and len(l) else np.nan})
    return pd.DataFrame(rows)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--r105-root",default="r105_artifacts")
    ap.add_argument("--history-cache",default=".cache/closebet_structure_env_oos_r102/v49_76_research_raw_history.csv")
    ap.add_argument("--output-dir",default="reports/closebet_paired_anatomy_r106")
    ap.add_argument("--discovery-end",default=DISCOVERY_END)
    ap.add_argument("--oos-start",default=OOS_START)
    ap.add_argument("--oos-end",default=OOS_END)
    a=ap.parse_args()

    out=Path(a.output_dir);out.mkdir(parents=True,exist_ok=True)
    ep=find(a.r105_root,"event_master_with_outcomes_and_shadow_tags.csv")
    hp=Path(a.history_cache)
    if ep is None or not hp.exists():
        raise SystemExit(f"R106_INPUT_MISSING event={ep} history={hp.exists()}")

    e=read_csv(ep);h=prep_hist(read_csv(hp))
    e["code"]=e.code.map(norm_code);e["signal_date"]=pd.to_datetime(e.signal_date,errors="coerce").dt.normalize()
    de=pd.Timestamp(a.discovery_end);osd=pd.Timestamp(a.oos_start);oed=pd.Timestamp(a.oos_end)
    disc=e[e.signal_date<=de].copy()
    oos=e[(e.signal_date>=osd)&(e.signal_date<=oed)].copy()

    scales,scaledf=robust_scale(disc)
    pairs=make_pairs(oos,scales)
    tr=build_pair_trajectories(pairs,h)
    tc=trajectory_contrast(tr)
    cp=checkpoint_table(tr)
    cpc=checkpoint_contrast(cp)
    slopes=pre_signal_slopes(tr)
    sc=slope_contrast(slopes)
    cc=context_contrast(pairs)

    # pair quality tiers are descriptive only; not tuned on outcomes
    if len(pairs):
        q1=float(pairs.match_distance.quantile(.33));q2=float(pairs.match_distance.quantile(.67))
        pairs["pair_quality"]=np.where(pairs.match_distance<=q1,"CLOSE_MATCH",
                               np.where(pairs.match_distance<=q2,"MEDIUM_MATCH","LOOSE_MATCH"))
    else:
        q1=q2=np.nan

    scaledf.to_csv(out/"discovery_frozen_pairing_scales.csv",index=False,encoding="utf-8-sig")
    pairs.to_csv(out/"oos_winner_stopfirst_pair_ledger.csv",index=False,encoding="utf-8-sig")
    tr.to_csv(out/"oos_pair_pre_signal_trajectory.csv",index=False,encoding="utf-8-sig")
    tc.to_csv(out/"oos_pair_trajectory_contrast.csv",index=False,encoding="utf-8-sig")
    cp.to_csv(out/"oos_pair_checkpoints.csv",index=False,encoding="utf-8-sig")
    cpc.to_csv(out/"oos_pair_checkpoint_contrast.csv",index=False,encoding="utf-8-sig")
    slopes.to_csv(out/"oos_pair_pre_signal_slopes.csv",index=False,encoding="utf-8-sig")
    sc.to_csv(out/"oos_pair_slope_contrast.csv",index=False,encoding="utf-8-sig")
    cc.to_csv(out/"oos_pair_context_contrast.csv",index=False,encoding="utf-8-sig")

    pair_counts=(pairs.groupby(["pattern","pair_quality"],dropna=False).size().rename("pairs").reset_index()
                 if len(pairs) else pd.DataFrame())
    pair_counts.to_csv(out/"oos_pair_counts.csv",index=False,encoding="utf-8-sig")

    meta={
        "revision":REVISION,"status":"PASS","discovery_end":a.discovery_end,"oos_start":a.oos_start,"oos_end":a.oos_end,
        "oos_rows":len(oos),"pair_rows":len(pairs),"trajectory_rows":len(tr),
        "focus_patterns":sorted(FOCUS_PATTERNS),"pair_features":sorted(scales.keys()),
        "pair_quality_q33":q1,"pair_quality_q67":q2,
        "checkpoint_rows":len(cp),"checkpoint_contrast_rows":len(cpc),
        "ma_long_history_fix":True,
        "research_only":True,"production_eligible":False,"selection_logic_changed":False,
        "score_rank_changed":False,"order_logic_changed":False,"same_sample_retuning":False,
        "outcome_used_for_pair_role_only":True,
        "pair_distance_scales_frozen_from_discovery_without_outcome_optimization":True,
        "notes":[
            "Winner vs STOP_FIRST are paired within the same strategy; same core label is preferred.",
            "Pairing distance uses discovery-frozen robust scales only.",
            "D-10..D0 trajectories are reconstructed from causal OHLCV.",
            "MA20/60/112/224 are computed on up to 280 causal bars before D-10..D0 slicing.",
            "Explicit D-10/D-5/D-3/D-2/D-1/D0 checkpoint tables are emitted.",
            "External context remains descriptive; no ranking/filter is created."
        ]
    }
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")

    lines=[
        "🔬 [CLOSING BET · WINNER ↔ STOP_FIRST PAIRED ANATOMY OOS R1.0.6]",
        f"status=PASS | OOS {a.oos_start}~{a.oos_end} n={len(oos)} | pairs={len(pairs)} | trajectory rows={len(tr)}",
        f"focus={','.join(sorted(FOCUS_PATTERNS))}",
        "",
        "핵심:",
        "- 같은 패턴 내 Winner와 STOP_FIRST를 구조적으로 가장 비슷한 상대와 1:1 매칭",
        "- 매칭거리 scale은 discovery 데이터 분포에서만 고정",
        "- 신호 직전 D-10~D0의 거래량/거래대금/종가위치/윗꼬리/MA이격/ATR 변화 비교",
        "- D-10/D-5/D-3/D-2/D-1/D0 체크포인트와 D-5→D0, D-1→D0 변화량 별도 출력",
        "- MA60/MA224는 장기 causal history에서 먼저 계산 후 D-10~D0 절단",
        "- 시장·섹터·환율·VIX·금리·SOX/Nasdaq은 별도 context contrast",
        "- 검색식/점수/랭킹/후보제거/주문 변경 0",
    ]
    (out/"report.txt").write_text("\n".join(lines),encoding="utf-8")
    print("\n".join(lines))

if __name__=="__main__":
    main()
