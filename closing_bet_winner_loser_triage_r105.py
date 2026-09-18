#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import argparse, json, math
from pathlib import Path
import numpy as np
import pandas as pd

REVISION="CLOSEBET_WINNER_LOSER_TRIAGE_R1051_CASEBOOK_FIX_20260918"
DISCOVERY_END="2026-08-18"
OOS_START="2026-08-19"
OOS_END="2026-09-18"

STRICT_FEATURES=[
    "wave_gain_pct","pullback_depth_pct","pullback_bars","pb_volume_vs_wave","pb_amount_vs_wave",
    "ma_cluster_delta_pct","signal_ma_cluster_width_pct","signal_vol5_vs_prev20","down_volume_share10.1",
    "restart_score","price_to_ma224_pct","entry_stock_ret_1d","entry_stock_ret_5d","entry_stock_ret_20d",
    "entry_vol20_ratio","entry_amount20_ratio","entry_ma20_dist_pct","entry_ma60_dist_pct",
    "entry_close_loc_pct","entry_upper_wick_pct","entry_range_pct","market_ret_1d_t1","market_ret_5d_t1",
    "market_ret_20d_t1","market_ma20_dist_t1","market_m5_slope5_t1",
]
EXPLORATORY_FEATURES=[
    "market_ret_1d","market_ret_5d","market_ret_20d","market_breadth","market_turnover_ratio",
    "sector_peer_mean_ret_1d","sector_peer_positive_pct","sector_peer_amount_b","sector_breadth",
    "sector_turnover_ratio","i_inst_20d_b","i_inst_60d_b","i_inst_120d_b",
    "USDKRW_ret5_pct","VIX_ret5_pct","US10Y_ret5_pct","SOX_ret5_pct","NASDAQ_ret5_pct",
    "KOSPI_ret5_pct","KOSDAQ_ret5_pct","DXY_ret5_pct","WTI_ret5_pct",
]
BIN_FEATURES=["wave_gain_pct","pullback_depth_pct","price_to_ma224_pct","entry_stock_ret_20d","marcap"]

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

def num(s):
    return pd.to_numeric(s,errors="coerce")

def norm_code(v):
    s=str(v or "").strip().upper()
    if s.endswith(".0") and s[:-2].isdigit():s=s[:-2]
    if len(s)==7 and s.startswith("A"):s=s[1:]
    return s.zfill(6) if s.isdigit() and len(s)<=6 else s

def bool_series(df,names):
    for c in names:
        if c in df:
            s=df[c]
            if s.dtype==bool:return s.fillna(False)
            return s.fillna(False).astype(str).str.lower().isin({"1","true","yes","y","on","t"})
    return pd.Series(False,index=df.index)

def coalesce_num(df,names):
    out=pd.Series(np.nan,index=df.index,dtype=float)
    for c in names:
        if c in df:
            out=out.where(out.notna(),num(df[c]))
    return out

def classify_outcome(df):
    q=df.copy()
    r1=coalesce_num(q,["ret_close_1d","ret1"])
    r3=coalesce_num(q,["ret_close_3d","ret3"])
    r5=coalesce_num(q,["ret_close_5d","evaluation_ret","ret5"])
    r10=coalesce_num(q,["ret_close_10d","ret10"])
    mfe=coalesce_num(q,["mfe","path_max_high_ret"])
    mae=coalesce_num(q,["mae","path_min_low_ret"])
    plus3=bool_series(q,["plus3_before_stop_flag","hit_plus3_hd"])
    stop=bool_series(q,["stop_first_flag"])
    pre3=coalesce_num(q,["path_pre_plus3_min_low_ret","path_first3d_min_low_ret"])

    cls=pd.Series("NO_EDGE",index=q.index,dtype=object)
    cls=cls.mask(stop,"STOP_FIRST")
    cls=cls.mask((~stop)&plus3&(pre3<=-1)&(pre3>-3),"SHAKEOUT_WIN")
    cls=cls.mask((~stop)&plus3&(r3>0)&(r3<=3),"QUICK_WIN")
    cls=cls.mask((~stop)&(mfe>=5)&(r5>0),"BIG_WIN")
    cls=cls.mask((~stop)&plus3&(r5>0),"NORMAL_WIN")
    cls=cls.mask((~stop)&(mfe>=3)&(r5<=0),"GIVEBACK")
    cls=cls.mask((~stop)&(~plus3)&(r1<0)&(r3<0)&(r5<0),"WEAK_LOSS")
    cls=cls.mask((~stop)&(~plus3)&(r5<0),"SLOW_LOSS")
    q["outcome_class"]=cls
    q["winner_group"]=cls.isin(["BIG_WIN","NORMAL_WIN","QUICK_WIN","SHAKEOUT_WIN"])
    q["loser_group"]=cls.isin(["STOP_FIRST","GIVEBACK","WEAK_LOSS","SLOW_LOSS"])
    q["_r1"]=r1;q["_r3"]=r3;q["_r5"]=r5;q["_r10"]=r10;q["_mfe"]=mfe;q["_mae"]=mae
    return q

def freeze_bins(disc):
    cuts={};rows=[]
    for f in BIN_FEATURES:
        if f not in disc:continue
        s=num(disc[f]).dropna()
        if len(s)>=30 and s.nunique()>=5:
            q33=float(s.quantile(1/3));q67=float(s.quantile(2/3))
            cuts[f]=(q33,q67);rows.append({"feature":f,"q33":q33,"q67":q67,"n":len(s)})
    return cuts,pd.DataFrame(rows)

def bucket(v,cut):
    try:x=float(v)
    except:return "UNK"
    if not math.isfinite(x):return "UNK"
    a,b=cut
    return "L" if x<a else ("M" if x<=b else "H")

def feature_contrast(df,features,scope_name):
    rows=[]
    scopes=[("ALL","ALL",df)]
    for p,g in df.groupby("primary_formula",dropna=False):
        scopes.append(("PATTERN",str(p),g))
    for (p,c),g in df.groupby(["primary_formula","core_pattern_label"],dropna=False):
        scopes.append(("PATTERN_CORE",f"{p}|{c}",g))
    for st,label,g in scopes:
        w=g[g.winner_group];l=g[g.loser_group]
        for f in features:
            if f not in g:continue
            a=num(w[f]).dropna();b=num(l[f]).dropna()
            rows.append({
                "scope_type":st,"scope":label,"feature":f,"causal_scope":scope_name,
                "n_win":len(a),"n_loss":len(b),
                "winner_median":a.median() if len(a) else np.nan,
                "loser_median":b.median() if len(b) else np.nan,
                "median_delta":a.median()-b.median() if len(a) and len(b) else np.nan,
                "winner_mean":a.mean() if len(a) else np.nan,
                "loser_mean":b.mean() if len(b) else np.nan,
                "mean_delta":a.mean()-b.mean() if len(a) and len(b) else np.nan,
            })
    return pd.DataFrame(rows)

def matched_contrast(oos,cuts):
    q=oos.copy()
    for f,c in cuts.items():
        q[f+"_bin"]=q[f].map(lambda x:bucket(x,c))
    keys=["primary_formula","core_pattern_label"]+[f+"_bin" for f in cuts]
    rows=[]
    feats=[f for f in STRICT_FEATURES+EXPLORATORY_FEATURES if f in q]
    for key,g in q.groupby(keys,dropna=False):
        w=g[g.winner_group];l=g[g.loser_group]
        if len(w)<2 or len(l)<2:continue
        base={k:str(v) for k,v in zip(keys,key if isinstance(key,tuple) else (key,))}
        for f in feats:
            a=num(w[f]).dropna();b=num(l[f]).dropna()
            if len(a)<2 or len(b)<2:continue
            rows.append({**base,"feature":f,"n_win":len(a),"n_loss":len(b),
                         "winner_median":a.median(),"loser_median":b.median(),
                         "median_delta":a.median()-b.median()})
    return pd.DataFrame(rows)

def frozen_tag_thresholds(disc):
    # Distribution-only thresholds; no outcome labels used.
    specs={}
    rows=[]
    definitions={
        "LIQUIDITY_PERSIST":"signal_vol5_vs_prev20",
        "OVEREXTENSION_20D":"entry_stock_ret_20d",
        "MA224_DISTANCE":"price_to_ma224_pct",
        "UPPER_WICK":"entry_upper_wick_pct",
        "SECTOR_BREADTH":"sector_peer_positive_pct",
        "MARKET_RET5_T1":"market_ret_5d_t1",
    }
    for tag,f in definitions.items():
        if f not in disc:continue
        s=num(disc[f]).dropna()
        if len(s)>=30:
            lo=float(s.quantile(1/3));hi=float(s.quantile(2/3))
            specs[tag]={"feature":f,"lo":lo,"hi":hi}
            rows.append({"tag":tag,"feature":f,"q33":lo,"q67":hi,"n":len(s)})
    return specs,pd.DataFrame(rows)

def apply_tags(df,specs):
    q=df.copy()
    q["TAG_STRUCTURE_OK"]=q["core_pattern_label"].eq("MATCH")
    q["TAG_MA_COMPRESSION"]=num(q.get("ma_cluster_delta_pct",np.nan)).le(0)
    q["TAG_RESTART_OK"]=num(q.get("restart_score",np.nan)).ge(0.50)
    q["TAG_SUPPLY_CONTROL"]=num(q.get("pb_volume_vs_wave",np.nan)).le(1.15) & num(q.get("down_volume_share10.1",np.nan)).le(0.55)
    q["TAG_LIQUIDITY_OK"]=False
    if "LIQUIDITY_PERSIST" in specs:
        sp=specs["LIQUIDITY_PERSIST"];x=num(q[sp["feature"]])
        q["TAG_LIQUIDITY_OK"]=x.ge(sp["lo"])
    q["TAG_OVEREXTENSION_RISK"]=False
    if "OVEREXTENSION_20D" in specs:
        sp=specs["OVEREXTENSION_20D"];q["TAG_OVEREXTENSION_RISK"]=num(q[sp["feature"]]).gt(sp["hi"])
    q["TAG_MA224_FAR_RISK"]=False
    if "MA224_DISTANCE" in specs:
        sp=specs["MA224_DISTANCE"];q["TAG_MA224_FAR_RISK"]=num(q[sp["feature"]]).abs().gt(max(abs(sp["lo"]),abs(sp["hi"])))
    q["TAG_WICK_RISK"]=False
    if "UPPER_WICK" in specs:
        sp=specs["UPPER_WICK"];q["TAG_WICK_RISK"]=num(q[sp["feature"]]).gt(sp["hi"])
    q["TAG_MARKET_OK_T1"]=False
    if "MARKET_RET5_T1" in specs:
        sp=specs["MARKET_RET5_T1"];q["TAG_MARKET_OK_T1"]=num(q[sp["feature"]]).ge(sp["lo"])
    q["TAG_SECTOR_OK_EXPLORATORY"]=False
    if "SECTOR_BREADTH" in specs:
        sp=specs["SECTOR_BREADTH"];q["TAG_SECTOR_OK_EXPLORATORY"]=num(q[sp["feature"]]).ge(sp["hi"])
    # No composite score/ranking. Just a transparent combo string.
    tagcols=[c for c in q.columns if c.startswith("TAG_")]
    q["shadow_tag_combo"]=q.apply(lambda r:"|".join(c[4:] for c in tagcols if bool(r[c])) or "NONE",axis=1)
    return q,tagcols

def tag_summary(oos,tagcols):
    rows=[]
    for p,g in oos.groupby("primary_formula",dropna=False):
        for c in tagcols:
            for val in [True,False]:
                z=g[g[c].eq(val)]
                if not len(z):continue
                rows.append({"pattern":p,"tag":c,"tag_value":val,"n":len(z),
                             "winner_rate":z.winner_group.mean()*100,
                             "loser_rate":z.loser_group.mean()*100,
                             "d5_mean":num(z["_r5"]).mean(),"d5_median":num(z["_r5"]).median(),
                             "mfe_median":num(z["_mfe"]).median(),"mae_median":num(z["_mae"]).median()})
    return pd.DataFrame(rows)

def combo_summary(oos):
    rows=[]
    for (p,cmb),g in oos.groupby(["primary_formula","shadow_tag_combo"],dropna=False):
        if len(g)<3:continue
        rows.append({"pattern":p,"shadow_tag_combo":cmb,"n":len(g),
                     "winner_rate":g.winner_group.mean()*100,"loser_rate":g.loser_group.mean()*100,
                     "d5_mean":num(g["_r5"]).mean(),"d5_median":num(g["_r5"]).median(),
                     "stop_first_rate":g.outcome_class.eq("STOP_FIRST").mean()*100,
                     "giveback_rate":g.outcome_class.eq("GIVEBACK").mean()*100})
    return pd.DataFrame(rows)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--r104-root",default="r104_artifacts")
    ap.add_argument("--output-dir",default="reports/closebet_winner_loser_triage_r105")
    ap.add_argument("--discovery-end",default=DISCOVERY_END)
    ap.add_argument("--oos-start",default=OOS_START)
    ap.add_argument("--oos-end",default=OOS_END)
    a=ap.parse_args()

    out=Path(a.output_dir);out.mkdir(parents=True,exist_ok=True)
    ep=find(a.r104_root,"independent_ohlcv_event_master.csv")
    if ep is None:raise SystemExit("R105_INPUT_MISSING independent_ohlcv_event_master.csv")
    e=read_csv(ep)
    e["code"]=e["code"].map(norm_code)
    e["signal_date"]=pd.to_datetime(e["signal_date"],errors="coerce").dt.normalize()
    if "primary_formula" not in e:e["primary_formula"]=e.get("primary_strategy",e.get("strategy","UNCLASSIFIED"))
    e=classify_outcome(e)

    de=pd.Timestamp(a.discovery_end);osd=pd.Timestamp(a.oos_start);oed=pd.Timestamp(a.oos_end)
    disc=e[e.signal_date<=de].copy()
    oos=e[(e.signal_date>=osd)&(e.signal_date<=oed)].copy()

    cuts,cutdf=freeze_bins(disc)
    tagspec,tagspecdf=frozen_tag_thresholds(disc)
    e,tagcols=apply_tags(e,tagspec)
    oos=e[(e.signal_date>=osd)&(e.signal_date<=oed)].copy()

    outcome=(oos.groupby(["primary_formula","core_pattern_label","outcome_class"],dropna=False)
             .agg(n=("code","size"),stocks=("code","nunique"),days=("signal_date","nunique"),
                  d5_mean=("_r5","mean"),d5_median=("_r5","median"),mfe_median=("_mfe","median"),mae_median=("_mae","median"))
             .reset_index())
    strict=feature_contrast(oos,STRICT_FEATURES,"CAUSAL_STRICT")
    exploratory=feature_contrast(oos,EXPLORATORY_FEATURES,"EXPLORATORY_CONTEXT")
    matched=matched_contrast(oos,cuts)
    tags=tag_summary(oos,tagcols)
    combos=combo_summary(oos)

    # Casebook: biggest clean winners/losses and same-pattern matched review targets.
    cb=oos[["signal_date","code","name","primary_formula","core_pattern_label","outcome_class","winner_group","loser_group","_r1","_r3","_r5","_r10","_mfe","_mae","shadow_tag_combo"]+
           [c for c in ["wave_gain_pct","pullback_depth_pct","pb_volume_vs_wave","ma_cluster_delta_pct","restart_score","price_to_ma224_pct",
                        "market_ret_5d_t1","sector_peer_positive_pct","sector_peer_mean_ret_1d","USDKRW_ret5_pct","VIX_ret5_pct"] if c in oos]].copy()
    cb["case_priority"]=np.where(cb.outcome_class.eq("BIG_WIN"),"TOP_WIN",
                         np.where(cb.outcome_class.eq("STOP_FIRST"),"STOP_FIRST",
                         np.where(cb.outcome_class.eq("GIVEBACK"),"GIVEBACK",
                         np.where(cb.loser_group,"LOSS","OTHER"))))
    cb=cb.sort_values(["case_priority","primary_formula","_mfe"],ascending=[True,True,False])

    # Coverage audit, especially external fields.
    cov=[]
    for f in STRICT_FEATURES+EXPLORATORY_FEATURES:
        if f in oos:
            n=int(oos[f].notna().sum());pct=n/max(1,len(oos))*100
            scope="CAUSAL_STRICT" if f in STRICT_FEATURES else "EXPLORATORY_CONTEXT"
            cov.append({"feature":f,"scope":scope,"available_rows":n,"oos_rows":len(oos),"coverage_pct":pct,
                        "usable_for_shadow":scope=="CAUSAL_STRICT" and pct>=70})
    cov=pd.DataFrame(cov)

    e.to_csv(out/"event_master_with_outcomes_and_shadow_tags.csv",index=False,encoding="utf-8-sig")
    outcome.to_csv(out/"oos_pattern_outcome_anatomy.csv",index=False,encoding="utf-8-sig")
    strict.to_csv(out/"oos_winner_loser_feature_contrast_strict.csv",index=False,encoding="utf-8-sig")
    exploratory.to_csv(out/"oos_winner_loser_feature_contrast_exploratory.csv",index=False,encoding="utf-8-sig")
    cutdf.to_csv(out/"discovery_frozen_matching_bins.csv",index=False,encoding="utf-8-sig")
    matched.to_csv(out/"oos_matched_winner_loser_contrast.csv",index=False,encoding="utf-8-sig")
    tagspecdf.to_csv(out/"discovery_frozen_shadow_tag_thresholds.csv",index=False,encoding="utf-8-sig")
    tags.to_csv(out/"oos_shadow_tag_anatomy.csv",index=False,encoding="utf-8-sig")
    combos.to_csv(out/"oos_shadow_tag_combo_anatomy.csv",index=False,encoding="utf-8-sig")
    cb.to_csv(out/"oos_manual_casebook.csv",index=False,encoding="utf-8-sig")
    cov.to_csv(out/"feature_coverage_and_causality.csv",index=False,encoding="utf-8-sig")

    meta={
        "revision":REVISION,"status":"PASS","discovery_end":a.discovery_end,"oos_start":a.oos_start,"oos_end":a.oos_end,
        "discovery_rows":len(disc),"oos_rows":len(oos),"patterns":int(oos.primary_formula.nunique()),
        "winner_rows":int(oos.winner_group.sum()),"loser_rows":int(oos.loser_group.sum()),
        "research_only":True,"production_eligible":False,"selection_logic_changed":False,
        "score_rank_changed":False,"order_logic_changed":False,"same_sample_retuning":False,
        "shadow_composite_score_created":False,"shadow_ranking_created":False,
        "strict_vs_exploratory_context_separated":True,
        "matched_contrast_enabled":True,
        "notes":[
            "Outcome classes are descriptive post-signal labels, never predictors.",
            "Shadow tag thresholds are frozen from discovery distributions only; no outcome optimization.",
            "CAUSAL_STRICT and EXPLORATORY_CONTEXT are separated.",
            "No candidate is removed or re-ranked."
        ]
    }
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")

    lines=[
        "🧬 [CLOSING BET · WINNER/LOSER ANATOMY + SHADOW TRIAGE OOS R1.0.5]",
        f"status=PASS | discovery<={a.discovery_end} n={len(disc)} | OOS {a.oos_start}~{a.oos_end} n={len(oos)}",
        f"winner={meta['winner_rows']} | loser={meta['loser_rows']} | patterns={meta['patterns']}",
        "",
        "핵심:",
        "- 평균수익률 대신 BIG_WIN/QUICK_WIN/SHAKEOUT_WIN/STOP_FIRST/GIVEBACK/WEAK_LOSS 등 결과경로를 분리",
        "- 같은 패턴 + 비슷한 구조 bin 안에서 Winner vs Loser matched contrast",
        "- CAUSAL_STRICT와 EXPLORATORY_CONTEXT를 분리",
        "- discovery 분포만으로 SHADOW 태그 경계 고정; OOS 수익으로 threshold 튜닝하지 않음",
        "- 종합점수/랭킹/후보제거 없음. 태그만 부착.",
        "",
        "RESEARCH_ONLY. LIVE 검색식·점수·랭킹·주문 변경 0."
    ]
    (out/"report.txt").write_text("\n".join(lines),encoding="utf-8")
    print("\n".join(lines))

if __name__=="__main__":
    main()
