#!/usr/bin/env python3
import argparse, json, re
from pathlib import Path
import numpy as np, pandas as pd
REVISION="EARLY_POWER_R5_1_CONTROLLED_COMPRESSION_QCUT_FIX_20261004"

def norm_code(v):
    s=re.sub(r"\D","",str(v or ""))
    return s[-6:].zfill(6) if s else ""

def load_marcap(paths):
    xs=[]
    for p in paths:
        q=pd.read_parquet(p)
        if "Date" not in q.columns:q=q.reset_index()
        xs.append(q)
    x=pd.concat(xs,ignore_index=True,sort=False)
    x["Date"]=pd.to_datetime(x["Date"],errors="coerce").dt.normalize()
    x["Code"]=x["Code"].map(norm_code)
    for c in ["Open","High","Low","Close","Volume","Amount"]:
        x[c]=pd.to_numeric(x[c],errors="coerce")
    return x.dropna(subset=["Date","Code","Open","High","Low","Close"]).sort_values(["Code","Date"]).drop_duplicates(["Code","Date"])

def feats(g):
    g=g.sort_values("Date").copy()
    c=g.Close.astype(float); v=g.Volume.fillna(0).astype(float)
    for n in [5,20,40,60,112,224]: g[f"ma{n}"]=c.rolling(n,min_periods=n).mean()
    g["bb40_width"]=c.rolling(40,min_periods=40).std()*4/g.ma40*100
    vals=pd.concat([g.ma20,g.ma60,g.ma112],axis=1)
    g["conv"]=(vals.max(axis=1)-vals.min(axis=1))/vals.max(axis=1)*100
    g["obv"]=(np.sign(c.diff()).fillna(0)*v).cumsum()
    return g.reset_index(drop=True)

def rebuild(g,d):
    idx=g.index[g.Date.eq(d)]
    if len(idx)==0:return {}
    i=int(idx[0])
    if i<25:return {}
    r=g.iloc[i]; c=float(r.Close)
    out={}
    for col,k,key in [("conv",5,"conv_change_5d"),("conv",10,"conv_change_10d"),
                      ("bb40_width",5,"bb40_change_5d"),("bb40_width",10,"bb40_change_10d")]:
        out[key]=float(r[col]-g.iloc[i-k][col]) if pd.notna(r[col]) and pd.notna(g.iloc[i-k][col]) else np.nan
    l5=g.iloc[i-4:i+1]; p15=g.iloc[i-19:i-4]
    for col,key in [("Volume","volume_last5_vs_prev15"),("Amount","amount_last5_vs_prev15")]:
        a=pd.to_numeric(l5[col],errors="coerce").mean(); b=pd.to_numeric(p15[col],errors="coerce").mean()
        out[key]=float(a/b) if pd.notna(b) and b>0 else np.nan
    w20=g.iloc[i-19:i+1]
    ch=w20.Close.diff()
    up=w20.loc[ch>0,"Volume"].sum(); dn=w20.loc[ch<0,"Volume"].sum()
    out["up_down_volume_ratio20"]=float(up/dn) if dn>0 else np.nan
    out["obv_change5"]=float(r.obv-g.iloc[i-5].obv)
    out["obv_change10"]=float(r.obv-g.iloc[i-10].obv)
    w=g.iloc[max(0,i-29):i+1].copy().reset_index(drop=True)
    li=int(w.Low.idxmin()); hi=int(w.iloc[li:].High.idxmax())
    lo=float(w.loc[li,"Low"]); hh=float(w.loc[hi,"High"])
    out["impulse_low_to_high_pct"]=(hh/lo-1)*100 if lo>0 else np.nan
    out["retrace_high_to_signal_pct"]=(hh-c)/(hh-lo)*100 if hh>lo else np.nan
    out["low_to_high_sessions"]=hi-li
    out["high_to_signal_sessions"]=(len(w)-1)-hi
    o,h,l=float(r.Open),float(r.High),float(r.Low)
    rng=max(h-l,1e-9)
    out["signal_close_loc_pct"]=(c-l)/rng*100
    out["signal_body_pct"]=(c-o)/o*100 if o>0 else np.nan
    out["signal_upper_wick_pct"]=(h-max(c,o))/o*100 if o>0 else np.nan
    return out

def qbin(s):
    s=pd.to_numeric(s,errors="coerce")
    out=pd.Series(index=s.index,dtype="object")
    ok=s.notna()
    n=int(ok.sum())
    if n<4:
        out.loc[ok]="ALL"
        return out
    # Deterministic tie handling: rank first, then quartile the ranks.
    # This prevents qcut duplicate-edge/label mismatches without using outcomes.
    ranked=s.loc[ok].rank(method="first")
    out.loc[ok]=pd.qcut(ranked,4,labels=["Q1","Q2","Q3","Q4"]).astype(str)
    return out

def outcome(r):
    p10=pd.to_numeric(pd.Series([r.get("first_plus10_day_h10")]),errors="coerce").iloc[0]
    p5=pd.to_numeric(pd.Series([r.get("first_plus5_day_h10")]),errors="coerce").iloc[0]
    m5=pd.to_numeric(pd.Series([r.get("first_minus5_day_h10")]),errors="coerce").iloc[0]
    d10=pd.to_numeric(pd.Series([r.get("d10_close_ret_pct")]),errors="coerce").iloc[0]
    if pd.notna(p10) and (pd.isna(m5) or p10<m5):return "CLEAN_PLUS10"
    if pd.notna(m5) and (pd.isna(p5) or m5<p5) and pd.notna(d10) and d10<0:return "STOP_FIRST_NEG"
    return "OTHER"

def summarize(df,keys,view):
    rows=[]
    for vals,g in df.groupby(keys,dropna=False):
        if not isinstance(vals,tuple):vals=(vals,)
        r={"view":view,"n":len(g),"signal_days":g.signal_date.nunique()}
        for k,v in zip(keys,vals):r[k]=v
        for lab in ["CLEAN_PLUS10","STOP_FIRST_NEG","OTHER"]:
            r[lab.lower()+"_pct"]=(g.outcome_group.eq(lab).mean()*100)
        for c in ["d10_close_ret_pct","mfe10_pct","mae10_pct"]:
            r[c+"_median"]=pd.to_numeric(g[c],errors="coerce").median()
        rows.append(r)
    return pd.DataFrame(rows)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--r3-ledger",required=True); ap.add_argument("--marcap",nargs="+",required=True)
    ap.add_argument("--out",default="reports/early_power_r5")
    a=ap.parse_args(); out=Path(a.out); out.mkdir(parents=True,exist_ok=True)
    led=pd.read_csv(a.r3_ledger,dtype={"code":str},low_memory=False)
    led=led[led.stage.eq("S3_CONVERGENCE")].copy()
    led["code"]=led.code.map(norm_code); led["signal_date"]=pd.to_datetime(led.signal_date).dt.normalize()
    led["outcome_group"]=led.apply(outcome,axis=1)
    px=load_marcap(a.marcap); by={c:feats(g) for c,g in px.groupby("Code",sort=False)}
    rows=[]
    for _,r in led.iterrows():
        g=by.get(r.code)
        if g is None:continue
        z=r.to_dict(); z.update(rebuild(g,r.signal_date)); rows.append(z)
    q=pd.DataFrame(rows)
    q["calendar_segment"]=np.where(q.signal_date.dt.year<=2025,"2025","2026")
    features=["conv_change_5d","conv_change_10d","bb40_change_10d","volume_last5_vs_prev15",
              "amount_last5_vs_prev15","low_to_high_sessions","high_to_signal_sessions",
              "signal_close_loc_pct","retrace_high_to_signal_pct"]
    for f in features:q[f+"_quartile"]=qbin(q[f])
    uni=pd.concat([summarize(q,[f+"_quartile"],"QUARTILE::"+f) for f in features],ignore_index=True)
    mat=summarize(q,["conv_change_5d_quartile","volume_last5_vs_prev15_quartile"],"CONV5_X_VOLUME")
    stab=pd.concat([summarize(q,["calendar_segment",f+"_quartile"],"STABILITY::"+f)
                    for f in ["conv_change_5d","volume_last5_vs_prev15","low_to_high_sessions","signal_close_loc_pct"]],
                   ignore_index=True)
    q.to_csv(out/"r5_s3_rebuilt_path_ledger.csv",index=False,encoding="utf-8-sig")
    uni.to_csv(out/"r5_univariate_shape_map.csv",index=False,encoding="utf-8-sig")
    mat.to_csv(out/"r5_conv_volume_matrix.csv",index=False,encoding="utf-8-sig")
    stab.to_csv(out/"r5_calendar_stability.csv",index=False,encoding="utf-8-sig")
    meta={"revision":REVISION,"research_only":True,"production_changed":False,
          "production_search_changed":False,"production_score_changed":False,
          "production_rank_changed":False,"production_order_changed":False,
          "automatic_orders":False,"same_sample_retuning":False,
          "population":"Frozen R3 S3_CONVERGENCE full population",
          "rows":len(q),"signal_days":int(q.signal_date.nunique()),
          "method":"Descriptive quartile shape maps; no threshold promotion."}
    (out/"meta.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")
    (out/"REPORT.txt").write_text("# EARLY POWER R5\n\n"+json.dumps(meta,ensure_ascii=False,indent=2)+"\n\n"+mat.to_string(index=False),encoding="utf-8")
    print("EARLY_POWER_R5_CONTROLLED_COMPRESSION_PASS"); print(mat.to_string(index=False))
if __name__=="__main__":main()
