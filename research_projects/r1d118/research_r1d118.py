#!/usr/bin/env python3
"""Stock-Hunter R1D118, strictly research-only band/turnover/trendline study.

No LIVE/SEARCH/WATCH imports or writes. Signals use information available
at the signal close; forward returns only for independent evaluation.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import math
from pathlib import Path
import sys

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
BANDS = ("BB20", "BB30", "BB40", "ENV20_10", "ENV40_10", "ENV20_20", "ENV40_20")
SCALES = ("SHORT", "MEDIUM", "LONG")
KINDS = ("UPPER", "LOWER")
COST_PCT = 0.25
MIN_WARMUP = 50


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def load_data(path, *, sample=False):
    d = pd.read_csv(path, dtype={"code": str, "date": str}, encoding="utf-8-sig")
    must = {"code", "date", "open", "high", "low", "close", "amount"}
    if not must.issubset(d.columns):
        raise ValueError(f"missing OHLCV fields {sorted(must-set(d.columns))}: {path}")
    if d.empty: raise ValueError(f"empty price file: {path}")
    d["code"] = d.code.astype(str).str.zfill(6)
    for n in ["open", "high", "low", "close", "amount"]:
        d[n] = pd.to_numeric(d[n], errors="raise")
    if not (d[["open", "high", "low", "close"]].gt(0).all().all()):
        raise ValueError("invalid non-positive OHLC")
    if not (d.high >= d[["open", "close", "low"]].max(axis=1)).all() or not (d.low <= d[["open", "close", "high"]].min(axis=1)).all():
        raise ValueError("OHLC bounds inconsistent")
    if not (d.amount >= 0).all(): raise ValueError("negative turnover")
    d = d.sort_values(["code", "date"], kind="stable")
    if d.duplicated(["code", "date"]).any(): raise ValueError("duplicate price day")
    if not d.date.str.fullmatch(r"\d{4}-\d{2}-\d{2}").all():raise ValueError("date format YYYY-MM-DD required")
    return {code: group.reset_index(drop=True) for code, group in d.groupby("code", sort=True)}


def verified_files(manifest):
    """manifest columns: code,path,sha256; SHA checks required for scale-up."""
    p = Path(manifest).resolve()
    d = pd.read_csv(p, dtype=str).fillna("")
    if not {"code", "path", "sha256"}.issubset(d.columns):raise ValueError("manifest needs code,path,sha256")
    if d.code.duplicated().any() or not d.code.str.fullmatch(r"\d{6}").all(): raise ValueError("invalid manifest code")
    files=[]
    for x in d.to_dict("records"):
        f = Path(x["path"])
        if not f.is_absolute():f=(p.parent/f).resolve()
        h=sha256(f)
        if not x["sha256"] or h.lower()!=x["sha256"].lower():raise ValueError(f"PINNED_SHA_MISMATCH {x['code']} {f}")
        files.append((x["code"],f,h))
    return files


def features(base):
    s=base.copy().reset_index(drop=True)
    for n in (7,15,20,30,40):
        m=s.close.rolling(n,min_periods=n).mean()
        s[f"MA{n}"]=m
        if n in (20,30,40):
            sd=s.close.rolling(n,min_periods=n).std(ddof=0)
            s[f"BB{n}_UPPER"]=m+2*sd
            s[f"BB{n}_LOWER"]=m-2*sd
            for pct in (10,20):
                s[f"ENV{n}_{pct}_UPPER"]=m*(1+pct/100)
                s[f"ENV{n}_{pct}_LOWER"]=m*(1-pct/100)
    s["AMOUNT_PREV20"]=s.amount.shift(1).rolling(20,min_periods=20).mean()
    s["AMOUNT_RATIO20"]=s.amount/s.AMOUNT_PREV20.replace(0,np.nan)
    return s


def trade(s, i):
    """Signal on D close; entry D+1 open; D+5 close, idealized D+1 high TP test."""
    if i is None or i+5>=len(s):return None
    ent=float(s.at[i+1,"open"])
    if not math.isfinite(ent) or ent<=0: return None
    d1=100*(float(s.at[i+1,"close"])/ent-1)-COST_PCT
    d5=100*(float(s.at[i+5,"close"])/ent-1)-COST_PCT
    tp3=int(float(s.at[i+1,"high"])>=1.03*ent)
    return {"d1_net_pct":d1, "d5_net_pct":d5,
            "tp3_high_reached":tp3, "tp3_else_d1_net_pct":2.75 if tp3 else d1,
            "entry_date":s.at[i+1,"date"],"exit_date":s.at[i+5,"date"]}


def make_event(code,kind,band,s,anchor,signal,phase,**kw):
    q=trade(s,signal)
    if q is None:return None
    return {"code":code,"family":kind,"band":band,"phase":phase,
            "anchor_idx":anchor,"anchor_date":s.at[anchor,"date"],
            "signal_idx":signal,"signal_date":s.at[signal,"date"],
            "amount_eok":float(s.at[signal,"amount"])/1e8,
            "amount_ratio20":float(s.at[signal,"AMOUNT_RATIO20"]),
            "anchor_amount_eok":float(s.at[anchor,"amount"])/1e8,
            "anchor_amount_ratio20":float(s.at[anchor,"AMOUNT_RATIO20"]),
            "amount_vs_anchor":float(s.at[signal,"amount"])/max(float(s.at[anchor,"amount"]),1),
            **kw, **q}


def upper_events(code,s):
    e=[];anch=[]
    for band in BANDS:
        b=f"{band}_UPPER"
        for t in range(MIN_WARMUP,len(s)-5):
            if float(s.at[t,"high"])<float(s.at[t,b]):continue
            if (s.iloc[t-10:t].high>=s.iloc[t-10:t][b]).any():continue
            matured=bool(t+15+5 < len(s))
            anch.append({"code":code,"band":band,"anchor_date":s.at[t,"date"],"anchor_idx":t,
                         "anchor_amount_eok":float(s.at[t,"amount"])/1e8,"anchor_amount_ratio20":float(s.at[t,"AMOUNT_RATIO20"]),
                         "future_15day_matured":matured,"eventually_pullback_label_expost":False})
            # Every first-touch is in the T0 denominator regardless of future pullback.
            a=make_event(code,"UPPER_TOUCH",band,s,t,t,"TOUCH")
            if a:e.append(a)
            p=None
            for k in range(t+2,min(t+16,len(s)-5)):
                peak=float(s.iloc[t:k+1].high.max())
                depth=100*(float(s.at[k,"close"])/peak-1)
                holds=[n for n in (7,15,20) if float(s.at[k,"low"])<=float(s.at[k,f"MA{n}"])*1.01
                       and float(s.at[k,"high"])>=float(s.at[k,f"MA{n}"])*.99
                       and float(s.at[k,"close"])>=float(s.at[k,f"MA{n}"])*.99]
                if depth<=-3 and holds:
                    p=k;break
            if p is None:continue
            anch[-1]["eventually_pullback_label_expost"]=True
            kw={"pullback_depth_pct":depth,"holds_ma":"+".join(map(str,holds)),
                "upper_slope5_pct":100*(float(s.at[p,b])/float(s.at[p-5,b])-1)}
            evt=make_event(code,"UPPER_PULLBACK",band,s,t,p,"PULLBACK",**kw)
            if evt:e.append(evt)
            # Separate new signal after confirmed rebound, never backdate to pullback.
            for j in range(p+1,min(p+4,len(s)-5)):
                if s.at[j,"close"]>s.at[j-1,"high"] and s.at[j,"close"]>s.at[j,"MA7"]:
                    q=make_event(code,"UPPER_REBOUND",band,s,t,j,"REBOUND",**kw)
                    if q:e.append(q)
                    break
    return e,anch


def lower_events(code,s):
    e=[];anch=[]
    for band in BANDS:
        b=f"{band}_LOWER"
        for t in range(MIN_WARMUP,len(s)-6):
            if float(s.at[t,"low"])>float(s.at[t,b]):continue
            if (s.iloc[t-10:t].low<=s.iloc[t-10:t][b]).any():continue
            r=None
            for j in range(t,min(t+6,len(s)-5)):
                if s.at[j,"close"]>=s.at[j,b]:r=j;break
            anch.append({"code":code,"band":band,"anchor_idx":t,"anchor_date":s.at[t,"date"],
                         "eventually_reclaim":r is not None})
            x=make_event(code,"LOWER_TOUCH",band,s,t,t,"TOUCH")
            if x:e.append(x)
            if r is not None:
                y=make_event(code,"LOWER_RECLAIM",band,s,t,r,"RECLAIM",
                             reclaim_lag=r-t)
                if y:e.append(y)
    return e,anch


def dedup_events(records,cooldown=10):
    if not records:return pd.DataFrame()
    df=pd.DataFrame(records)
    pr={x:i for i,x in enumerate(BANDS)}
    df["_prio"]=df.band.map(pr)
    df=df.sort_values(["family","code","signal_idx","_prio","anchor_idx"],kind="stable")
    df=df.drop_duplicates(["family","code","signal_date"],keep="first")
    selected=[]
    for (_,code),g in df.groupby(["family","code"],sort=True):
        last=-cooldown-1
        for row in g.to_dict("records"):
            if row["signal_idx"]-last<=cooldown:continue
            selected.append(row);last=row["signal_idx"]
    return pd.DataFrame(selected).drop(columns=["_prio"]).sort_values(["family","code","signal_date"]).reset_index(drop=True)


def _line_source(path, mode):
    if path is None:return None
    x=pd.read_csv(path,dtype={"code":str,"date":str,"line_id":str,"active_line_id":str},low_memory=False)
    for c in ("code","date","scale","kind","algorithm","idx"):
        if c not in x.columns:raise ValueError(f"trendline missing {c}")
    x.code=x.code.astype(str).str.zfill(6)
    x.scale=x.scale.astype(str).str.upper();x.kind=x.kind.astype(str).str.upper()
    if not x.scale.isin(SCALES).all() or not x.kind.isin(KINDS).all():raise ValueError("unknown scale/kind")
    mode=mode.upper()
    schema={"R112":("active_line_id","active_level","active_known_date"),
            "R116":("line_id","level","known_date"),
            "R117":("line_id","exact_level","known_date")}
    if mode not in schema:raise ValueError("mode must be R112|R116|R117")
    lid,level,known=schema[mode]
    for n in (lid,level,known):
        if n not in x.columns:raise ValueError(f"trendline {mode} missing {n}")
    if x.duplicated(["code","algorithm","scale","kind","date"]).any():raise ValueError("duplicate line state lane")
    if not x.algorithm.astype(str).str.len().gt(0).all():raise ValueError("empty algorithm")
    x["_lid"]=x[lid].fillna("").astype(str).str.strip()
    x["_level"]=pd.to_numeric(x[level],errors="coerce")
    x["_known"]=x[known].fillna("").astype(str)
    invalid=(x._lid.ne("") & (x._known.eq("")|x._known.gt(x.date)))
    if invalid.any():raise ValueError("PIT_FUTURE_LINE_LEAKAGE known_date > row date")
    if "known_idx" in x.columns:
        k=pd.to_numeric(x.known_idx,errors="coerce")
        i=pd.to_numeric(x.idx,errors="raise")
        if (x._lid.ne("") & (k.isna() | k.gt(i))).any():
            raise ValueError("PIT_FUTURE_LINE_LEAKAGE known_idx > row idx")
    if (x._lid.ne("") & (x._level.isna() | x._level.le(0))).any():
        raise ValueError("active line must have positive exact source level")
    return x


def join_line_context(event_df, price_by_code, line_state, *, algorithm="TS_BODY_WICK_PROXY"):
    """No re-derived line geometry; prices and active-line identities from original as-of states only.
    Event date key joins exact source date AND matching original zero-based idx.
    Where absent, distinguish MISSING_LINE_SOURCE from NO_ACTIVE_LINE.
    """
    if event_df.empty:return event_df
    x=event_df.copy()
    for sc in SCALES:
        for kind in KINDS:
            prefix=f"{sc}_{kind}"
            x[f"{prefix}_coverage"]="MISSING_LINE_SOURCE" if line_state is None else "MISSING_SOURCE_DATE"
            x[f"{prefix}_id"]=""
            x[f"{prefix}_level"]=np.nan
            x[f"{prefix}_gap_pct"]=np.nan
            x[f"{prefix}_touch_or_cross"]=False
    if line_state is None:return x
    # An R112/R116/R117 state is not a suitable input for new codes unless actual PIT lane exists.
    lines=line_state[line_state.algorithm.eq(algorithm)].copy()
    for code, grp in x.groupby("code"):
        s=price_by_code[code]
        ix_lookup=dict(zip(s.date.astype(str),range(len(s))))
        for event_i,ev in grp.iterrows():
            day=str(ev.signal_date);i=ix_lookup[day]
            sub=lines[(lines.code==code)&(lines.date==day)&(lines.idx.astype(int)==i)]
            for sc in SCALES:
                for kind in KINDS:
                    pref=f"{sc}_{kind}"
                    z=sub[(sub.scale==sc)&(sub.kind==kind)]
                    if z.empty:continue
                    if len(z)!=1:raise ValueError("ambiguous line lane")
                    r=z.iloc[0]
                    x.at[event_i,f"{pref}_coverage"]="VALID_SOURCE_DATE"
                    if r._lid and np.isfinite(r._level):
                        x.at[event_i,f"{pref}_id"]=r._lid
                        x.at[event_i,f"{pref}_level"]=r._level
                        close=float(s.at[i,"close"])
                        x.at[event_i,f"{pref}_gap_pct"]=100*(close/r._level-1)
                        x.at[event_i,f"{pref}_touch_or_cross"]=bool(float(s.at[i,"low"])<=r._level<=float(s.at[i,"high"]))
    return x


def join_exact_retest_event(event_df, price_by_code, role_csv, algorithm="TS_BODY_WICK_PROXY"):
    """R112 immutable broken-line ID role retest at the signal close only.

    Historical role events are future *outcomes* at dates before their event;
    only exact retest_date == signal_date and known earlier break are legal.
    This is NOT a substitute for missing original line-state coverage.
    """
    result=event_df.copy()
    for scale in SCALES:
        result[f"{scale}_retired_upper_support_retests"]=0
        result[f"{scale}_retired_upper_respected"]=0
        result[f"{scale}_retired_upper_failed"]=0
        result[f"{scale}_retired_upper_line_ids"]=""
    if role_csv is None:
        result["exact_role_event_coverage"]="MISSING_ROLE_EVENT_SOURCE"
        return result
    roles=pd.read_csv(role_csv,dtype={"code":str,"retest_date":str,"break_date":str,"broken_line_id":str},low_memory=False)
    fields={"code","algorithm","scale","broken_line_id","old_kind","new_role_candidate",
            "break_idx","break_date","retest_idx","retest_date","status"}
    if not fields.issubset(roles):raise ValueError("R112 exact role-event source schema mismatch")
    roles.code=roles.code.astype(str).str.zfill(6)
    roles=roles[(roles.algorithm==algorithm)&(roles.old_kind=="UPPER")&
      (roles.new_role_candidate=="SUPPORT")].copy()
    if (roles.break_date>=roles.retest_date).any() or (roles.break_idx>=roles.retest_idx).any():
        raise ValueError("PIT invalid broken-resistance role chronology")
    result["exact_role_event_coverage"]="SOURCE_PRESENT_EVENT_SPARSE"
    for idx,row in result.iterrows():
        code=str(row.code);date=str(row.signal_date);pi=int(row.signal_idx)
        for sc in SCALES:
            z=roles[(roles.code==code)&(roles.scale==sc)&(roles.retest_date==date)&(roles.retest_idx==pi)]
            if z.empty:continue
            ids=sorted(z.broken_line_id.unique())
            respected=int(z.status.astype(str).str.contains("RESPECTED|CONFIRMED",regex=True).sum())
            failed=int(z.status.astype(str).str.contains("FAILED",regex=False).sum())
            result.at[idx,f"{sc}_retired_upper_support_retests"]=len(ids)
            result.at[idx,f"{sc}_retired_upper_respected"]=respected
            result.at[idx,f"{sc}_retired_upper_failed"]=failed
            result.at[idx,f"{sc}_retired_upper_line_ids"]="|".join(ids)
    return result

def market_context(market_path):
    if market_path is None:return None
    m=pd.read_csv(market_path,dtype={"market":str,"date":str})
    if not {"market","date","close","amount"}.issubset(m):raise ValueError("market input needs market,date,close,amount")
    if m.duplicated(["market","date"]).any():raise ValueError("market date duplication")
    frames=[]
    for key,g in m.groupby("market"):
        g=g.sort_values("date").copy()
        g["market_ret20_pct"]=100*(g.close/g.close.shift(20)-1)
        g["market_amount_ratio20"]=g.amount/g.amount.shift(1).rolling(20).mean()
        frames.append(g[["market","date","market_ret20_pct","market_amount_ratio20"]])
    return pd.concat(frames,ignore_index=True)


def _stats(df):
    if df.empty:return {"events":0,"stocks":0,"win_rate":None,"mean_d5_net_pct":None,"median_d5_net_pct":None,"tp3_high_reached_rate":None}
    return {"events":int(len(df)),"stocks":int(df.code.nunique()),
        "win_rate":float((df.d5_net_pct>0).mean()),"mean_d5_net_pct":float(df.d5_net_pct.mean()),
        "median_d5_net_pct":float(df.d5_net_pct.median()),
        "tp3_high_reached_rate":float(df.tp3_high_reached.mean())}


def compare_rules(selected):
    records=[]
    for family,g in selected.groupby("family"):
        # Crucially apply filters AFTER dedup; never tune dedup separately per threshold.
        for floor in (0,100,300,500):
            for ratio in (0,1.0,1.2,1.5):
                v=g[(g.amount_eok>=floor)&(g.amount_ratio20>=ratio)]
                records.append({"family":family,"min_amount_eok":floor,"min_amount_ratio20":ratio,**_stats(v)})
    return pd.DataFrame(records)


def verify_line_input_coverage(events, line_source):
    """Six causal line-state lanes only; do not mix role-event availability.

    A role-event feed describes sparse realized retests and is separately audited.
    NO_ACTIVE_LINE is a valid known state if the source date was verified.
    """
    cols=[f"{scale}_{kind}_coverage" for scale in SCALES for kind in KINDS]
    missing=[c for c in cols if c not in events.columns]
    if missing:
        raise ValueError("MISSING_SIX_LANE_COVERAGE_COLUMNS: "+",".join(missing))
    return bool(line_source is not None and not events.empty
                and events[cols].eq("VALID_SOURCE_DATE").all().all())


def run(stocks,output,*,line_source=None,line_mode="R112",role_file=None,market_file=None,sector_file=None,
        sample=False,required_min_stocks=200,shard_index=0,shard_count=1,input_provenance=None,study_start=None):
    """Stages data as future-evaluation only; research cannot be marked OOS ready without full coverage."""
    if shard_count<1 or shard_index<0 or shard_index>=shard_count:raise ValueError("invalid shard index/count")
    all_codes=sorted(stocks);codes=[c for i,c in enumerate(all_codes) if i%shard_count==shard_index]
    if not codes:raise ValueError("empty shard")
    out=Path(output);out.mkdir(parents=True,exist_ok=True)
    line=_line_source(line_source,line_mode)
    records=[];anchors=[];sources={}
    for code in codes:
        s=features(stocks[code]);stocks[code]=s
        u,ua=upper_events(code,s);l,la=lower_events(code,s)
        records.extend(u+l);anchors.extend([dict(family="UPPER_TOUCH",**z)for z in ua]);anchors.extend([dict(family="LOWER_TOUCH",**z)for z in la]);sources[code]=len(s)
    selected=dedup_events(records)
    # Apply requested study period only after independent causal signals and
    # cooldowns were computed with the earlier warmup. Selection cutoff is
    # independent of all after-cutoff outcomes.
    if study_start is not None:
        if not (len(study_start)==10 and study_start[4]=='-' and study_start[7]=='-'):
            raise ValueError('study_start must be YYYY-MM-DD')
        selected=selected[selected.signal_date.astype(str)>=study_start].copy().reset_index(drop=True)
        anchors=[z for z in anchors if str(z['anchor_date'])>=study_start]
    if selected.empty:raise ValueError("no event rows in requested shard")
    selected=join_line_context(selected,stocks,line)
    selected=join_exact_retest_event(selected,stocks,role_file)
    if sector_file is not None:
        sec=pd.read_csv(sector_file,dtype={"code":str,"date":str})
        must={"code","date","sector","market"}
        if not must.issubset(sec):raise ValueError("sector PIT needs code,date,sector,market")
        if sec.duplicated(["code","date"]).any():raise ValueError("sector duplicate code date")
        selected=selected.merge(sec[["code","date","sector","market"]],left_on=["code","signal_date"],right_on=["code","date"],how="left",validate="many_to_one").drop(columns=["date"])
        if selected["market"].isna().any() or selected["sector"].isna().any():
            raise ValueError("PIT sector/market membership missing for event date: no present-day fallback")
    else:
        selected["sector"]="MISSING_PIT"
        selected["market"] = selected.code.map(lambda code:"KOSPI" if code in ("006400","071840") else "KOSDAQ") if sample else "UNVERIFIED_MARKET"
    mk=market_context(market_file)
    if mk is not None:
        if not sample and sector_file is None:raise ValueError("PIT market membership required for full-market index join")
        selected=selected.merge(mk,left_on=["market","signal_date"],right_on=["market","date"],how="left",validate="many_to_one").drop(columns=["date"])
        if not sample and selected.market_ret20_pct.isna().any():raise ValueError("market date missing or insufficient warmup")
    else:
        selected["market_ret20_pct"]=np.nan;selected["market_amount_ratio20"]=np.nan
    selected["discovery_or_holdout"]=np.where(selected.signal_date.astype(str)<"2024-01-01","EXPLORATORY_PRE2024","HOLDOUT_2024PLUS_NOT_PREDECLARED")
    selected.to_csv(out/"R118_EVENT_LEDGER_PIT.csv",index=False,encoding="utf-8-sig")
    pd.DataFrame(anchors).to_csv(out/"R118_ALL_ANCHOR_DENOMINATORS.csv",index=False,encoding="utf-8-sig")
    rules=compare_rules(selected);rules.to_csv(out/"R118_THRESHOLD_MATRIX.csv",index=False,encoding="utf-8-sig")
    code_stats=[{"family":fam,"code":c,**_stats(g)} for (fam,c),g in selected.groupby(["family","code"])]
    pd.DataFrame(code_stats).to_csv(out/"R118_STOCK_CONCENTRATION.csv",index=False,encoding="utf-8-sig")
    # A placeholder sector tag must never be represented as verified PIT
    # industry classification (historical Market membership is still usable).
    sector_classified=bool(sector_file is not None and selected.sector.notna().all()
            and not selected.sector.astype(str).str.startswith(('UNVERIFIED_','MISSING_','UNKNOWN_')).any())
    # Check ONLY the six actual R111/R112/R116/R117 trendline-state lanes.
    # exact_role_event_coverage is a different event stream and may legitimately
    # be SOURCE_PRESENT_EVENT_SPARSE when the six trendline dates are all valid.
    complete=verify_line_input_coverage(selected,line)
    readiness={"version":"R1D118_RESEARCH_ONLY","study_stocks":len(codes),"total_input_stocks":len(all_codes),
      "raw_bars_by_code":sources,"sample":bool(sample),"shard_index":shard_index,"shard_count":shard_count,
      "target_stocks":required_min_stocks,"large_universe_met":len(all_codes)>=required_min_stocks and shard_count==1,
      "all_3scale_line_coverage":complete,"market_connected":mk is not None,
      "sector_pit_connected":sector_classified,"historical_market_membership_connected":sector_file is not None,
      "study_start":study_start,"upstream_source":input_provenance or {},
      "decision":"RESEARCH_ONLY_NOT_PRODUCTION_READY",
      "blocked_reasons":[],"event_counts":{str(k):int(v) for k,v in selected.family.value_counts().items()}}
    if len(all_codes)<required_min_stocks:readiness["blocked_reasons"].append("UNIVERSE_BELOW_TARGET")
    if shard_count>1:readiness["blocked_reasons"].append("SHARD_NOT_GLOBAL_MERGE")
    if not complete:readiness["blocked_reasons"].append("TRENDLINE_PIT_COVERAGE_MISSING")
    if role_file is None:readiness["blocked_reasons"].append("R112_EXACT_RETEST_EVENT_SOURCE_MISSING")
    if mk is None:readiness["blocked_reasons"].append("MARKET_DATA_MISSING")
    if not sector_classified:readiness["blocked_reasons"].append("SECTOR_CLASSIFICATION_UNVERIFIED_OR_MISSING")
    if sample:readiness["blocked_reasons"].append("SMOKE_SAMPLE_ONLY")
    (out/"R118_READINESS_AUDIT.json").write_text(json.dumps(readiness,ensure_ascii=False,indent=2),encoding="utf-8")
    return readiness


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    g=p.add_mutually_exclusive_group(required=True)
    g.add_argument("--manifest",help="Pinned code,path,sha256 CSV for 200-500 stock data")
    g.add_argument("--smoke",action="store_true",help="Bundled 3-stock R109 partial OHLCV only")
    p.add_argument("--out",default=str(HERE/"results"))
    p.add_argument("--line-states",help="Real R112/R116/R117 PIT active-line state CSV, no synthetic MA substitutes")
    p.add_argument("--line-mode",choices=["R112","R116","R117"],default="R112")
    p.add_argument("--role-events",help="Optional exact-ID R112 broken-line role retest events (not future features)")
    p.add_argument("--market-daily",help="Actual PIT index market,date,close,amount")
    p.add_argument("--sector-pit",help="PIT code,date,sector and optional market metadata")
    p.add_argument("--shard-index",type=int,default=0)
    p.add_argument("--shard-count",type=int,default=1)
    p.add_argument("--target-stocks",type=int,default=200)
    p.add_argument("--study-start",help="Only assess signals on/after date; retain earlier causal warmup and dedup")
    args=p.parse_args(argv)
    provenance={}
    if args.smoke:
        fname=HERE/"sample"/"R109_3STOCK_PARTIAL_NOT_FULL.csv"
        stocks=load_data(fname,sample=True)
        provenance={"smoke_file":str(fname.name),"sha256":sha256(fname)}
    else:
        files=verified_files(args.manifest)
        stocks={}
        for code,f,h in files:
            r=load_data(f)
            if len(r)!=1 or code not in r:raise ValueError("manifest code/data mismatch "+code)
            stocks.update(r)
            provenance[code]={"file":str(f),"sha256":h}
    result=run(stocks,args.out,line_source=args.line_states,line_mode=args.line_mode,role_file=args.role_events,market_file=args.market_daily,sector_file=args.sector_pit,
               sample=args.smoke,required_min_stocks=args.target_stocks,shard_index=args.shard_index,shard_count=args.shard_count,input_provenance=provenance,study_start=args.study_start)
    print(json.dumps(result,ensure_ascii=False,indent=2))
    return 0

if __name__=="__main__":sys.exit(main())
