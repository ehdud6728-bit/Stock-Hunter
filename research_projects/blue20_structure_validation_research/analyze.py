"""Descriptive follow-through validation of frozen 500-stock resistance zones."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

HERE=Path(__file__).resolve().parent
KEY=["code","date","band","window"]
FIELDS=["open","high","low","close","volume","amount"]


def structure_features(trials, prices):
    tests=trials.sort_values("date")
    n=len(tests)
    result=dict(tests=n,drop_shrinking=pd.NA,reapproach_faster=pd.NA,
                higher_trough=pd.NA,compression=pd.NA,drop_delta=np.nan,
                gap_delta=np.nan,trough_delta_pct=np.nan)
    if n<2:
        return result
    latest=tests.iloc[-3:]
    result["drop_shrinking"]=bool(latest.drop_atr.iloc[-1]<latest.drop_atr.iloc[0])
    result["drop_delta"]=float(latest.drop_atr.iloc[-1]-latest.drop_atr.iloc[0])
    if n<3:
        return result
    positions=prices.index[prices.date.isin(latest.date)].to_numpy()
    assert len(positions)==3
    gaps=np.diff(positions)
    troughs=[prices.low.iloc[a+1:b].min() for a,b in zip(positions,positions[1:])]
    result.update(reapproach_faster=bool(gaps[1]<gaps[0]),
                  higher_trough=bool(troughs[1]>troughs[0]),
                  gap_delta=int(gaps[1]-gaps[0]),
                  trough_delta_pct=float(100*(troughs[1]/troughs[0]-1)))
    result["compression"]=bool(result["drop_shrinking"] and result["reapproach_faster"] and result["higher_trough"])
    return result


def observation(prices,j,low,high,atr,k):
    """Completed bars through k only. Later outcomes are deliberately separate."""
    prefix=prices.iloc[j+1:j+k+1]
    support=False
    for i in range(j+1,j+k+1):
        b=prices.iloc[i]
        if (prices.close.iloc[i-1]>high and low<=b.low<=high+.25*atr
                and b.close>high and b.close>b.open):
            support=True
    return dict(close_above=bool(prefix.close.iloc[-1]>high),
                all_above=bool(prefix.close.gt(high).all()),
                support_seen=support,
                min_close_atr=float((prefix.close.min()-high)/atr))


def load_prices(manifest):
    m=pd.read_csv(manifest,dtype=str)
    assert len(m)==500 and m.code.nunique()==500
    frames={}
    for row in m.itertuples(index=False):
        p=manifest.parent/row.path.replace("\\","/")
        assert hashlib.sha256(p.read_bytes()).hexdigest()==row.sha256
        d=pd.read_csv(p,dtype={"code":str,"date":str})
        assert d.code.eq(row.code).all() and not d.date.duplicated().any() and d.date.is_monotonic_increasing
        frames[row.code]=d
    return frames


def annotate(events,trials,frames,known_flags=None):
    trial_map={key:g for key,g in trials.groupby(["code","band","window"],sort=False)}
    records=[]
    repaired=[]
    for e in events.itertuples(index=False):
        record=e._asdict()
        prices=frames[e.code]
        j=int(prices.index[prices.date.eq(e.date)][0])
        first=prices.date.iloc[max(0,j-e.window)]
        record["source_window_start"]=first
        record["has_known7"]=any(first<=date<=e.date for date in (known_flags or {}).get(e.code,[]))
        if e.state=="NO_REPEAT_ZONE":
            records.append(record)
            continue
        assert j+20<len(prices)
        assert prices.close.iloc[j]==e.d0_close
        pool=trial_map[(e.code,e.band,e.window)]
        # The prior detail artifact stored the trial date in its date column.
        # Recover event membership from the pinned event's test dates and zone.
        history=pool[pool.date.isin(e.test_dates.split("|")) &
                     np.isclose(pool.zone_low,e.low,rtol=0,atol=1e-8) &
                     np.isclose(pool.zone_high,e.high,rtol=0,atol=1e-8)]
        columns=["date","known_date","high","close","atr","after_min_close","drop_atr","drop_pct","close_to_min_pct"]
        history=history[columns].drop_duplicates().sort_values("date")
        assert history.date.is_unique and set(history.date)==set(e.test_dates.split("|"))
        assert len(history)==int(e.count)
        actual=prices.set_index("date").loc[history.date]
        np.testing.assert_allclose(actual.high,history.high,rtol=0,atol=1e-8)
        np.testing.assert_allclose(actual.close,history.close,rtol=0,atol=1e-8)
        assert history.known_date.lt(e.date).all()
        for t in history.itertuples(index=False):
            row=t._asdict()
            row["test_date"]=row.pop("date")
            repaired.append(dict(code=e.code,event_date=e.date,band=e.band,window=e.window,**row))
        values=structure_features(history,prices)
        record.update(values)
        record["d20_date"]=prices.date.iloc[j+20]
        record["session_index"]=j
        for k in (1,3,5):
            facts=observation(prices,j,e.low,e.high,e.prior_atr,k)
            record.update({f"d{k}_{name}":v for name,v in facts.items()})
            record[f"d{k}_known_date"]=prices.date.iloc[j+k]
            future=prices.close.iloc[j+k+1:j+21]
            assert len(future)==20-k
            record[f"after{k}_hold_ratio"]=float(future.gt(e.high).mean())
            record[f"after{k}_min_atr"]=float((future.min()-e.high)/e.prior_atr)
            old=record.get(f"d{k}_hold")
            assert bool(old)==facts["close_above"]
        records.append(record)
    result=pd.DataFrame(records)
    reference=result[result.window.eq(120)].sort_values(["code","date","band"])
    chosen=[]
    expiry={}
    for r in reference.itertuples():
        if r.date>expiry.get(r.code,""):
            chosen.append((r.code,r.date,r.band))
            d=frames[r.code]
            j=int(d.index[d.date.eq(r.date)][0])
            expiry[r.code]=d.date.iloc[j+20]
    result["nonoverlap"]=pd.MultiIndex.from_frame(result[KEY[:3]]).isin(pd.MultiIndex.from_tuples(chosen))
    return result,pd.DataFrame(repaired)


def tabulate(data):
    rows=[]
    features=["drop_shrinking","reapproach_faster","higher_trough","compression"]
    for scope,base in [("ALL_LONG",data[data.long_bull]),
                       ("KNOWN7_EXCLUDED",data[data.long_bull & ~data.has_known7]),
                       ("NO_RECORDED_FLAGS",data[data.long_bull & ~data.flagged_window]),
                       ("NONOVERLAP",data[data.long_bull & data.nonoverlap])]:
        base=base[base.state.ne("NO_REPEAT_ZONE")]
        for dims in (["window","band","year"],["window","band","year","state"]):
            for key,g in base.groupby(dims,dropna=False):
                for name in features:
                    for value in (True,False):
                        part=g[g[name].eq(value)]
                        if part.empty:
                            continue
                        rows.append(dict(scope=scope,strata="state" if "state" in dims else "pooled",
                                         **dict(zip(dims,key)),feature=name,value=value,n=len(part),
                                         stocks=part.code.nunique(),
                                         d1_hold_pct=100*part.d1_close_above.mean(),
                                         d3_hold_pct=100*part.d3_close_above.mean(),
                                         d5_hold_pct=100*part.d5_close_above.mean(),
                                         mean_hold20_pct=100*part.hold_ratio20.mean(),
                                         d20_hold_pct=100*part.d20_hold.mean()))
    return pd.DataFrame(rows)


def followup(data):
    rows=[]
    for scope,base in [("ALL_LONG",data[data.long_bull]),
                       ("KNOWN7_EXCLUDED",data[data.long_bull & ~data.has_known7]),
                       ("NO_RECORDED_FLAGS",data[data.long_bull & ~data.flagged_window]),
                       ("NONOVERLAP",data[data.long_bull & data.nonoverlap])]:
        base=base[base.state.ne("NO_REPEAT_ZONE")]
        for dims in (["window","band","year"],["window","band","year","state"]):
            for key,g in base.groupby(dims,dropna=False):
                for k in (1,3,5):
                    for feature in ("close_above","all_above","support_seen"):
                        for value in (True,False):
                            part=g[g[f"d{k}_{feature}"].eq(value)]
                            if part.empty:continue
                            rows.append(dict(scope=scope,strata="state" if "state" in dims else "pooled",
                                             **dict(zip(dims,key)),known_offset=k,feature=feature,value=value,
                                             n=len(part),stocks=part.code.nunique(),
                                             later_hold_pct=100*part[f"after{k}_hold_ratio"].mean(),
                                             d20_hold_pct=100*part.d20_hold.mean(),
                                             later_min_atr=part[f"after{k}_min_atr"].mean()))
    return pd.DataFrame(rows)


def cluster_interval(g,feature,outcome,seed=719,draws=1000):
    valid=g[g[feature].notna() & g[outcome].notna()].copy()
    true=valid[valid[feature].eq(True)]
    false=valid[valid[feature].eq(False)]
    if true.empty or false.empty:
        return dict(n_true=len(true),n_false=len(false),delta_pp=np.nan,lo_pp=np.nan,hi_pp=np.nan,
                    stocks=valid.code.nunique(),bootstrap_valid=0)
    codes=sorted(valid.code.unique())
    ix={c:i for i,c in enumerate(codes)}
    sums=np.zeros((len(codes),4))
    for row in valid.itertuples(index=False):
        i=ix[row.code];side=0 if getattr(row,feature) else 2
        sums[i,side]+=getattr(row,outcome)
        sums[i,side+1]+=1
    rng=np.random.default_rng(seed)
    counts=rng.multinomial(len(codes),np.full(len(codes),1/len(codes)),size=draws)
    sampled=counts@sums
    ok=(sampled[:,1]>0)&(sampled[:,3]>0)
    delta=100*(sampled[ok,0]/sampled[ok,1]-sampled[ok,2]/sampled[ok,3])
    return dict(n_true=len(true),n_false=len(false),delta_pp=100*(true[outcome].mean()-false[outcome].mean()),
                lo_pp=float(np.quantile(delta,.025)) if len(delta) else np.nan,
                hi_pp=float(np.quantile(delta,.975)) if len(delta) else np.nan,
                stocks=len(codes),bootstrap_valid=int(ok.sum()))


def intervals(data):
    rows=[]
    base=data[data.long_bull & data.window.eq(120) & data.band.eq("UPPER")]
    for scope,g in [("ALL_LONG",base),("NO_RECORDED_FLAGS",base[~base.flagged_window]),
                    ("KNOWN7_EXCLUDED",base[~base.has_known7]),
                    ("NONOVERLAP",base[base.nonoverlap])]:
        for year in (2023,2024,"ALL"):
            z=g if year=="ALL" else g[g.year.eq(year)]
            for name in ("drop_shrinking","reapproach_faster","higher_trough","compression"):
                rows.append(dict(scope=scope,year=year,feature=name,outcome="hold_ratio20",
                                 **cluster_interval(z,name,"hold_ratio20")))
            for k in (1,3,5):
                for name in ("close_above","all_above","support_seen"):
                    feature=f"d{k}_{name}";outcome=f"after{k}_hold_ratio"
                    rows.append(dict(scope=scope,year=year,feature=feature,outcome=outcome,
                                     **cluster_interval(z,feature,outcome)))
    return pd.DataFrame(rows)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--source",type=Path,required=True)
    p.add_argument("--manifest",type=Path,required=True)
    p.add_argument("--out",type=Path,required=True)
    a=p.parse_args()
    pins=json.loads((HERE/"SOURCE_SHA256.json").read_text())
    for name,digest in pins.items():
        assert hashlib.sha256((a.source/name).read_bytes()).hexdigest()==digest,name
    events=pd.read_csv(a.source/"EVENTS.csv.gz",dtype={"code":str},low_memory=False)
    trials=pd.read_csv(a.source/"PRICE_TESTS.csv.gz",dtype={"code":str,"date":str,"known_date":str})
    frames=load_prices(a.manifest)
    causes=pd.read_csv(a.source/"ANOMALY_CAUSES.csv",dtype={"code":str,"date":str})
    known_flags=causes.groupby("code").date.apply(list).to_dict()
    results,repaired=annotate(events,trials,frames,known_flags)
    a.out.mkdir(parents=True,exist_ok=True)
    results.to_csv(a.out/"EVENTS.csv.gz",index=False)
    repaired.to_csv(a.out/"EVENT_TESTS_REPAIRED.csv.gz",index=False)
    tabulate(results).to_csv(a.out/"STRUCTURE_GROUPS.csv",index=False)
    followup(results).to_csv(a.out/"FOLLOWUP_GROUPS.csv",index=False)
    intervals(results).to_csv(a.out/"CLUSTER_INTERVALS.csv",index=False)
    audit=dict(research_only=True,source_run=42354426551,stocks=len(frames),events=1804,
               feature_rows=len(results),prior_tests=len(trials),policy_optimization=False,
               price_basis="UNVERIFIED",intraday="NOT_ACQUIRED")
    (a.out/"AUDIT.json").write_text(json.dumps(audit,indent=2),encoding="utf-8")
    print(audit,flush=True)


if __name__=="__main__":
    main()
