"""Fixed 500-stock descriptive study, no entry policy optimization."""
from __future__ import annotations
import json,hashlib,sys,os
from pathlib import Path

import numpy as np
import pandas as pd

HERE=Path(__file__).resolve().parent
RESEARCH=Path(os.environ.get("BLUE20_RESEARCH_ROOT",HERE.parent))
sys.path[:0]=[str(RESEARCH/"blue20_repeated_resistance_research"),str(RESEARCH/"blue20_profile_cloud_avwap_research")]
from inputs import rebuild,LEDGER
from research import CASES,identity_digest
from study import volume_profile
from structure import zones,choose
from interaction_charts import gallery

OUT=HERE/"results"


def run():
    OUT.mkdir(exist_ok=True)
    frames,source,frozen,asof=rebuild()
    ledger=pd.read_csv(LEDGER,dtype={"code":str},low_memory=False)
    assert len(ledger)==1804 and identity_digest(ledger)==frozen["identity_sha256"]
    flags=pd.read_csv(HERE/"reference/AMOUNT_SCOPE_FLAGS.csv",dtype={"code":str})
    basis=pd.read_csv(HERE/"reference/REFERENCE_PRICE_FLAGS.csv",dtype={"code":str})
    rows=[]; trials=[]; case_sensitivity=[]; charts=[]
    for code,group in ledger.groupby("code"):
        d=frames[code]
        index={date:i for i,date in enumerate(d.date)}
        raw=source[code]
        raw_ref=raw.Close-raw.Changes
        prior=raw.Close.shift()
        basis_dates=set(raw.loc[prior.notna() & raw_ref.ne(prior),"date"])
        amt=d.amount/d.volume.where(d.volume.gt(0))
        amount_dates=set(d.loc[amt.lt(d.low)|amt.gt(d.high),"date"])
        d["amount_ratio20"]=d.amount/d.amount.rolling(20).mean().shift().replace(0,np.nan)
        for e in group.itertuples(index=False):
            j=index[e.date]
            bar=d.iloc[j]
            history=d.iloc[:j]
            case=(code,e.date) in CASES and e.band=="UPPER"
            for n in (60,120,240):
                candidates,w=zones(history,n)
                selected=choose(candidates,bar,d.close.iloc[j-1])
                identity=dict(code=code,date=e.date,band=e.band,year=int(e.year),window=n)
                record=dict(**identity,d0_close=bar.close,d0_open=bar.open,prior_atr=bar.prior_atr20,
                            long_bull=bar.close-bar.open>=2*bar.prior_atr20,
                            amount_ratio20=bar.amount_ratio20,
                            upper_wick=(bar.high-max(bar.open,bar.close))/(bar.high-bar.low)
                            if bar.high>bar.low else 0.,candidate_zones=len(candidates),
                            basis_flags=sum(date in basis_dates for date in w.date),
                            amount_flags=sum(date in amount_dates for date in w.date),
                            observed_bars=len(w))
                profile=volume_profile(w) if len(w)==n else None
                if profile:
                    i=int(profile["weights"].argmax())
                    record.update(poc=profile["poc"],vah=profile["vah"],val=profile["val"],
                                  density_low=profile["centers"][i]-profile["width"]/2,
                                  density_high=profile["centers"][i]+profile["width"]/2,
                                  density_mass_pct=100*profile["weights"][i]/sum(profile["weights"]))
                if selected:
                    record.update(selected)
                    record["progress_efficiency"]=selected["progress_atr"]/bar.amount_ratio20 if bar.amount_ratio20>0 else np.nan
                    record["positive_efficiency"]=max(0.,selected["progress_atr"])/bar.amount_ratio20 if bar.amount_ratio20>0 else np.nan
                    record["density_overlap"]=max(0.,min(selected["high"],record["density_high"])-max(selected["low"],record["density_low"]))/(selected["high"]-selected["low"])
                    record["zone_vs_vah_atr"]=(selected["high"]-record["vah"])/bar.prior_atr20
                    for method in ("range","typical"):
                        p=profile if method=="range" else volume_profile(w,method=method)
                        edges=np.r_[p["centers"]-p["width"]/2,p["centers"][-1]+p["width"]/2]
                        overlap=np.maximum(0.,np.minimum(edges[1:],selected["high"])-np.maximum(edges[:-1],selected["low"]))/p["width"]
                        record["zone_volume_"+method+"_pct"]=100*float(overlap@p["weights"])/sum(p["weights"])
                    active=[z for z in candidates if z["high"]>=bar.close]
                    if active:
                        nearest=min(active,key=lambda z:abs(z["high"]-bar.close))
                        record.update(overhead_low=nearest["low"],overhead_high=nearest["high"],overhead_tests=nearest["count"])
                    end=min(j+21,len(d))
                    future=d.iloc[j+1:end]
                    record["followup_bars"]=len(future)
                    record["hold_ratio20"]=float((future.close>selected["high"]).mean()) if len(future) else np.nan
                    record["min_close_progress20_atr"]=(future.close.min()-selected["high"])/bar.prior_atr20
                    for offset in (1,2,3,5,10,20):
                        record[f"d{offset}_hold"]=bool(d.close.iloc[j+offset]>selected["high"]) if j+offset<len(d) else None
                        record[f"d{offset}_progress_atr"]=(d.close.iloc[j+offset]-selected["high"])/bar.prior_atr20 if j+offset<len(d) else np.nan
                    chosen=min(candidates,key=lambda z:(max(0,z["low"]-max(bar.open,bar.close),min(bar.open,bar.close)-z["high"]),
                                                       abs(bar.close-z["high"]),-z["count"],z["anchor"]))
                    for t in chosen["tests"]:
                        assert t["known_date"]<e.date
                        trials.append({**identity,**t, "zone_low":selected["low"],"zone_high":selected["high"]})
                else:
                    record["state"]="NO_REPEAT_ZONE"
                rows.append(record)
                if case:
                    for tolerance in (.25,.5,.75):
                        z,_=zones(history,n,tolerance)
                        c=choose(z,bar,d.close.iloc[j-1])
                        case_sensitivity.append(dict(**identity,tolerance=tolerance,
                                                     state=c["state"] if c else "NO_REPEAT_ZONE",
                                                     count=c["count"] if c else 0,
                                                     low=c["low"] if c else np.nan,high=c["high"] if c else np.nan))
                    if n==120:
                        charts.append((CASES[(code,e.date)],d.iloc[j-120:j+21].copy(),record,profile))
    result=pd.DataFrame(rows)
    assert len(result)==5412
    result["flagged_window"]=result.basis_flags.gt(0)|result.amount_flags.gt(0)
    result.to_csv(OUT/"EVENTS.csv.gz",index=False)
    pd.DataFrame(trials).to_csv(OUT/"PRICE_TESTS.csv.gz",index=False)
    pd.DataFrame(case_sensitivity).to_csv(OUT/"CASE_TOLERANCE_SENSITIVITY.csv",index=False)
    result[result.set_index(["code","date"]).index.isin(CASES) & result.band.eq("UPPER")].to_csv(OUT/"CASES.csv",index=False,encoding="utf-8-sig")
    parts=[]
    for scope,sub in (("ALL_CANDLES",result),("LONG_BULL",result[result.long_bull]),
                       ("LONG_BULL_NO_KNOWN_FLAGS",result[result.long_bull & ~result.flagged_window])):
        for keys,g in sub.groupby(["window","band","year","state"],dropna=False):
            parts.append(dict(scope=scope,window=keys[0],band=keys[1],year=keys[2],state=keys[3],
                              n=len(g),median_tests=g["count"].median() if "count" in g else np.nan,
                              median_drop_atr=g.median_drop_atr.median(),median_reapproach=g.median_reapproach.median(),
                              rising_troughs=int(g.rising_troughs.eq(True).sum()),
                              mean_amount_ratio=g.amount_ratio20.mean(),mean_efficiency=g.progress_efficiency.mean(),
                              mean_wick=g.upper_wick.mean(),mean_hold_ratio20=g.hold_ratio20.mean(),
                              d1_hold_pct=100*g.d1_hold.mean(),d5_hold_pct=100*g.d5_hold.mean(),
                              d20_hold_pct=100*g.d20_hold.mean(),flagged_windows=int(g.flagged_window.sum())))
    pd.DataFrame(parts).to_csv(OUT/"GROUPS.csv",index=False)
    gallery(charts,OUT/"GALLERY_KO.html")
    audit=dict(research_only=True,stocks=len(frames),events=len(ledger),feature_rows=len(result),
               test_rows=len(trials),cutoff=asof,identity_sha256=frozen["identity_sha256"],
               volume="DAILY_ESTIMATE",price_basis="UNVERIFIED",intraday="NOT_ACQUIRED",
               trading_policy="FROZEN",full_market="HELD_PENDING_INPUT_ASSURANCE",
               protocol_sha256=hashlib.sha256((HERE/"PROTOCOL_KO.md").read_bytes()).hexdigest())
    (OUT/"AUDIT.json").write_text(json.dumps(audit,indent=2),encoding="utf-8")
    print(audit,flush=True)


if __name__=="__main__":
    run()
