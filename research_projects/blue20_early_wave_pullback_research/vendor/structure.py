"""Two-sided confirmed pivot highs clustered without using trading volume."""
from __future__ import annotations
import numpy as np


def pivots(history):
    h=history.high.to_numpy()
    c=history.close.to_numpy()
    a=history.prior_atr20.to_numpy()
    output=[]
    # Three later closes must exist before D0. A pivot is observed only then.
    for i in range(2,len(history)-3):
        if not np.isfinite(a[i]) or a[i]<=0:
            continue
        if h[i]>max(h[i-2:i]) and h[i]>=max(h[i+1:i+3]):
            output.append(i)
    return output


def zones(history, n, tolerance=.5):
    w=history.iloc[-n:].reset_index(drop=True)
    if len(w)<n:
        return [],w
    points=pivots(w)
    candidates=[]
    seen=set()
    for anchor in points:
        center=w.high.iloc[anchor]
        width=tolerance*w.prior_atr20.iloc[anchor]
        low,high=center-width,center+width
        trials=[]
        for i in points:
            if abs(w.high.iloc[i]-center)>width:
                continue
            if trials and i-trials[-1]["index"]<5:
                continue
            after=w.iloc[i+1:i+4]
            # A later close through the upper boundary cancels this failed trial.
            if (after.close>high).any() or after.close.min()>=low:
                continue
            row=w.iloc[i]
            decline_atr=(row.high-after.close.min())/row.prior_atr20
            if decline_atr<.5:
                continue
            trials.append(dict(index=i,date=row.date,known_date=w.date.iloc[i+3],
                               high=row.high,close=row.close,atr=row.prior_atr20,
                               after_min_close=float(after.close.min()),
                               drop_atr=float(decline_atr),
                               drop_pct=float(100*(1-after.close.min()/row.high)),
                               close_to_min_pct=float(100*(after.close.min()/row.close-1))))
        if len(trials)<2:
            continue
        key=tuple(t["date"] for t in trials)
        if key in seen:
            continue
        seen.add(key)
        gaps=np.diff([t["index"] for t in trials])
        troughs=[float(w.low.iloc[a["index"]+1:b["index"]].min()) for a,b in zip(trials,trials[1:])]
        breaks=np.flatnonzero(w.close.to_numpy()>high)
        last_break=int(breaks[-1]) if len(breaks) else -1
        current_failures=sum(t["index"]>last_break for t in trials)
        candidates.append(dict(low=float(low),high=float(high),center=float(center),anchor=w.date.iloc[anchor],
                               count=len(trials),median_drop_atr=float(np.median([t["drop_atr"] for t in trials])),
                               median_drop_pct=float(np.median([t["drop_pct"] for t in trials])),
                               drop_change_atr=trials[-1]["drop_atr"]-trials[0]["drop_atr"],
                               median_reapproach=float(np.median(gaps)),
                               last_reapproach=int(gaps[-1]),reapproach_change=int(gaps[-1]-gaps[0]),
                               troughs=troughs,
                               rising_troughs=bool(len(troughs)>=2 and all(b>a for a,b in zip(troughs,troughs[1:]))),
                               trough_change_pct=100*(troughs[-1]/troughs[0]-1) if len(troughs)>=2 else np.nan,
                               prior_closes_above=int(len(breaks)),failures_since_last_break=current_failures,
                               failures_growing=bool(trials[-1]["drop_atr"]>trials[0]["drop_atr"]),
                               tests=trials))
    return candidates,w


def choose(candidates, bar, previous):
    if not candidates:
        return None
    # Distance to the candle body is followed by distance to the close. Tie-breaks
    # use prior test count and anchor date, never subsequent outcome.
    bottom,top=sorted((bar.open,bar.close))
    def rank(z):
        distance=max(0.,z["low"]-top,bottom-z["high"])
        return (distance,abs(bar.close-z["high"]),-z["count"],z["anchor"])
    z=min(candidates,key=rank)
    result={k:v for k,v in z.items() if k not in ("tests","troughs")}
    result["test_dates"]="|".join(t["date"] for t in z["tests"])
    result["trough_prices"]="|".join(map(str,z["troughs"]))
    if previous>z["high"]:
        state="ALREADY_CLEARED" if bar.close>z["high"] else "REENTRY_BELOW_OLD_RESISTANCE"
    elif bar.close<z["low"]:
        state="OVERHEAD"
    elif bar.close<=z["high"]:
        state="CLOSE_INSIDE"
    else:
        state="BREAK_WEAK" if (bar.close-z["high"])/bar.prior_atr20<.5 else "BREAK_STRONG"
    result["state"]=state
    result["overlap"]=max(0.,min(top,z["high"])-max(bottom,z["low"]))/(top-bottom) if top>bottom else 0.
    result["progress_atr"]=(bar.close-z["high"])/bar.prior_atr20
    return result
