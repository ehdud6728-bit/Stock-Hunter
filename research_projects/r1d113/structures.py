"""R1-D113 -- independent past-only geometry of boxes, channels and wedges.

Research-only. Distinct from R1-D112 trendline lifetime engine. Candidate
geometry is built from *confirmed* local pivots; a structural breakout is
measured against a structure frozen on a preceding close, not a pattern
retroactively constructed from the breakout candle.
"""
from __future__ import annotations
import math
from collections import defaultdict
import numpy as np
import pandas as pd

WINDOWS=(22,35,50,70)
BASES=('WICK','BODY')
MIN_HISTORY=25
MAX_HOLD=20


def _atr(frame):
    prev=frame.close.shift(1)
    tr=pd.concat([(frame.high-frame.low).abs(),(frame.high-prev).abs(),(frame.low-prev).abs()],axis=1).max(axis=1)
    return tr.rolling(14,min_periods=5).mean().fillna(tr.expanding().mean()).to_numpy(float)


def _raw(frame):
    d=frame.copy().reset_index(drop=True)
    d['date']=pd.to_datetime(d.date)
    for k in ('open','high','low','close','volume','amount'):
        if k not in d:raise ValueError('missing raw field '+k)
        d[k]=pd.to_numeric(d[k],errors='raise').astype(float)
    if not d.date.is_monotonic_increasing or d.date.duplicated().any():raise ValueError('date order')
    if ((d.high<d[['open','close','low']].max(axis=1)) |(d.low>d[['open','close','high']].min(axis=1))).any():
        raise ValueError('invalid OHLC')
    return d


def _pivots(values,start,last,radius=2,upper=True):
    """Pivots through last index; strictly require right radius to be known."""
    out=[]
    for k in range(max(start+radius,radius), last-radius+1):
        w=values[k-radius:k+radius+1]
        if len(w)!=2*radius+1:continue
        v=values[k]
        if (v==max(w) if upper else v==min(w)) and np.count_nonzero(w==v)==1:
            out.append(k)
    return out


def _touches(indices,values,m,b,atr,at,last,tol_factor=.7):
    matching=[k for k in indices if abs(values[k]-(m*k+b)) <= max(.005*at,.7*atr[k],tol_factor*atr[k])]
    independent=[]
    for k in matching:
        if not independent or k-independent[-1]>=3:independent.append(k)
    return independent


def classify(mu,ml,first_width,last_width,atr):
    """Classify slopes in ATR/bar and check structural contraction/parallelism."""
    if atr<=0 or first_width<=0 or last_width<=0:return None
    u=mu/atr;l=ml/atr
    contraction=1-last_width/first_width
    if abs(u)<=.075 and abs(l)<=.075:return 'HORIZONTAL_BOX'
    if u<=-.075 and l<=-.075 and abs(u-l)<=.16 and abs(contraction)<=.34:return 'DESCENDING_CHANNEL'
    if u>=.075 and l>=.075 and abs(u-l)<=.16 and abs(contraction)<=.34:return 'ASCENDING_CHANNEL'
    if contraction>=.20 and u<-.055 and l>.055:return 'SYMMETRICAL_TRIANGLE'
    if contraction>=.20 and u<-.08 and l<-.055 and u<l-.03:return 'FALLING_WEDGE'
    if contraction>=.20 and u>.055 and l>.08 and l>u+.03:return 'RISING_WEDGE'
    return None


def candidate_at(frame,asof,span,basis,atr=None,require_pattern=True):
    """Single pattern hypothesis at a historical as-of close; no t+1 reads."""
    d=frame
    t=int(asof);start=t-int(span)+1
    if t>=len(d) or start<0: return None
    if atr is None:atr=_atr(d)
    if not np.isfinite(atr[t]) or atr[t]<=0:return None
    o=d.open.to_numpy(float);c=d.close.to_numpy(float)
    upper=d.high.to_numpy(float) if basis=='WICK' else np.maximum(o,c)
    lower=d.low.to_numpy(float) if basis=='WICK' else np.minimum(o,c)
    hi=_pivots(upper,start,t,upper=True)
    lo=_pivots(lower,start,t,upper=False)
    if len(hi)<2 or len(lo)<2:return None
    # Regression is among prior-confirmed turning points, not arbitrary endpoints.
    mu,bu=np.polyfit(hi,upper[hi],1);ml,bl=np.polyfit(lo,lower[lo],1)
    top=float(mu*t+bu);bottom=float(ml*t+bl)
    first=float((mu*start+bu)-(ml*start+bl));last=top-bottom
    if not (top>bottom>0 and first>0):return None
    pct=100*last/max(1,float(c[t]));
    if pct<.7 or pct>30:return None
    typ=classify(mu,ml,first,last,float(atr[t]))
    if require_pattern and not typ:return None
    th=_touches(hi,upper,mu,bu,atr,float(c[t]),t)
    tl=_touches(lo,lower,ml,bl,atr,float(c[t]),t)
    if len(th)<2 or len(tl)<2:return None
    xs=np.arange(start,t+1,dtype=int);ups=mu*xs+bu;lows=ml*xs+bl
    wicks_in=np.mean((d.high.to_numpy(float)[xs]<=ups+atr[xs]*.75)&(d.low.to_numpy(float)[xs]>=lows-atr[xs]*.75))
    bodies_in=np.mean((np.maximum(o[xs],c[xs])<=ups+atr[xs]*.5)&(np.minimum(o[xs],c[xs])>=lows-atr[xs]*.5))
    if bodies_in<.62:return None
    # Prefer repeated meaningful reactions and coherent containment over
    # artificially tight line intersections; returns never enter this score.
    score=round(4*min(5,len(th)+len(tl)) + 20*bodies_in + 10*wicks_in
                - min(10,abs(mu-ml)/float(atr[t])*5) - max(0,pct-17)*.5,3)
    return {'pattern':typ,'basis':basis,'window':int(span),'asof_idx':t,
      'asof':str(d.iloc[t].date.date()),'start_idx':start,
      'upper_m':round(float(mu),7),'upper_b':round(float(bu),5),
      'lower_m':round(float(ml),7),'lower_b':round(float(bl),5),
      'upper_price':round(top,4),'lower_price':round(bottom,4),
      'channel_height_pct':round(pct,4),'top_pivot_count':len(hi),'bottom_pivot_count':len(lo),
      'upper_touch_count':len(th),'lower_touch_count':len(tl),
      'upper_anchor_1':str(d.iloc[hi[0]].date.date()),'upper_anchor_2':str(d.iloc[hi[-1]].date.date()),
      'lower_anchor_1':str(d.iloc[lo[0]].date.date()),'lower_anchor_2':str(d.iloc[lo[-1]].date.date()),
      'body_inside_fraction':round(float(bodies_in),5),'wick_inside_fraction':round(float(wicks_in),5),
      'geometry_score':score,'when_known':'AT_ASOF_CLOSE_ONLY',
      'warning':'GEOMETRY_CANDIDATE_NOT_TRADE_SIGNAL'}


def _level(rec,t,part):return float(rec[part+'_m'])*t+float(rec[part+'_b'])


def scan(d,*,windows=WINDOWS,bases=BASES):
    """Return (best snapshot/day/type, day lifecycle) with PIT frozen boundaries.

    Historical state is immutable: all forward states are emitted in separate
    rows, and the signal decision at t references the candidate at t-1 only.
    """
    d=_raw(d);atr=_atr(d);cand=[];states=[];events=[];recent={};last_active={}
    dates=d.date.dt.strftime('%Y-%m-%d').tolist()
    c=d.close.to_numpy(float);h=d.high.to_numpy(float);l=d.low.to_numpy(float)
    av=d.amount.to_numpy(float)
    for t in range(max(MIN_HISTORY,min(windows)-1),len(d)):
        # First, transitions of formations last known on t-1.
        if t>0:
            for key,old in list(recent.items()):
                k=(old['pattern'],old['basis'])
                if t-int(old['asof_idx'])>MAX_HOLD:
                    events.append({'idx':t,'date':dates[t],'pattern':old['pattern'],'basis':old['basis'],
                         'event':'STALE_ARCHIVE','formed_asof':old['asof'],'formed_idx':old['asof_idx'],
                         'level':None,'close':float(c[t]),'rule':'TIME_LIMIT'});recent.pop(key,None);continue
                up=_level(old,t,'upper');down=_level(old,t,'lower')
                prev_up=_level(old,t-1,'upper');prev_down=_level(old,t-1,'lower')
                # Crucially: require a pattern from PREVIOUS close; never back-fill.
                tol=max(.004*float(c[t]),.25*float(atr[t]))
                cross_up=(c[t]>up+tol and c[t-1]<=prev_up+max(.004*c[t-1],.25*atr[t-1]))
                cross_down=(c[t]<down-tol and c[t-1]>=prev_down-max(.004*c[t-1],.25*atr[t-1]))
                if cross_up or cross_down:
                    ev='BREAKOUT_UP' if cross_up else 'BREAKDOWN_DOWN'
                    level=up if cross_up else down
                    body=abs(float(c[t]-d.open.iloc[t]))/max(1e-8,float(h[t]-l[t]))
                    prior_high=float(max(h[max(0,t-3):t]));prior_low=float(min(l[max(0,t-3):t]))
                    amount20=float(np.mean(av[max(0,t-20):t])) if t>0 else float('nan')
                    events.append({'idx':t,'date':dates[t],'pattern':old['pattern'],'basis':old['basis'],
                      'event':ev,'formed_asof':old['asof'],'formed_idx':int(old['asof_idx']),
                      'level':round(level,4),'close':float(c[t]),'atr14':round(float(atr[t]),4),
                      'upper_m':old['upper_m'],'upper_b':old['upper_b'],
                      'lower_m':old['lower_m'],'lower_b':old['lower_b'],
                      'body_fraction':round(body,4),'upper_wick_fraction':round(float((h[t]-max(d.open.iloc[t],c[t]))/(h[t]-l[t])),4) if h[t]>l[t] else 0,
                      'lower_wick_fraction':round(float((min(d.open.iloc[t],c[t])-l[t])/(h[t]-l[t])),4) if h[t]>l[t] else 0,
                      'close_over_prior_3high':bool(c[t]>prior_high),'close_under_prior_3low':bool(c[t]<prior_low),
                      'amount_vs_prior20':round(float(av[t]/amount20),4) if amount20>0 else None,
                      'rule':'CLOSE_CROSSED_PREVIOUSLY_KNOWN_BOUNDARY'});
                    last_active[k]={'event':ev,'idx':t,'old':old,'line_level':level}
                    recent.pop(key,None)
        # Confirmed pivot generation only through t. Choose top (per pattern,
        # basis) from all window hypotheses, independently of manual references.
        best={}
        for span in windows:
            for basis in bases:
                x=candidate_at(d,t,span,basis,atr)
                if not x:continue
                key=(x['pattern'],x['basis']);y=best.get(key)
                if y is None or (x['geometry_score'],x['window'])>(y['geometry_score'],y['window']):best[key]=x
        for key,x in best.items():
            cand.append(x)
            prior=recent.get(key)
            if prior is None:
                states.append({'idx':t,'date':dates[t],'pattern':key[0],'basis':key[1],
                               'state':'FORMATION_NEW','asof_idx':t,'asof':x['asof'],
                               'upper_price':x['upper_price'],'lower_price':x['lower_price'],
                               'geometry_score':x['geometry_score'],'window':x['window']})
            else:
                # Snapshots still retain earlier configuration for monitoring;
                # candidates are freshly asof-scored but never retroactively edited.
                states.append({'idx':t,'date':dates[t],'pattern':key[0],'basis':key[1],
                               'state':'FORMATION_UPDATED','asof_idx':t,'asof':x['asof'],
                               'upper_price':x['upper_price'],'lower_price':x['lower_price'],
                               'geometry_score':x['geometry_score'],'window':x['window']})
            recent[key]=x
    return pd.DataFrame(cand),pd.DataFrame(states),pd.DataFrame(events)
