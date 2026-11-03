"""R1-D115 external user-supplied ATR prominence / price-cluster baseline.

Independent, research-only. Source concepts retained: find_peaks(prominence=ATR),
1.5% high/low clustering and previous-20-day volume. Critical changes:
freeze the candidate on t-1, evaluate today's close against THAT candidate,
separate signal detection from experimental quality/ranking and never use
future OHLCV. No manual ticker or reference anchor enters detection.
"""
from __future__ import annotations
import hashlib
import math
import numpy as np
import pandas as pd
from scipy.signal import find_peaks

LOOKBACK=80


def raw_check(frame):
    d=frame.copy().reset_index(drop=True)
    for k in ('open','high','low','close','volume'):
        if k not in d:raise ValueError('missing '+k)
        d[k]=pd.to_numeric(d[k],errors='raise').astype(float)
    if 'date' not in d:raise ValueError('missing date')
    d['date']=pd.to_datetime(d.date)
    if d.date.duplicated().any() or not d.date.is_monotonic_increasing:
        raise ValueError('dates unsorted/duplicated')
    if ((d.high<d[['open','close','low']].max(axis=1)) | (d.low>d[['open','close','high']].min(axis=1))).any():
        raise ValueError('inconsistent OHLC')
    return d


def calc_atr(frame,n=14):
    pc=frame.close.shift(1)
    tr=pd.concat([(frame.high-frame.low).abs(),(frame.high-pc).abs(),
                  (frame.low-pc).abs()],axis=1).max(axis=1)
    return float(tr.rolling(n,min_periods=n).mean().iloc[-1])


def pivots(frame,atr,k=1.):
    if not math.isfinite(atr) or atr<=0:return np.array([],dtype=int),np.array([],dtype=int)
    hi,_=find_peaks(frame.high.to_numpy(float),prominence=atr*k)
    lo,_=find_peaks(-frame.low.to_numpy(float),prominence=atr*k)
    return hi,lo


def cluster_levels_with_anchors(prices,indices,tol=.015,min_touch=2):
    """Original mean-relative clustering, plus independently dated touch anchors.

    Does not claim volume-at-price / institutional accumulation; pivot prices only.
    Repeated touches must be separated by >=3 bars to reduce one-swing duplicates.
    """
    groups=[]
    for price,index in sorted(zip(prices,indices),key=lambda x:float(x[0])):
        p=float(price)
        if p<=0:continue
        if groups and abs(p-np.mean([r[0] for r in groups[-1]]))/p<tol:
            groups[-1].append((p,int(index)))
        else:groups.append([(p,int(index))])
    out=[]
    for rows in groups:
        selected=[]
        for p,i in sorted(rows,key=lambda x:x[1]):
            if not selected or i-selected[-1][1]>=3:selected.append((p,i))
        if len(selected)<min_touch:continue
        out.append({'level':float(np.mean([x[0] for x in rows])),
                    'touches':len(selected),
                    'anchors':tuple(i for _,i in selected),
                    'price_min':float(min(x[0] for x in rows)),
                    'price_max':float(max(x[0] for x in rows))})
    return out


def cluster_levels_original(prices,tol=.015,min_touch=2):
    """Exact user cluster logic; unlike safe version, no time independence rule."""
    groups=[]
    for p in sorted(float(v) for v in prices):
        if groups and abs(p-np.mean(groups[-1]))/p<tol:
            groups[-1].append(p)
        else:groups.append([p])
    return [(float(np.mean(g)),len(g)) for g in groups if len(g)>=min_touch]


def original_like_scan(frame,lookback=LOOKBACK,vol_mult=1.5):
    """Reference recreation of the provided code, NOT causal benchmark for trading."""
    d=frame.iloc[-lookback:].reset_index(drop=True)
    if len(d)<22:return []
    atr=calc_atr(d)
    if not math.isfinite(atr) or atr<=0:return []
    hi,lo=pivots(d,atr);t=len(d)-1;c=float(d.close.iloc[-1]);pc=float(d.close.iloc[-2]);sig=[]
    volume_ok=bool(d.volume.iloc[-1]>d.volume.iloc[-21:-1].mean()*vol_mult)
    if len(hi)>=3:
        h=hi[-4:]
        slope,b=np.polyfit(h,d.high.values[h],1)
        resid=np.abs(d.high.values[h]-(slope*h+b)).max()
        if slope<0 and resid<=atr*.5 and c>slope*t+b and pc<=slope*(t-1)+b:
            sig.append(('falling_channel_breakout',volume_ok))
    for lv,touches in cluster_levels_original(d.high.values[hi]):
        if c>lv*1.005 and pc<=lv:
            sig.append((f'box_breakout@{lv:.0f}(터치{touches})',volume_ok))
    for lv,touches in cluster_levels_original(d.low.values[lo]):
        if c<lv*.995 and pc>=lv:
            sig.append((f'box_breakdown@{lv:.0f}(터치{touches})',volume_ok))
    return sig


def candidate_snapshot(past,lookback=LOOKBACK,k=1.,tol=.015):
    """All candidates are generated strictly at the last past close (t-1)."""
    d=past.iloc[-lookback:].reset_index(drop=False)
    if len(d)<max(22,lookback//2):return [],float('nan')
    atr=calc_atr(d)
    if not math.isfinite(atr) or atr<=0:return [],atr
    hi,lo=pivots(d,atr,k)
    candidates=[]
    # distinct resistance/support zones; each remains distinct from a box
    for kind,field,indices in [('RESISTANCE','high',hi),('SUPPORT','low',lo)]:
        for zone in cluster_levels_with_anchors(d[field].to_numpy(float)[indices],indices,tol=tol):
            anchors=tuple(int(d.iloc[x]['index']) for x in zone['anchors'])
            date_ids=tuple(str(past.iloc[i].date.date()) for i in anchors)
            cid=hashlib.sha1(f'{kind}|{date_ids}'.encode()).hexdigest()[:14]
            candidates.append(dict(kind=kind,level=round(zone['level'],5),
                         touches=zone['touches'],anchor_indices=anchors,
                         anchor_dates='|'.join(date_ids),candidate_id=cid,
                         candidate_type='ATR_PIVOT_CLUSTER',
                         source='USER_SUPPLIED_BASELINE_PIT_REWORK',
                         price_min=zone['price_min'],price_max=zone['price_max']))
    if len(hi)>=3:
        h=hi[-4:]
        slope,b=np.polyfit(h,d.high.to_numpy(float)[h],1)
        resid=float(np.abs(d.high.values[h]-(slope*h+b)).max())
        if slope<0 and resid<=atr*.5:
            anchors=tuple(int(d.iloc[x]['index']) for x in h)
            date_ids=tuple(str(past.iloc[i].date.date()) for i in anchors)
            cid=hashlib.sha1(f'DESC_RESISTANCE|{date_ids}'.encode()).hexdigest()[:14]
            candidates.append(dict(kind='DESC_RESISTANCE',
                         level=round(float(slope*(len(d)-1)+b),5),
                         touches=len(h),anchor_indices=anchors,
                         anchor_dates='|'.join(date_ids),candidate_id=cid,
                         candidate_type='ATR_PIVOT_DESC_REGRESSION',
                         source='USER_SUPPLIED_BASELINE_PIT_REWORK',
                         slope=round(float(slope),8),intercept=round(float(b),8),
                         last_local_idx=len(d)-1,fit_residual_atr=round(resid/atr,4)))
    return candidates,atr


def safe_scan_at(d,t,lookback=LOOKBACK,vol_mult=1.5):
    """Event date t uses candidate frozen on close of t-1. Never reads >t."""
    if t<lookback+1: return [],[]
    past=d.iloc[:t].copy() # last close is t-1
    candidates,atr=candidate_snapshot(past,lookback)
    today=d.iloc[t];yesterday=d.iloc[t-1]
    avg=float(d.volume.iloc[t-20:t].mean())
    vr=float(today.volume/avg) if avg>0 else float('nan')
    formed_date=str(yesterday.date.date())
    date=str(today.date.date()); px=float(today.close);pc=float(yesterday.close)
    out=[]
    for z in candidates:
        val=float(z['level']); typ=z['kind']
        if typ=='DESC_RESISTANCE':
            # at t-1, level is prior endpoint; extrapolate exactly 1 trading day
            now_level=val+float(z['slope'])
            crossing=pc<=val and px>now_level
            event='DESC_RESISTANCE_BREAKOUT'
        elif typ=='RESISTANCE':
            now_level=val
            crossing=pc<=val and px>val*1.005
            event='RESISTANCE_CLUSTER_BREAKOUT'
        else:
            now_level=val
            crossing=pc>=val and px<val*.995
            event='SUPPORT_CLUSTER_BREAKDOWN'
        if crossing:
            out.append({'idx':t,'date':date,'event':event,
                        'candidate_id':z['candidate_id'],'candidate_type':z['candidate_type'],
                        'formed_asof':formed_date,'known_before_event':True,
                        'level_prior_close':val,'level_event':round(now_level,5),
                        'close_prior':pc,'close_event':px,'touches':int(z['touches']),
                        'anchor_dates':z['anchor_dates'],
                        'atr_prior':atr,'volume_vs_prior20':vr,
                        'volume_surged':bool(vr>vol_mult),
                        'body_share_pct':100*abs(today.close-today.open)/max(1e-8,float(today.high-today.low)),
                        'upper_wick_share_pct':100*(today.high-max(today.open,today.close))/max(1e-8,float(today.high-today.low)),
                        'lower_wick_share_pct':100*(min(today.open,today.close)-today.low)/max(1e-8,float(today.high-today.low)),
                        'close_vs_prior3_high_pct':100*(px/max(d.high.iloc[t-3:t])-1),
                        'direction':('UP' if typ!='SUPPORT' else 'DOWN'),
                        'warning':'DESCRIPTIVE_EVENT_NOT_TRADE_SIGNAL'})
    return out,candidates


def group_repeated_events(events,tolerance=.015,spacing=10):
    """Ex-post descriptive dedup; first event remains first. No rank score."""
    if events.empty:return events.copy(),pd.DataFrame()
    x=events.sort_values(['code','idx','event','level_event']).copy().reset_index(drop=True)
    groups=[];seen={}
    for i,r in x.iterrows():
        key=(r['code'],r['direction'],r['candidate_type'])
        last=seen.get(key)
        if last and int(r.idx)-last['idx']<=spacing and abs(float(r.level_event)/last['level']-1)<=tolerance:
            label=last['id']; last['idx']=int(r.idx);last['level']=float(r.level_event);last['n']+=1
        else:
            label=f'{r.code}_{r.direction}_{i:05d}'
            last={'id':label,'idx':int(r.idx),'level':float(r.level_event),'n':1}
            seen[key]=last
        groups.append(label)
    x['episode_id']=groups
    summary=(x.groupby('episode_id',as_index=False).agg(
      code=('code','first'),event=('event','first'),
      first_date=('date','first'),last_date=('date','last'),
      observations=('event','size'),volume_peak=('volume_vs_prior20','max')))
    return x,summary
