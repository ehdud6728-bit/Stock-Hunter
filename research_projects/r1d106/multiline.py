"""R1-D106 research-only immutable multi-anchor lines, PIT triangle and follow-through.

No look-ahead: candle anchor known at its close, pivot anchor at j+2 close.
Every candidate stays in catalog. The live diagnostic subset is not deletion.
Signal events and outcome labels live in DIFFERENT outputs.
"""
from __future__ import annotations
from collections import defaultdict
from itertools import combinations
import math
import numpy as np
import pandas as pd

BULL='BULL_BODY_TOP'
BEAR='BEAR_BODY_BOTTOM'
HIGH='WICK_HIGH_PIVOT2'
LOW='WICK_LOW_PIVOT2'
BULL_LOW='BULL_BODY_BOTTOM'
BEAR_TOP='BEAR_BODY_TOP'
FAMILIES=(BULL,BEAR,HIGH,LOW,BULL_LOW,BEAR_TOP)
UPPER_FAMILIES=(BULL,HIGH,BEAR_TOP)
LOWER_FAMILIES=(LOW,BEAR,BULL_LOW)
RAW_FIELDS=('open','high','low','close','volume','amount')


def validate(fr):
    if not {'date',*RAW_FIELDS}.issubset(fr):raise ValueError('missing OHLCV')
    if fr.empty:raise ValueError('empty OHLCV')
    dates=pd.to_datetime(fr.date)
    if not dates.is_monotonic_increasing or dates.duplicated().any():raise ValueError('dates must be sorted/unique')
    if not np.isfinite(fr[list(RAW_FIELDS)].to_numpy(float)).all():raise ValueError('nonfinite OHLCV')
    if (fr[list(RAW_FIELDS[:4])].to_numpy(float)<=0).any():raise ValueError('nonpositive prices')
    if ((fr.high<fr[['open','close','low']].max(axis=1)) | (fr.low>fr[['open','close','high']].min(axis=1))).any():
        raise ValueError('invalid bar OHLC')
    if (fr.volume<0).any() or (fr.amount<0).any():raise ValueError('negative volume')


def _pivots(fr,kind,r=2):
    val=fr.high.to_numpy(float) if kind==HIGH else fr.low.to_numpy(float)
    out=[]
    for j in range(r,len(val)-r):
        chunk=val[j-r:j+r+1]
        if kind==HIGH:yes=val[j]==chunk.max()
        else:yes=val[j]==chunk.min()
        if yes and np.count_nonzero(chunk==val[j])==1:
            out.append({'idx':j,'date':str(fr.iloc[j].date.date()),'value':val[j],
                        'first_known_idx':j+r,'basis':'HIGH' if kind==HIGH else 'LOW',
                        'family':kind,'strength':1.0})
    return out


def anchors(fr,body_ratio=.25,close_location=.64):
    """BODY anchors need real candle body and favorable close location, not future pivots."""
    validate(fr)
    out=[]
    for j,v in enumerate(fr.itertuples()):
        hi,lo,o,c=float(v.high),float(v.low),float(v.open),float(v.close)
        r=hi-lo
        if r<=0:continue
        body=abs(c-o)/r;pos=(c-lo)/r
        if c>o and body>=body_ratio and pos>=close_location:
            for kind,value,basis in ((BULL,c,'BODY_TOP_CLOSE'),(BULL_LOW,o,'BODY_BOTTOM_OPEN')):
                out.append({'idx':j,'date':str(v.date.date()),'value':value,
                            'first_known_idx':j,'basis':basis,'family':kind,
                            'strength':round(body*(.5+.5*pos),5)})
        if c<o and body>=body_ratio and pos<=1-close_location:
            for kind,value,basis in ((BEAR,c,'BODY_BOTTOM_CLOSE'),(BEAR_TOP,o,'BODY_TOP_OPEN')):
                out.append({'idx':j,'date':str(v.date.date()),'value':value,
                            'first_known_idx':j,'basis':basis,'family':kind,
                            'strength':round(body*(1-.5*pos),5)})
    out+=_pivots(fr,HIGH)+_pivots(fr,LOW)
    return pd.DataFrame(out).sort_values(['first_known_idx','family','idx']).reset_index(drop=True)


def line_value(l,t):
    return float(l['v2'])+float(l['slope_krw_per_bar'])*(int(t)-int(l['i2']))


def make_lines(fr,*,gap_min=3,gap_max=25):
    """All meaningful same-family anchor pairs; no future labels, no ranking or expiry.

    Intervening wick penetrations counted *only up through second anchor's first-known day*.
    Does not throw away invalid-looking lines: separate evidence records reliability.
    """
    if not 1<=gap_min<=gap_max:raise ValueError('bad gap')
    a=anchors(fr)
    lines=[]
    for fam,g in a.groupby('family'):
        rec=g.sort_values('idx').to_dict('records')
        for i,b in enumerate(rec):
            for p in reversed(rec[:i]):
                gap=b['idx']-p['idx']
                if gap>gap_max:break
                if gap<gap_min:continue
                k=int(max(p['first_known_idx'],b['first_known_idx']))
                if k>=len(fr):continue
                slope=(b['value']-p['value'])/gap
                lid=f'{fam}|{p["date"]}|{b["date"]}|{p["basis"]}'
                up=fam in UPPER_FAMILIES
                # prior intervals and bars up to line confirmation only; optional evidence
                if b['idx']>p['idx']+1:
                    xs=np.arange(p['idx']+1,b['idx']);level=p['value']+slope*(xs-p['idx'])
                    highs=fr.high.to_numpy(float)[xs]; lows=fr.low.to_numpy(float)[xs]
                    penetration=int(((highs>level*1.012) if up else (lows<level*.988)).sum())
                else:penetration=0
                lines.append({'line_id':lid,'family':fam,'kind':'UPPER' if up else 'LOWER',
                    'a1':p['date'],'a2':b['date'],'i1':int(p['idx']),'i2':int(b['idx']),
                    'v1':float(p['value']),'v2':float(b['value']),
                    'known_idx':k,'known_date':str(fr.iloc[k].date.date()),
                    'slope_krw_per_bar':round(float(slope),6),
                    'slope_pct_per_bar':round(100*float(slope)/float(p['value']),5),
                    'anchor_strength':round(min(float(p['strength']),float(b['strength'])),5),
                    'penetrations_1p2pct':penetration,
                    'anchor_confirmation_type':'CLOSE_SAME_DAY' if 'PIVOT' not in fam else 'RIGHT_TWO_BARS',
                    'status':'ARCHIVE_FOREVER_NO_EXPIRY'})
    cols=['line_id','family','kind','a1','a2','i1','i2','v1','v2','known_idx','known_date',
          'slope_krw_per_bar','slope_pct_per_bar','anchor_strength','penetrations_1p2pct',
          'anchor_confirmation_type','status']
    return pd.DataFrame(lines,columns=cols).sort_values(['known_idx','kind','family','i2','i1']).reset_index(drop=True)


def price_crosses(fr,lines,*,scan_age=40):
    """Emits first line cross using only a line known by previous bar close.
    A line may have many crosses over time; none are retroactively deleted.
    raw crossing and >=0.4% confirmed-close are distinct diagnostics.
    """
    events=[]
    if lines.empty:return pd.DataFrame(columns=['line_id','idx','date','event','pct_beyond','close','line_price','known_idx'])
    by_known=defaultdict(list)
    for l in lines.to_dict('records'):by_known[int(l['known_idx'])].append(l)
    active=[];close=fr.close.to_numpy(float)
    for t in range(1,len(fr)):
        active+=by_known.get(t-1,[])
        for l in active:
            if t-int(l['i2'])>scan_age:continue # scanning scope; historical line remains in catalog
            prev=line_value(l,t-1);now=line_value(l,t)
            if prev<=0 or now<=0:continue
            a=close[t-1]-prev;b=close[t]-now
            ev='CROSS_UP' if a<=0 and b>0 else 'CROSS_DOWN' if a>=0 and b<0 else ''
            if ev:
                events.append({'line_id':l['line_id'],'family':l['family'],'kind':l['kind'],
                  'idx':t,'date':str(fr.iloc[t].date.date()),'event':ev,
                  'pct_beyond':round(100*b/now,4),'close':close[t],
                  'line_price':round(now,3),'known_idx':l['known_idx'],
                  'strong_close_0p4pct':bool(abs(100*b/now)>=.4),
                  'slope_pct_per_bar':l['slope_pct_per_bar'],
                  'anchor1':l['a1'],'anchor2':l['a2']})
    return pd.DataFrame(events)


def triangles(fr,lines,*,max_pair_age=28,limit_per_date=30):
    """No hindsight wedges. Each upper/lower candidate is known at *both* confirmations.
    Candidate geometry only; don't label a post-breakout formation a pre-breakout signal.
    """
    if limit_per_date<1:raise ValueError('limit_per_date')
    out=[]
    if lines.empty:return pd.DataFrame()
    rec=lines.to_dict('records')
    # Main focus: body-top/wick-pivot upper and confirmed wick-pivot lower.
    upper=[l for l in rec if l['family'] in (BULL,HIGH)]
    lower=[l for l in rec if l['family']==LOW]
    for u in upper:
        for d in lower:
            known=max(u['known_idx'],d['known_idx']);last_anchor=max(u['i2'],d['i2'])
            if known>=len(fr) or abs(u['i2']-d['i2'])>max_pair_age:continue
            if known-last_anchor>max_pair_age:continue
            # At the first day BOTH are known, inspect the triangle's geometry.
            left=max(u['i2'],d['i2']); width_start=line_value(u,left)-line_value(d,left)
            top=line_value(u,known);bot=line_value(d,known);width=top-bot
            if not (bot>0 and top>bot and width_start>0):continue
            closing=(u['slope_krw_per_bar']-d['slope_krw_per_bar'])<0
            if not closing:continue
            height=100*width/fr.close.iloc[known]
            if height>30:continue
            # limit long-overlapping geometry unrelated to recent price.
            price=fr.close.iloc[known]
            if price<bot*.86 or price>top*1.14:continue
            slope_difference=u['slope_krw_per_bar']-d['slope_krw_per_bar']
            apex_idx=known + width / (-slope_difference)  # geometry only; price after known NOT read
            out.append({'date':str(fr.iloc[known].date.date()),'idx':known,
               'apex_index_estimate':round(apex_idx,3),
               'bars_until_apex':round(apex_idx-known,3),
               'upper_id':u['line_id'],'lower_id':d['line_id'],
               'upper_family':u['family'],'lower_family':d['family'],
               'upper_a1':u['a1'],'upper_a2':u['a2'],
               'lower_a1':d['a1'],'lower_a2':d['a2'],
               'upper_known_date':u['known_date'],'lower_known_date':d['known_date'],
               'top':round(top,3),'bottom':round(bot,3),
               'width_pct_price':round(float(height),4),
               'width_change_krw_per_bar':round(u['slope_krw_per_bar']-d['slope_krw_per_bar'],4),
               'upper_slope':u['slope_krw_per_bar'],'lower_slope':d['slope_krw_per_bar'],
               'price_position_at_first_known':'ABOVE_TOP' if price>top else 'BELOW_BOTTOM' if price<bot else 'INSIDE',
               'shape':'DESCENDING_TRIANGLE_LIKE' if abs(d['slope_krw_per_bar'])<abs(u['slope_krw_per_bar'])*.15 else 'DESCENDING_WEDGE_LIKE' if d['slope_krw_per_bar']<0 else 'CONVERGING',
               'signal_readiness':'NO_PRIOR_TRIANGLE_BREAKOUT_SIGNAL' if price>top or price<bot else 'INSIDE_AT_FIRST_JOINT_CONFIRMATION',
               'warning':'GEOMETRY_CANDIDATE_NOT_CONFIRMED_BREAKOUT'})
    out=pd.DataFrame(out)
    if out.empty:return out
    # Keep all triangles in catalog (no winner selection); no suppression by outcome.
    return out.sort_values(['idx','upper_a2','lower_a2','upper_id','lower_id']).reset_index(drop=True)


def followthrough(fr,events,lines,horizon=(1,3,5,10,20)):
    """Ex-post evaluation separately; frozen historical line extrapolated, never re-anchored.
    Only usable historical event bars are kept with explicit censoring.
    """
    if events.empty:return pd.DataFrame()
    lmap={l['line_id']:l for l in lines.to_dict('records')}
    out=[]
    for ev in events.to_dict('records'):
        t=int(ev['idx']);l=lmap[ev['line_id']]
        rec={k:ev[k] for k in ('line_id','idx','date','event','pct_beyond','family','kind','line_price')}
        for h in horizon:
            j=t+h
            if j>=len(fr):
                rec[f'Dp{h}_available']=False;rec[f'Dp{h}_close_pct']=None;rec[f'Dp{h}_line_gap_pct']=None
            else:
                price=float(fr.close.iloc[j]);ln=line_value(l,j)
                rec[f'Dp{h}_available']=True
                rec[f'Dp{h}_close_pct']=round(100*(price/fr.close.iloc[t]-1),4)
                rec[f'Dp{h}_line_gap_pct']=round(100*(price/ln-1),4) if ln>0 else None
        for h in (3,5,10,20):
            end=min(len(fr)-1,t+h);window=range(t+1,end+1)
            if end<t+h or not len(window):
                rec[f'Dp{h}_full_window']=False;rec[f'Dp{h}_hold_all_closes']=None
                rec[f'Dp{h}_first_recross_date']=None
                continue
            cmp=[float(fr.close.iloc[j])-line_value(l,j) for j in window]
            above=ev['event']=='CROSS_UP'
            maintained=[x>0 if above else x<0 for x in cmp]
            rec[f'Dp{h}_full_window']=True
            rec[f'Dp{h}_hold_all_closes']=bool(all(maintained))
            fail=next((j for j,ok in zip(window,maintained) if not ok),None)
            rec[f'Dp{h}_first_recross_date']=str(fr.iloc[fail].date.date()) if fail is not None else None
            # A wick into a frozen upper resistance from above with close above
            # is a retest *candidate*, not proof of future profitable support.
            touches=[]
            for j in window:
                level=line_value(l,j)
                if level<=0:continue
                if above:
                    yes=(float(fr.low.iloc[j])<=level and float(fr.close.iloc[j])>=level)
                else:
                    yes=(float(fr.high.iloc[j])>=level and float(fr.close.iloc[j])<=level)
                if yes:touches.append(str(fr.iloc[j].date.date()))
            rec[f'Dp{h}_retest_candidate_count']=len(touches)
            rec[f'Dp{h}_first_retest_date']=touches[0] if touches else None
        out.append(rec)
    return pd.DataFrame(out)
