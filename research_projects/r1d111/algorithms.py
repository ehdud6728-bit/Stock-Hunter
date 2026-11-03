"""R1-D111 independent, causal, public-method-inspired trendline comparisons.

IMPORTANT: These are OUR transparent proxies, not vendor source or vendor output.
They deliberately share only the frozen OHLCV input, pivot/candle extraction,
and common output schema. They use no post-event profit/returns for ranking.
"""
from __future__ import annotations
from collections import defaultdict
from math import isfinite
import numpy as np
import pandas as pd

ALGORITHMS=(
    'TV_PIVOT_ATR_PROXY',
    'TS_BODY_WICK_PROXY',
    'AC_PATTERN_FLOW_PROXY',
    'OSS_TOUCH_PEN_PROXY',
)
PARAMETERS={
    'TV_PIVOT_ATR_PROXY':{'radii':(5,25),'atr_multiplier':(2.,5.),'max_span':150,'max_life':90},
    'TS_BODY_WICK_PROXY':{'body_min_ratio':.25,'close_location':.64,'max_span_body':45,'max_span_wick':85,'max_life':55},
    'AC_PATTERN_FLOW_PROXY':{'body_min_ratio':.25,'close_location':.64,'max_span_body':50,'max_span_wick':75,'max_life':55},
    'OSS_TOUCH_PEN_PROXY':{'max_body_penetrations':2,'touch_tolerance_atr':.30,'max_span':90,'max_life':60},
}

def prepare(df):
    """Index 0..n-1 is a TRADING-BAR index; no forward shifts."""
    need={'date','open','high','low','close','volume','amount'}
    if not need.issubset(df.columns):raise ValueError('OHLCV columns missing')
    d=df.copy();d['date']=pd.to_datetime(d.date)
    d=d.sort_values('date').reset_index(drop=True)
    if len(d)==0 or d.date.duplicated().any():raise ValueError('invalid dates')
    for c in ('open','high','low','close','volume','amount'):d[c]=pd.to_numeric(d[c])
    if d[['open','high','low','close']].isna().any().any() or (d[['open','high','low','close']]<=0).any().any():raise ValueError('invalid price')
    if ((d.high<d[['open','close','low']].max(axis=1))|(d.low>d[['open','close','high']].min(axis=1))).any():raise ValueError('invalid OHLC')
    if (d[['volume','amount']]<0).any().any():raise ValueError('invalid flow')
    prev=d.close.shift(1);tr=pd.concat([d.high-d.low,(d.high-prev).abs(),(d.low-prev).abs()],axis=1).max(axis=1)
    d['atr14']=tr.rolling(14,min_periods=14).mean()
    d['amount_vs_prev20']=d.amount/d.amount.shift(1).rolling(20,min_periods=20).mean()
    d['volume_vs_prev20']=d.volume/d.volume.shift(1).rolling(20,min_periods=20).mean()
    d['body_top']=d[['open','close']].max(axis=1)
    d['body_bottom']=d[['open','close']].min(axis=1)
    d['body_share']=(d.close-d.open).abs()/(d.high-d.low).replace(0,np.nan)
    d['close_location']=(d.close-d.low)/(d.high-d.low).replace(0,np.nan)
    d['prior3_high']=d.high.shift(1).rolling(3,min_periods=3).max()
    return d


def make_anchors(d):
    """Extrema j known at j+r; candle body known at j close. No future read by signal."""
    rows=[];h=d.high.to_numpy(float);lo=d.low.to_numpy(float);o=d.open.to_numpy(float);c=d.close.to_numpy(float)
    for j in range(len(d)):
        ran=h[j]-lo[j];date=str(d.iloc[j].date.date())
        if ran>0:
            body=abs(c[j]-o[j])/ran;location=(c[j]-lo[j])/ran
            if c[j]>o[j] and body>=.25 and location>=.64:
                rows.extend([{'idx':j,'known':j,'family':'BULL_BODY_TOP','kind':'UPPER','value':c[j], 'basis':'CLOSE', 'strength':body},
                             {'idx':j,'known':j,'family':'BULL_BODY_BOTTOM','kind':'LOWER','value':o[j],'basis':'OPEN','strength':body}])
            if c[j]<o[j] and body>=.25 and location<=.36:
                rows.extend([{'idx':j,'known':j,'family':'BEAR_BODY_TOP','kind':'UPPER','value':o[j],'basis':'OPEN','strength':body},
                             {'idx':j,'known':j,'family':'BEAR_BODY_BOTTOM','kind':'LOWER','value':c[j],'basis':'CLOSE','strength':body}])
    for radius in (2,3,5,25):
        for j in range(radius,len(d)-radius):
            for kind,arr,name in [('UPPER',h,'WICK_HIGH'),('LOWER',lo,'WICK_LOW')]:
                v=arr[j];seg=arr[j-radius:j+radius+1]
                is_pivot=(v==seg.max() if kind=='UPPER' else v==seg.min()) and int(np.count_nonzero(seg==v))==1
                if is_pivot:
                    rows.append({'idx':j,'known':j+radius,'family':f'{name}_PIVOT{radius}',
                        'kind':kind,'value':float(v),'basis':'HIGH' if kind=='UPPER' else 'LOW',
                        'strength':1.,'radius':radius})
    for r in rows:
        r['date']=str(d.iloc[r['idx']].date.date())
        r['known_date']=str(d.iloc[r['known']].date.date())
        r['radius']=int(r.get('radius',0))
    return sorted(rows,key=lambda x:(x['known'],x['idx'],x['family']))


def _index_anchors(anchors,algorithm):
    if algorithm=='TV_PIVOT_ATR_PROXY':return [a for a in anchors if a['radius'] in (5,25)]
    if algorithm in ('TS_BODY_WICK_PROXY','AC_PATTERN_FLOW_PROXY'):
        return [a for a in anchors if a['radius']==0 or a['radius'] in (2,5)]
    if algorithm=='OSS_TOUCH_PEN_PROXY':return [a for a in anchors if a['radius']==0 or a['radius'] in (2,3,5)]
    raise ValueError(algorithm)


def _tv_lines(d,pivots):
    """Confirmation-time ZigZag prominence proxy, NOT exact TradingView implementation.

    Events are processed in the order they become known. A stronger same-side
    pivot can replace the pending endpoint only from its confirmation onward.
    Historical lines remain in the catalog with original first-known dates.
    """
    accepted={5:{'last':None,'UPPER':[],'LOWER':[]},25:{'last':None,'UPPER':[],'LOWER':[]}}
    candidates=[]
    for a in sorted(pivots,key=lambda x:(x['known'],x['idx'],x['kind'])):
        radius=a['radius'];state=accepted[radius];prev=state['last'];atr=float(d.atr14.iloc[a['idx']]);mult=2. if radius==5 else 5.
        if not isfinite(atr) or atr<=0:continue
        eligible=False
        if prev is None:eligible=True
        elif a['kind']==prev['kind']:
            eligible=(a['value']>prev['value'] if a['kind']=='UPPER' else a['value']<prev['value'])
        else:
            prior_atr=float(d.atr14.iloc[prev['idx']]);threshold=mult*prior_atr if isfinite(prior_atr) else mult*atr
            eligible=abs(a['value']-prev['value'])>=threshold
        if not eligible:continue
        same=state[a['kind']]
        if len(same):
            p=same[-1]
            if a['idx']-p['idx']>=3 and a['idx']-p['idx']<=150:
                candidates.append(_line(d,p,a,'TV_PIVOT_ATR_PROXY'))
        same.append(a);state['last']=a
    return candidates


def _line(d,a,b,algorithm):
    i,j=int(a['idx']),int(b['idx']);v1,v2=float(a['value']),float(b['value']);m=(v2-v1)/(j-i)
    known=max(int(a['known']),int(b['known']));kind=a['kind'];fam=a['family']
    # Baseline, always based on OHLCV no later than confirmation date.
    gap=j-i;xs=np.arange(i+1,j);y=v1+m*(xs-i)
    atr=(float(d.atr14.iloc[known]) if isfinite(d.atr14.iloc[known]) else float(d.iloc[known].close)*.02)
    tol=max(1e-6,.3*atr)
    if len(xs):
        vals=d.iloc[xs];bodies=vals.body_top.to_numpy(float) if kind=='UPPER' else vals.body_bottom.to_numpy(float)
        wicks=vals.high.to_numpy(float) if kind=='UPPER' else vals.low.to_numpy(float)
        body_pen=int(((bodies>y+tol) if kind=='UPPER' else (bodies<y-tol)).sum())
        wick_pen=int(((wicks>y+tol) if kind=='UPPER' else (wicks<y-tol)).sum())
        # An intermediate bar that falls near line; endpoint bars excluded.
        touch=int((abs(wicks-y)<=tol).sum())
        body_touch=int((abs(bodies-y)<=tol).sum())
        distances=np.abs(bodies-y)
        mean_error=float(distances.mean()/max(atr,1e-6))
    else:body_pen=wick_pen=touch=body_touch=0;mean_error=0.
    origin='BODY' if a['radius']==0 else 'WICK'
    family=fam
    lineid=f'{algorithm}|{family}|{a["date"]}|{b["date"]}|{kind}'
    return {'algorithm':algorithm,'line_id':lineid,'kind':kind,'family':family,'origin':origin,
      'a1':a['date'],'a2':b['date'],'i1':i,'i2':j,'v1':v1,'v2':v2,'m':m,
      'known_idx':known,'known_date':str(d.iloc[known].date.date()),'span':gap,
      'atr_at_known':atr,'body_pen':body_pen,'wick_pen':wick_pen,'interior_wick_touches':touch,
      'interior_body_touches':body_touch,'mean_body_error_atr':round(mean_error,5),
      'anchor_strength':min(float(a['strength']),float(b['strength'])),
      'source_a1_known':a['known_date'],'source_a2_known':b['known_date'],
      'candidate_status':'TWO_ANCHOR_PROVISIONAL'}


def generate_candidates(d,algorithm,anchors=None):
    if algorithm not in ALGORITHMS:raise ValueError('unknown algorithm')
    anchors=make_anchors(d) if anchors is None else anchors
    use=_index_anchors(anchors,algorithm)
    if algorithm=='TV_PIVOT_ATR_PROXY':rows=_tv_lines(d,use)
    else:
        group=defaultdict(list);rows=[]
        for a in sorted(use,key=lambda z:(z['known'],z['idx'])):
            same=group[a['family']]
            maxgap=(45 if a['radius']==0 else 85) if algorithm=='TS_BODY_WICK_PROXY' else (
                    (50 if a['radius']==0 else 75) if algorithm=='AC_PATTERN_FLOW_PROXY' else 90)
            # Limit enumeration per endpoint: preserve local relevance without quadratic explosion.
            n=0
            for p in reversed(same):
                gap=a['idx']-p['idx']
                if gap>maxgap:break
                if gap<3:continue
                if n>=10:break
                l=_line(d,p,a,algorithm)
                if algorithm=='OSS_TOUCH_PEN_PROXY' and l['body_pen']>2:continue
                if algorithm=='TS_BODY_WICK_PROXY' and l['body_pen']>4:continue
                if algorithm=='AC_PATTERN_FLOW_PROXY' and l['body_pen']>4:continue
                rows.append(l);n+=1
            same.append(a)
    rows.sort(key=lambda x:(x['known_idx'],x['family'],x['i2'],x['i1']))
    ids=[x['line_id'] for x in rows]
    if len(set(ids))!=len(ids):raise AssertionError('duplicate algorithm line id')
    return rows


def value(line,t):return float(line['v2'])+float(line['m'])*(int(t)-int(line['i2']))


def quality(d,line,t):
    """Fast vectorized PIT ranking; only data through t can affect the score."""
    known=int(line['known_idx'])
    if t<known or t>=len(d):raise ValueError('line not yet known')
    close=d['close'].values;high=d['high'].values;low=d['low'].values
    atr_arr=d['atr14'].values
    px=float(close[t]);level=value(line,t)
    if not isfinite(level) or level<=0:return -1e6,{}
    gap=100*(px/level-1);age=t-known;strength=float(line['anchor_strength'])
    atr=float(atr_arr[t]);atr=atr if isfinite(atr) and atr>0 else .02*px
    tol=.30*atr
    end=min(t,int(line['i2'])+60)
    start=max(known+1,int(line['i2'])+1,t-29)
    holds=0;wrong=0;touches=0
    if start<=end:
        xs=np.arange(start,end+1,dtype=int)
        levels=float(line['v2'])+float(line['m'])*(xs-int(line['i2']))
        mask=levels>0
        if mask.any():
            ix=xs[mask];levels=levels[mask]
            vals=high[ix] if line['kind']=='UPPER' else low[ix]
            near=np.abs(vals-levels)<=tol
            touches=int(near.sum())
            respected=close[ix]<=levels*.996 if line['kind']=='UPPER' else close[ix]>=levels*1.004
            holds=int((near&respected).sum())
            invalid=close[ix]>levels*1.012 if line['kind']=='UPPER' else close[ix]<levels*.988
            wrong=int(invalid.sum())
    structural=8.*strength+min(7.,line['span']*.25)-5*line['body_pen']-1.5*line['wick_pen']
    recent=max(0.,4.-.05*age);proximity=max(-10.,6.-.60*abs(gap))
    if line['algorithm']=='TV_PIVOT_ATR_PROXY':
        score=structural+recent+proximity+4*min(2,holds)-4*wrong+2*(line['origin']=='WICK')
    elif line['algorithm']=='TS_BODY_WICK_PROXY':
        score=structural+recent+proximity+5*min(3,holds)+2*line['interior_wick_touches']-4*wrong
    elif line['algorithm']=='AC_PATTERN_FLOW_PROXY':
        rowbody=d['body_share'].values[known];rowvol=d['amount_vs_prev20'].values[known]
        rowloc=d['close_location'].values[known];rowprior=d['prior3_high'].values[known]
        body=float(rowbody) if isfinite(rowbody) else 0
        vol=float(rowvol) if isfinite(rowvol) else 0
        loc=float(rowloc) if isfinite(rowloc) else .5
        recent_high=bool(isfinite(rowprior) and close[known]>rowprior)
        candle=5*body+2*min(3,vol)+2*abs(loc-.5)+2*recent_high
        score=structural*.65+recent+proximity+3*min(2,holds)-3*wrong+candle
    else:
        score=structural+recent+proximity+4*line['interior_body_touches']+4*min(3,holds)-7*wrong-2*line['mean_body_error_atr']
    return float(round(score,5)),{'gap_pct':round(gap,4),'touches_after_known':touches,
         'respected_after_known':holds,'wrong_side_after_known':wrong,'atr':atr}
