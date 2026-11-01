"""R1-D99: NON-DESTRUCTIVE research metadata for R1-D98 trendline lines.

R1-D98 `bucket`, `status` and line selection are immutable inputs.
Old WATCH lines remain WATCH with NO age limit. Similar lines are grouped
for visual presentation only; all original line IDs and rows are retained.
All event inspection is PIT (date <= asof). No trading signals or PnL.
"""
from __future__ import annotations
from typing import Optional
import math
import pandas as pd

TOUCH_EPS = 0.01    # R1-D98 descriptive proximity, NOT proof of support
CROSS_EPS = 0.003  # R1-D98 close tolerance
SIMILAR_PRICE_PCT = 0.8    # review-only visualization hypothesis
SIMILAR_SLOPE_PCT = 0.15   # review-only visualization hypothesis


def positions(d):
    dates=d.date.dt.strftime('%Y-%m-%d').tolist()
    if len(set(dates)) != len(dates):
        raise ValueError('Duplicate daily dates are not valid')
    if dates != sorted(dates):
        raise ValueError('Prices must be sorted chronologically')
    return {day: i for i, day in enumerate(dates)}


def line_value(r, k):
    i,j=int(r['h1_idx']),int(r['h2_idx'])
    if not 0<=i<j:raise ValueError('invalid line anchors')
    return float(r['h1_value']) + (float(r['h2_value']) - float(r['h1_value'])) * (k-i)/(j-i)


def _event_index(pos, value, confirmed, t):
    if value is None or (isinstance(value,float) and math.isnan(value)) or str(value).strip() in ('','nan','NaT','None'):
        return None
    idx=pos.get(str(value))
    if idx is None or idx<confirmed or idx>t:
        raise ValueError('out-of-bounds or missing historical event date: '+str(value))
    return idx


def describe_line(r,d,asof):
    """Add observation only; never change R98 state/bucket/eligibility."""
    pos=positions(d)
    if asof not in pos:raise ValueError('ASOF date not in OHLCV')
    t=pos[asof]
    if not 0<=int(r['h1_idx'])<int(r['h2_idx'])<int(r['h2_confirm_idx'])<=t:
        raise ValueError('unconfirmed or invalid R98 line')
    conf=int(r['h2_confirm_idx'])
    first=_event_index(pos,r.get('first_event_date',''),conf,t)
    last=_event_index(pos,r.get('last_event_date',''),conf,t)
    if first is not None and last is not None and last<first:
        raise ValueError('last event before first')
    first_age = t-first if first is not None else None
    cross_age = t-last if last is not None else None
    last_touch: Optional[int]=None
    touch_count=0
    last_reclaim:Optional[int]=None
    reclaim_count=0
    if first is not None and r['kind']=='RESISTANCE':
        # Full as-of history: a touch 40 or 80 bars after first break is kept.
        # A touch is descriptive, NOT definitive support conversion.
        for k in range(first+1,t+1):
            lv=line_value(r,k)
            q=d.iloc[k]
            touch=(float(q.low)<=lv*(1+TOUCH_EPS)
                   and float(q.high)>=lv*(1-TOUCH_EPS)
                   and float(q.close)>=lv*(1-CROSS_EPS))
            if touch:
                last_touch=k
                touch_count+=1
    if first is not None and r['kind']=='SUPPORT':
        for k in range(first+1,t+1):
            lp,lv=line_value(r,k-1),line_value(r,k)
            p,q=d.iloc[k-1],d.iloc[k]
            if float(p.close)<=lp*(1-CROSS_EPS) and float(q.close)>lv*(1+CROSS_EPS):
                last_reclaim=k
                reclaim_count+=1
    recent_interaction=max((k for k in (last,last_touch,last_reclaim) if k is not None),default=None)
    asof_line=line_value(r,t)
    if asof_line<=0:raise ValueError('nonpositive line')
    dist_pct=100*(float(d.iloc[t].close)/asof_line-1)
    old_bucket=str(r['bucket'])
    # Grouping is only a note; STATUS/Bucket/eligible flags are not touched.
    return dict(
        d99_bucket=old_bucket,d99_status=str(r['status']),
        d99_kept_without_expiry=True,
        d99_first_event_age_bars=first_age,
        d99_last_cross_age_bars=cross_age,
        d99_last_contact_age_bars=t-last_touch if last_touch is not None else None,
        d99_last_contact_date=str(d.iloc[last_touch].date.date()) if last_touch is not None else '',
        d99_contact_count_all=touch_count,
        d99_last_reclaim_age_bars=t-last_reclaim if last_reclaim is not None else None,
        d99_last_reclaim_date=str(d.iloc[last_reclaim].date.date()) if last_reclaim is not None else '',
        d99_reclaim_count_all=reclaim_count,
        d99_last_interaction_age_bars=t-recent_interaction if recent_interaction is not None else None,
        d99_repeat_cross_recorded=bool(first is not None and last is not None and last>first),
        d99_repeat_cross_is_fresh=False,
        d99_line_distance_signed_pct=round(dist_pct,3),
        d99_observation_only=True,
        d99_support_flip_proven=False,
    )


def enrich(lines,d,asof):
    pos=positions(d)
    if asof not in pos:raise ValueError('ASOF absent')
    # Truncate AFTER ASOF for explicit isolation from future candles.
    clipped=d.iloc[:pos[asof]+1].copy().reset_index(drop=True)
    return [dict(row,**describe_line(row,clipped,asof)) for row in lines]


def is_similar(a,b,price_pct=SIMILAR_PRICE_PCT,slope_pct=SIMILAR_SLOPE_PCT):
    if (str(a.get('code','')),str(a.get('asof','')),a['kind'],a['tier'],a['bucket'],a['status']) != (str(b.get('code','')),str(b.get('asof','')),b['kind'],b['tier'],b['bucket'],b['status']):
        return False
    denom=max(abs(float(a['close_asof'])),1)
    price_diff=100*abs(float(a['line_asof'])-float(b['line_asof']))/denom
    slope_diff=abs(float(a['slope_pct_per_bar'])-float(b['slope_pct_per_bar']))
    return price_diff<=price_pct and slope_diff<=slope_pct


def presentation_groups(rows,price_pct=SIMILAR_PRICE_PCT,slope_pct=SIMILAR_SLOPE_PCT):
    """Full line-to-representative mapping; never drops or changes a WATCH line."""
    # Same as supplied R98 row order, usually already score ranked.
    refs=[];mapping=[];groups={}
    for r in rows:
        match=next((ref for ref in refs if is_similar(r,ref,price_pct,slope_pct)),None)
        if match is None:
            match=r
            refs.append(r)
        key=(str(r.get('code','')),str(r.get('asof','')),str(match['line_id']))
        groups.setdefault(key,[]).append(str(r['line_id']))
        mapping.append(dict(code=str(r.get('code','')),asof=str(r.get('asof','')),
             line_id=str(r['line_id']),bucket=str(r['bucket']),status=str(r['status']),
             representative_line_id=str(match['line_id']),
             is_display_representative=bool(r['line_id']==match['line_id']),
             preserved_in_watch=bool(r['bucket']=='WATCH'),
             decision='DISPLAY_REPRESENTATIVE' if r['line_id']==match['line_id'] else 'DISPLAY_GROUP_MEMBER_NO_REMOVAL'))
    for m in mapping:
        m['group_member_line_ids']='|'.join(groups[(m['code'],m['asof'],m['representative_line_id'])])
        m['group_size']=len(groups[(m['code'],m['asof'],m['representative_line_id'])])
    return mapping


def assert_unmodified(original,annotated):
    if len(original)!=len(annotated):raise AssertionError('a candidate disappeared')
    protected=['code','asof','line_id','kind','tier','bucket','status','rank_score',
               'first_event_date','last_event_date','fresh_break_today','eligible_primary',
               'eligible_watch','structure_flags']
    for src,dst in zip(original,annotated):
        for key in protected:
            if str(src.get(key,''))!=str(dst.get(key,'')):
                raise AssertionError('R1-D98 field modified: '+key)
        if src['bucket']=='WATCH' and dst['d99_bucket']!='WATCH':
            raise AssertionError('WATCH was expired or removed')
