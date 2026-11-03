"""R1-D107 point-in-time line significance, grouping, and triangle readiness.

RESEARCH_ONLY. No future labels in any selector; no edits to upstream catalogs.
All thresholds are explanatory diagnostics, never optimized on stock outcomes.
"""
from __future__ import annotations
from typing import Mapping
import math
import numpy as np
import pandas as pd

# Import frozen R1-D106 line formula. Caller adds r1d106 to PYTHONPATH.
from multiline import line_value


def _price(frame: pd.DataFrame, t: int, field: str) -> float:
    return float(frame.iloc[t][field])


def line_quality(frame: pd.DataFrame, line: Mapping, asof_idx: int) -> dict:
    """Evaluate a candidate using observations at or before asof_idx only.

    Historical retests cannot be presumed at second-anchor close. A line is
    provisional on its creation day. Quality excludes future reactions.
    """
    t=int(asof_idx);known=int(line['known_idx']);last=int(line['i2'])
    if t<known or t>=len(frame):raise ValueError('line not yet known / date absent')
    span=int(line['i2'])-int(line['i1'])
    if span<=0:raise ValueError('invalid anchor span')
    close=_price(frame,t,'close');level=line_value(line,t)
    if not math.isfinite(level) or level<=0:raise ValueError('invalid current line level')
    upper=line['kind']=='UPPER'
    window=range(max(last+1,known+1,t-29),t+1)
    touches=0;holds=0;wrong=0;first_touch=None;first_break=None;touch_dates=[]
    # No lookahead: observations exclusively from bars following confirmation.
    for j in window:
        ln=line_value(line,j)
        if ln<=0:continue
        o=frame.iloc[j];c=float(o.close);lo=float(o.low);hi=float(o.high)
        # up to 0.5% price distance to line counts as retest attempt
        if upper:
            contact=(hi>=ln*.995 and lo<=ln*1.005)
            respected=c<=ln*1.004
            invalid=c>ln*1.012
        else:
            contact=(lo<=ln*1.005 and hi>=ln*.995)
            respected=c>=ln*.996
            invalid=c<ln*.988
        if contact:
            touches+=1;touch_dates.append(str(o.date.date()))
            if respected:holds+=1
            if first_touch is None:first_touch=str(o.date.date())
        if invalid:
            wrong+=1
            if first_break is None:first_break=str(o.date.date())
    gap_pct=100*(close/level-1)
    # Evidence score -- no future returns. The zero-distance 'same-day' trap
    # gets NO bonus. Do not label this a probability of profitability.
    a_strength=float(line['anchor_strength']);pen=int(line['penetrations_1p2pct'])
    strength_points=min(22.,22.*max(0.,a_strength))
    span_points=min(12.,12.*span/10.)
    interior_points=max(0.,20.-8.*pen)
    maturity=t-known
    maturity_points=0. if maturity<=0 else (8. if maturity>=3 else 4.)
    respect_points=min(18.,6.*holds)
    wrong_points=min(20.,7.*wrong)
    slope=float(line['slope_pct_per_bar'])
    extreme_slope_penalty=8. if abs(slope)>2 else 0.
    quality=round(max(0.,min(100.,strength_points+span_points+interior_points+maturity_points+respect_points-wrong_points-extreme_slope_penalty)),3)
    return {'line_id':line['line_id'],'asof':str(frame.iloc[t].date.date()),'asof_idx':t,
            'kind':line['kind'],'family':line['family'],'a1':line['a1'],'a2':line['a2'],
            'known_date':line['known_date'],'known_idx':known,'age_since_known_bars':maturity,
            'anchor_span_bars':span,'anchor_strength':a_strength,'interior_penetrations':pen,
            'slope_pct_per_bar':slope,'line_price':round(level,3),'close':close,
            'close_to_line_pct':round(gap_pct,4),'quality_diagnostic_0_100':quality,
            'post_confirmation_contacts':touches,'post_confirmation_respected_contacts':holds,
            'post_confirmation_decisive_wrong_side_closes':wrong,
            'first_contact_date_PIT':first_touch,'first_wrong_side_date_PIT':first_break,
            'touch_dates_PIT':'|'.join(touch_dates),
            'readiness':'NEW_LINE_PROVISIONAL' if maturity==0 else
                        'OBSERVED_1_2_BARS' if maturity<3 else 'OBSERVED_3PLUS_BARS',
            'warning':'QUALITY_DIAGNOSTIC_NOT_WIN_PROBABILITY'}


def display_groups(qualities: pd.DataFrame, *, max_each_kind:int=5, corridor_pct:float=.8,
                   slope_tol_pp:float=.25) -> pd.DataFrame:
    """Display-only deduplication, never mutates/cuts immutable R106 line catalog."""
    cols=list(qualities.columns)+['display_group','display_rank','is_display_representative']
    if qualities.empty:return pd.DataFrame(columns=cols)
    if not {'kind','line_price','slope_pct_per_bar','quality_diagnostic_0_100'}.issubset(qualities):
        raise ValueError('missing quality attributes')
    result=[]
    for kind, subset in qualities.groupby('kind'):
        ranked=subset.sort_values(['quality_diagnostic_0_100','known_idx','line_id'],ascending=[False,False,True])
        groups=[]
        for r in ranked.to_dict('records'):
            matched=next((i for i,g in enumerate(groups)
                if abs(100*(r['line_price']/g['line_price']-1))<=corridor_pct and
                   abs(r['slope_pct_per_bar']-g['slope_pct_per_bar'])<=slope_tol_pp),None)
            if matched is None:
                groups.append(r);matched=len(groups)-1
            r['display_group']=f'{kind}|{matched+1}'
            r['display_rank']=matched+1
            r['is_display_representative']=(r['line_id']==groups[matched]['line_id'] and matched<max_each_kind)
            result.append(r)
    return pd.DataFrame(result)


def triangle_quality(frame:pd.DataFrame, tri:Mapping, lines_by_id:dict, asof_idx:int) -> dict:
    """Evaluate a geometry candidate at t without future bars or hindsight.

    A confirmed wedge breakout REQUIRES prior inside bars and >=0.4% close
    breakout with >=2 bars remaining before apex. Already-outside geometry
    never becomes a prior-detected triangle breakout in this study.
    """
    t=int(asof_idx);known=int(tri['idx'])
    if t<known or t>=len(frame):raise ValueError('triangle not known / date missing')
    u=lines_by_id[tri['upper_id']];d=lines_by_id[tri['lower_id']]
    if max(int(u['known_idx']),int(d['known_idx']))>t:
        raise ValueError('line not known')
    top=line_value(u,t);bot=line_value(d,t)
    price=_price(frame,t,'close');apex=float(tri['apex_index_estimate'])
    bars_left=apex-t
    geometry_possible=(top>bot>0)
    prev_count=0;outside_count=0;history=[]
    # Only inspect previous bars when both lines were already known.
    for j in range(known,t):
        ut=line_value(u,j);lt=line_value(d,j)
        if ut<=lt or lt<=0:continue
        c=_price(frame,j,'close')
        inside=(lt<=c<=ut)
        prev_count+=int(inside)
        outside_count+=int(not inside)
        history.append('I' if inside else 'O')
    at_first=tri['price_position_at_first_known']
    if not geometry_possible or bars_left<=0:state='APEX_PASSED_OR_INVALID'
    elif bars_left<2:state='APEX_TOO_CLOSE_FOR_NEW_BREAKOUT'
    elif at_first!='INSIDE':state='ALREADY_OUTSIDE_AT_FIRST_CONFIRMATION'
    elif prev_count<2:state='INSUFFICIENT_PRIOR_INSIDE_BARS'
    elif price>=top*1.004 and _price(frame,t-1,'close')<=line_value(u,t-1)*1.004:
        state='PIT_UP_BREAK_WITH_PRIOR_INSIDE'
    elif price<=bot*.996 and _price(frame,t-1,'close')>=line_value(d,t-1)*.996:
        state='PIT_DOWN_BREAK_WITH_PRIOR_INSIDE'
    elif bot<=price<=top:state='ACTIVE_INSIDE'
    else:state='ALREADY_OUTSIDE_NO_NEW_BREAK'
    result={k:tri[k] for k in ('upper_id','lower_id','shape','upper_a1','upper_a2','lower_a1','lower_a2','date')}
    result.update({'asof':str(frame.iloc[t].date.date()),'asof_idx':t,
                   'top':round(top,3),'bottom':round(bot,3),'width_pct_close':round(100*(top-bot)/price,3),
                   'apex_index_estimate':apex,'bars_until_apex':round(bars_left,3),
                   'prior_inside_bars':prev_count,'prior_outside_bars':outside_count,
                   'first_known_price_position':at_first,'state_PIT':state,
                   'close_to_top_pct':round(100*(price/top-1),3) if top>0 else np.nan,
                   'close_to_bottom_pct':round(100*(price/bot-1),3) if bot>0 else np.nan,
                   'not_yet_trade_signal':state not in ('PIT_UP_BREAK_WITH_PRIOR_INSIDE','PIT_DOWN_BREAK_WITH_PRIOR_INSIDE')})
    return result


def apex_limited_outcome(frame:pd.DataFrame, tri:Mapping, lines_by_id:dict, asof_idx:int, horizon=20) -> dict:
    """EXPOST only: do not count post-apex falling line as supported."""
    t=int(asof_idx);u=lines_by_id[tri['upper_id']]
    if horizon<1:raise ValueError(horizon)
    if t+horizon>=len(frame):return {'expost_full':False,'ret20_pct_EXPOST':None,'pre_apex_hold_count_EXPOST':None}
    apex=float(tri['apex_index_estimate'])
    end=min(t+horizon,int(math.ceil(apex))-1)
    pre_apex=range(t+1,max(t+1,end+1))
    retained=sum(_price(frame,j,'close')>=line_value(u,j) for j in pre_apex)
    return {'expost_full':True,'ret20_pct_EXPOST':round(100*(_price(frame,t+horizon,'close')/_price(frame,t,'close')-1),3),
            'pre_apex_days_EXPOST':len(pre_apex),'pre_apex_hold_count_EXPOST':retained,
            'pre_apex_hold_all_EXPOST':bool(len(pre_apex)>0 and retained==len(pre_apex))}
