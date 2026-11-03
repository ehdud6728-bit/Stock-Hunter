"""Autochartist-public-CONCEPT-inspired two-edge pattern + candle/flow diagnostics.

NOT the proprietary Autochartist model. All metrics at an asof t read only <=t.
No user hard-coded reference lines enter the candidate ranking.
"""
from __future__ import annotations
from math import isfinite
import numpy as np
import pandas as pd
from algorithms import quality,value


def pattern_asof(d,lines,t,*,max_each=12,max_output=6):
    if t<1 or t>=len(d):raise ValueError('invalid asof')
    close=float(d.close.iloc[t]);valid=[l for l in lines if l['known_idx']<=t and l['known_idx']>=t-55
        and l['i2']>=t-65 and value(l,t)>0 and abs(100*(close/value(l,t)-1))<=15]
    qualified={}
    for k in ('UPPER','LOWER'):
        pool=[l for l in valid if l['kind']==k]
        # Independent score + current distance; NEVER future follow-through.
        pool.sort(key=lambda l:(quality(d,l,t)[0],l['i2']),reverse=True)
        qualified[k]=pool[:max_each]
    results=[]
    dclosed=d.close.to_numpy(float);drange=(d.high-d.low).to_numpy(float)
    for u in qualified['UPPER']:
        for l in qualified['LOWER']:
            joint=max(int(u['known_idx']),int(l['known_idx']))
            # At most one event if both lines already known; no preconfirmed triangles.
            high=value(u,t);bottom=value(l,t);delta=float(u['m'])-float(l['m'])
            if high<=bottom or bottom<=0 or delta>=0:continue
            width=high-bottom
            horizon=width/(-delta)
            if horizon<0 or horizon>45:continue
            known_upper=value(u,joint);known_lower=value(l,joint)
            if known_upper<=known_lower:continue
            first=str(d.iloc[joint].date.date())
            first_close=float(dclosed[joint]);first_position=('ABOVE' if first_close>known_upper else 'BELOW' if first_close<known_lower else 'INSIDE')
            # Observed pattern containment only after BOTH lines exist.
            within=[]
            for j in range(joint,t):
                up=value(u,j);down=value(l,j)
                within.append(bool(down<=dclosed[j]<=up))
            inside_before=int(sum(within));outside_before=len(within)-inside_before
            typical=float(np.mean(drange[max(0,t-14):t])) if t else float('nan')
            breakout_strength=float(drange[t]/typical) if isfinite(typical) and typical>0 else np.nan
            amount=float(d.amount_vs_prev20.iloc[t]) if isfinite(d.amount_vs_prev20.iloc[t]) else np.nan
            volume=float(d.volume_vs_prev20.iloc[t]) if isfinite(d.volume_vs_prev20.iloc[t]) else np.nan
            body=float(d.body_share.iloc[t]) if isfinite(d.body_share.iloc[t]) else np.nan
            lowwick=(float(d.close_location.iloc[t]) if isfinite(d.close_location.iloc[t]) else np.nan)
            status=('ALREADY_OUTSIDE_WHEN_FIRST_KNOWN' if first_position!='INSIDE' else
                'NEAR_APEX' if horizon<2 else
                'BREAKOUT_UP_PRIOR_INSIDE' if inside_before>=2 and close>=high*1.004 else
                'BREAKOUT_DOWN_PRIOR_INSIDE' if inside_before>=2 and close<=bottom*.996 else
                'INSUFFICIENT_PRIOR_INSIDE' if inside_before<2 else
                'ACTIVE_INSIDE' if bottom<=close<=high else 'OUTSIDE_NO_NEW_BREAK')
            # Connection stability: count price approaches to either edge in base+post-known.
            contact_u=int(u['interior_wick_touches'])
            contact_l=int(l['interior_wick_touches'])
            clarity=(inside_before/len(within)) if within else np.nan
            uniformity=1/(1+abs(int(u['i2'])-int(l['i2']))/max(1,int(u['span'])+int(l['span'])))
            # No supposed 1..10 proprietary scores. Diagnostic pieces are independent raw units.
            results.append({'algorithm':'AC_PATTERN_FLOW_PROXY','asof':str(d.iloc[t].date.date()),'idx':t,
                'upper_line_id':u['line_id'],'lower_line_id':l['line_id'],
                'upper_anchor1':u['a1'],'upper_anchor2':u['a2'],
                'lower_anchor1':l['a1'],'lower_anchor2':l['a2'],
                'first_both_known_date':first,'first_both_known_position':first_position,
                'top':round(high,3),'bottom':round(bottom,3),'width_pct_close':round(100*width/close,3),
                'bars_to_apex_estimate':round(horizon,3),
                'prior_inside_bars_PIT':inside_before,'prior_outside_bars_PIT':outside_before,
                'clarity_fraction_PIT':round(clarity,4) if isfinite(clarity) else np.nan,
                'uniformity_proxy_PIT':round(uniformity,4),'base_touch_upper':contact_u,'base_touch_lower':contact_l,
                'candle_range_vs_prior14_PIT':round(breakout_strength,4) if isfinite(breakout_strength) else np.nan,
                'candle_body_share_PIT':round(body,4) if isfinite(body) else np.nan,
                'candle_close_location_PIT':round(lowwick,4) if isfinite(lowwick) else np.nan,
                'amount_vs_prev20_PIT':round(amount,4) if isfinite(amount) else np.nan,
                'volume_vs_prev20_PIT':round(volume,4) if isfinite(volume) else np.nan,
                'state_PIT':status,'not_trade_signal':True,
                'ranking_score_for_display_only':round(quality(d,u,t)[0]+quality(d,l,t)[0],4)})
    results.sort(key=lambda x:(x['ranking_score_for_display_only'],x['width_pct_close']),reverse=True)
    return results[:max_output]
