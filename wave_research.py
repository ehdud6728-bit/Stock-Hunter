"""Causal wave/squeeze diagnostics. Measurements are hypotheses, not buy signals.

All input bars are <= signal date; historical confirmation uses only already
observed bars. Outcome columns are never inputs to these features.
"""
import numpy as np
import pandas as pd


def wave_features(hist):
    out = {'wave_status': 'INSUFFICIENT_HISTORY'}
    if len(hist) < 45:
        return out
    z=hist.reset_index(drop=True)
    c=z.close.astype(float); h=z.high.astype(float); l=z.low.astype(float)
    v=z.volume.astype(float); a=z.amount.astype(float)
    if (c<=0).any() or (h<=0).any() or (l<=0).any():
        return {'wave_status':'BAD_PRICE'}
    true_range=pd.concat([(h-l),(h-c.shift()).abs(),(l-c.shift()).abs()],axis=1).max(axis=1)
    atr=true_range.rolling(14,min_periods=14).mean()/c*100
    daily_abs=c.pct_change().abs()*100
    def mean_ratio(x, n=5, baseline=20):
        early=x.iloc[-(n+baseline):-n].mean()
        later=x.iloc[-n:].mean()
        return float(later/early) if pd.notna(early) and early>0 else np.nan
    out.update({
        'wave_status':'OK',
        'squeeze_atr_pct':float(atr.iloc[-1]),
        'squeeze_atr5_vs_prior20':mean_ratio(atr),
        'squeeze_absret5_vs_prior20':mean_ratio(daily_abs),
        'squeeze_volume5_vs_prior20':mean_ratio(v),
        'squeeze_amount5_vs_prior20':mean_ratio(a),
        'squeeze_close_range_5d_pct':float((h.tail(5).max()-l.tail(5).min())/c.iloc[-1]*100),
        'squeeze_close_range_20d_pct':float((h.tail(20).max()-l.tail(20).min())/c.iloc[-1]*100),
    })
    # Strict fractal pivots confirmed after 2 bars (never use a future bar).
    highs=h.to_numpy(); lows=l.to_numpy()
    pivots=[]
    for i in range(2,len(z)-2):
        if highs[i]>max(highs[i-2:i]) and highs[i]>=max(highs[i+1:i+3]):
            pivots.append((i,'HIGH',highs[i]))
        if lows[i]<min(lows[i-2:i]) and lows[i]<=min(lows[i+1:i+3]):
            pivots.append((i,'LOW',lows[i]))
    pivots.sort(key=lambda p:(p[0],p[1]))
    # Collapse successive same-type pivots into most extreme.
    clean=[]
    for p in pivots:
        if clean and clean[-1][1]==p[1]:
            if (p[1]=='HIGH' and p[2]>=clean[-1][2]) or (p[1]=='LOW' and p[2]<=clean[-1][2]):
                clean[-1]=p
        else:clean.append(p)
    out['wave_confirmed_pivots']=len(clean)
    # Last two fully confirmed corrective legs; no selection on later returns.
    legs=[]
    for p,q in zip(clean,clean[1:]):
        if p[1]=='HIGH' and q[1]=='LOW' and p[2]>q[2]:
            seg=z.iloc[p[0]+1:q[0]+1]
            legs.append({'depth':float((p[2]-q[2])/p[2]*100),'days':q[0]-p[0],
                         'volume':float(seg.volume.mean()),'amount':float(seg.amount.mean()),
                         'low':float(q[2])})
    if len(legs)>=2:
        prev,cur=legs[-2:]
        out.update({'wave_prev_depth_pct':prev['depth'],'wave_last_depth_pct':cur['depth'],
                    'wave_depth_contraction_ratio':cur['depth']/prev['depth'] if prev['depth']>0 else np.nan,
                    'wave_time_symmetry_ratio':cur['days']/prev['days'] if prev['days']>0 else np.nan,
                    'wave_amount_contraction_ratio':cur['amount']/prev['amount'] if prev['amount']>0 else np.nan,
                    'wave_volume_contraction_ratio':cur['volume']/prev['volume'] if prev['volume']>0 else np.nan,
                    'wave_last_vs_prev_trough_pct':(cur['low']/prev['low']-1)*100 if prev['low']>0 else np.nan})
    return out
