"""R1-D100 point-in-time Bollinger AND Envelope full-band research metrics.

Frozen original R1-D96-D99 files remain unchanged. This module enriches a copy
and only uses price observations at or before each requested asof date.

Baseline: BB20 period 20, K=2, population stdev(ddof=0).
Research companions: BB30, BB40 with same K=2.
Envelope: 20/40 periods, +/-10% centered on the corresponding SMA.
Do NOT interpret Envelope relative band width as volatility: it is constant 20%.
"""
from __future__ import annotations
import math
import numpy as np
import pandas as pd

BB_PERIODS = (20, 30, 40)
ENV_PERIODS = (20, 40)
BB_K = 2.0
ENV_K = .10


def enrich_bands(d: pd.DataFrame) -> pd.DataFrame:
    if not {'date', 'close', 'low', 'high'}.issubset(d.columns):
        raise ValueError('date/close/low/high columns required')
    out = d.copy()
    close = pd.to_numeric(out['close'], errors='raise')
    for n in BB_PERIODS:
        center = close.rolling(n, min_periods=n).mean()
        sd = close.rolling(n, min_periods=n).std(ddof=0)
        out[f'BB{n}_MID'] = center
        out[f'BB{n}_HI'] = center + BB_K * sd
        out[f'BB{n}_LO'] = center - BB_K * sd
        out[f'BB{n}_WIDTH_PCT'] = 100 * (out[f'BB{n}_HI'] - out[f'BB{n}_LO']) / center
        out[f'BB{n}_WIDTH_ABS'] = out[f'BB{n}_HI'] - out[f'BB{n}_LO']
    for n in ENV_PERIODS:
        center = close.rolling(n, min_periods=n).mean()
        out[f'ENV{n}_MID'] = center
        out[f'ENV{n}_10_HI'] = center * (1 + ENV_K)
        out[f'ENV{n}_10_LO'] = center * (1 - ENV_K)
        out[f'ENV{n}_WIDTH_PCT'] = 100 * (out[f'ENV{n}_10_HI'] - out[f'ENV{n}_10_LO']) / center
        out[f'ENV{n}_WIDTH_ABS'] = out[f'ENV{n}_10_HI'] - out[f'ENV{n}_10_LO']
    return out


def future_price_tamper_copy(d: pd.DataFrame, asof_idx: int, multiplier: float = 1.217) -> pd.DataFrame:
    """Independent PIT negative control; NEVER used for actual candidate selection.

    The original input, all rows at/before asof, dates, and traded amount are
    untouched. All OHLC are cast to float BEFORE fractional future mutation,
    preventing pandas LossySetitemError for KRX integer price columns.
    """
    if not (0 <= asof_idx < len(d)):
        raise ValueError('asof_idx outside dataframe')
    if not (np.isfinite(multiplier) and multiplier > 0):
        raise ValueError('multiplier must be positive and finite')
    if not {'open', 'high', 'low', 'close'}.issubset(d.columns):
        raise ValueError('OHLC columns are required')
    out = d.copy(deep=True)
    positions_after = out.index[asof_idx+1:]
    for col in ('open', 'high', 'low', 'close'):
        out[col] = pd.to_numeric(out[col], errors='raise').astype(float)
        if len(positions_after):
            out.loc[positions_after,col] = out.loc[positions_after,col] * multiplier
    return out


def _at(d, asof: str) -> int:
    dates=pd.to_datetime(d.date,errors='raise')
    if not dates.is_monotonic_increasing or dates.duplicated().any():
        raise ValueError('dates must be unique and ascending')
    ix=np.flatnonzero(dates.eq(pd.Timestamp(asof)).to_numpy())
    if len(ix)!=1:raise ValueError('asof date must occur exactly once')
    return int(ix[0])


def _pct_move(now, prev):
    if not (np.isfinite(now) and np.isfinite(prev)) or prev==0:
        return None
    return round(100*(now/prev-1),4)


def _safe(v):
    return round(float(v),4) if np.isfinite(v) else None


def band_report(d: pd.DataFrame, asof: str) -> list[dict]:
    """Rows: BB20/30/40 & ENV20/40, computed at the historical snapshot.

    5/20-day width changes in percentage POINTS, not percent changes of %width.
    For Envelope, relative width stays 20% by construction; absolute width
    follows the moving average. Upper/lower slopes are tracked independently.
    """
    k=_at(d,asof)
    ans=[]
    for family, periods in (('BB',BB_PERIODS), ('ENV',ENV_PERIODS)):
        for n in periods:
            prefix=f'BB{n}' if family=='BB' else f'ENV{n}'
            up=f'{prefix}_HI' if family=='BB' else f'{prefix}_10_HI'
            lo=f'{prefix}_LO' if family=='BB' else f'{prefix}_10_LO'
            mid=f'{prefix}_MID'
            wp=f'{prefix}_WIDTH_PCT'
            wa=f'{prefix}_WIDTH_ABS'
            need=(up,lo,mid,wp,wa)
            if not all(x in d.columns for x in need):raise KeyError(f'Missing band columns for {prefix}; call enrich_bands()')
            cur=d.iloc[k]
            u,l,m,w,a=[float(cur[c]) for c in need]
            if not all(np.isfinite(x) for x in (u,l,m,w,a)) or m<=0 or u<=l:
                raise ValueError(f'Insufficient positive band observations: {asof}, {prefix}')
            def back(col,lag):
                return float(d.iloc[k-lag][col]) if k>=lag else math.nan
            width5=back(wp,5);width20=back(wp,20)
            spread5=_safe(w-width5) if np.isfinite(width5) else None
            spread20=_safe(w-width20) if np.isfinite(width20) else None
            high,low,cl=[float(cur[c]) for c in ('high','low','close')]
            # BB and ENV both get the same price/range/reaction assessment.
            ans.append({
                'asof':asof,'family':family,'period':n,'params':'BB k=2, ddof=0' if family=='BB' else 'ENV +/-10%',
                'center':_safe(m),'upper':_safe(u),'lower':_safe(l),
                'band_width_abs':_safe(a),'band_width_pct':_safe(w),
                'width_change5_pp':spread5,'width_change20_pp':spread20,
                'upper_slope5_pct':_pct_move(u,back(up,5)),
                'lower_slope5_pct':_pct_move(l,back(lo,5)),
                'upper_slope20_pct':_pct_move(u,back(up,20)),
                'lower_slope20_pct':_pct_move(l,back(lo,20)),
                'center_slope20_pct':_pct_move(m,back(mid,20)),
                'abs_width_change20_pct':_pct_move(a,back(wa,20)),
                'price_position_pct':_safe(100*(cl-l)/(u-l)),
                'close_above_upper':bool(cl>u),'close_below_lower':bool(cl<l),
                'high_touched_upper':bool(high>=u),'low_touched_lower':bool(low<=l),
                'width_is_fixed_relative':family=='ENV',
                'width_trend':('EXPANDING' if spread5 is not None and spread5>0.1 else
                               'CONTRACTING' if spread5 is not None and spread5<-.1 else
                               'STABLE_OR_UNAVAILABLE') if family=='BB' else 'RELATIVE_WIDTH_FIXED_BY_FORMULA',
                'research_only':True,
            })
    # Compare same-period BB vs ENV width, without implying any predictive edge.
    env={r['period']:r for r in ans if r['family']=='ENV'}
    for r in ans:
        r['bb_to_envelope_width_ratio']=round(r['band_width_abs']/env[r['period']]['band_width_abs'],4) if r['family']=='BB' and r['period'] in env else None
    return ans
