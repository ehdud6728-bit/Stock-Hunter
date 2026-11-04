"""Strict R112 six-lane to R116 one-bar source marks adapter (RESEARCH ONLY)."""
from __future__ import annotations
import pandas as pd

def original_daily_marks(raw_prepared, six_lanes, code):
    """Return EXACTLY one time-ordered original OHLCV mark per trading bar.

    R112 produces six (scale, kind) rows per date. R116 requires a single
    (idx, date, close) observation for each bar. Validate the full six-lane
    grid and its alignment to the original OHLCV; never silently deduplicate
    corrupt/mismatched R112 states or change original R112/R116 algorithms.
    """
    marks=pd.DataFrame({'idx':range(len(raw_prepared)),
                        'date':raw_prepared.date.dt.strftime('%Y-%m-%d'),
                        'close':pd.to_numeric(raw_prepared.close,errors='raise')})
    expected={(scale,kind) for scale in ('SHORT','MEDIUM','LONG')
             for kind in ('UPPER','LOWER')}
    if len(six_lanes)!=6*len(marks):
        raise AssertionError('R112_6_LANES_NOT_COMPLETE '+code)
    required={'idx','date','scale','kind','close'}
    if not required.issubset(six_lanes.columns):
        raise AssertionError('R112_LANE_FIELDS_MISSING '+code)
    if six_lanes[['idx','date','scale','kind']].duplicated().any():
        raise AssertionError('R112_DUPLICATE_LANE '+code)
    left=six_lanes[['idx','date','scale','kind','close']].copy()
    if left[['idx','date','close']].isna().any().any():
        raise AssertionError('R112_MISSING_PRICE_MARK '+code)
    left['idx']=pd.to_numeric(left['idx'],errors='raise').astype(int)
    left['date']=left['date'].astype(str)
    left['close']=pd.to_numeric(left['close'],errors='raise')
    grouped=left.groupby('idx',sort=True)
    if set(grouped.groups)!=set(range(len(marks))):
        raise AssertionError('R112_BAR_INDEX_MISMATCH '+code)
    for t,g in grouped:
        if len(g)!=6 or set(zip(g.scale,g.kind))!=expected:
            raise AssertionError('R112_SIX_LANE_GRID_INVALID '+code)
        m=marks.iloc[int(t)]
        if (g.date!=m.date).any() or not (g.close==m.close).all():
            raise AssertionError('R112_ORIGINAL_OHLCV_MARK_MISMATCH '+code)
    if marks[['idx','date']].duplicated().any():
        raise AssertionError('ORIGINAL_OHLCV_MARK_DUPLICATE '+code)
    return marks

