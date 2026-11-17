#!/usr/bin/env python3
"""Independent ENVELOPE(20,20) / legacy ENVELOPE(20,10) daily audit.

This is a POST-CLOSE pattern audit, NOT an intraday executable trading system.
Even if a day's low crossed a band, that band uses the day's final close and
therefore cannot be treated as the level actually visible at the day's low.
No future bars, fixed current-index constituents, or automatic trading is used.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd


def _code(v: object) -> str:
    s = str(v).strip().removesuffix('.0')
    return s.zfill(6) if s.isdigit() else s


def analyze(daily: pd.DataFrame, members: pd.DataFrame | None = None) -> pd.DataFrame:
    required = {'code', 'date', 'open', 'high', 'low', 'close', 'volume', 'amount'}
    if missing := required - set(daily.columns):
        raise ValueError(f'DAILY_COLUMNS_MISSING:{sorted(missing)}')
    d = daily.copy()
    d['code'] = d.code.map(_code)
    d['date'] = pd.to_datetime(d.date, errors='coerce').dt.normalize()
    for name in ('open', 'high', 'low', 'close', 'volume', 'amount'):
        d[name] = pd.to_numeric(d[name], errors='coerce')
    if d['date'].isna().any() or d['code'].isna().any() or d.duplicated(['code', 'date']).any():
        raise ValueError('INVALID_OR_DUPLICATED_CODE_DATE')
    d.sort_values(['code', 'date'], inplace=True)

    membership_keys: set[tuple[str, pd.Timestamp]] | None = None
    if members is not None:
        if {'code', 'date', 'is_member'} - set(members.columns):
            raise ValueError('MEMBERSHIP_REQUIRES_CODE_DATE_IS_MEMBER')
        m = members.copy()
        m['code'] = m.code.map(_code)
        m['date'] = pd.to_datetime(m.date, errors='coerce').dt.normalize()
        if m.date.isna().any() or m.duplicated(['code', 'date']).any():
            raise ValueError('INVALID_MEMBERSHIP_IDENTITY')
        if not m.is_member.astype(str).isin(['True', 'False', 'true', 'false', '1', '0']).all():
            raise ValueError('MEMBERSHIP_BOOLEAN_REQUIRED')
        m = m.loc[m.is_member.astype(str).str.lower().isin(['true', '1'])]
        membership_keys = set(zip(m.code, m.date))

    results = []
    for code, g in d.groupby('code', sort=False):
        g = g.reset_index(drop=True)
        valid = (np.isfinite(g[['open', 'high', 'low', 'close', 'volume', 'amount']]).all(axis=1)
                 & (g[['open', 'high', 'low', 'close']] > 0).all(axis=1)
                 & (g.volume >= 0) & (g.amount >= 0)
                 & (g.high >= g[['open', 'low', 'close']].max(axis=1))
                 & (g.low <= g[['open', 'high', 'close']].min(axis=1)))
        closes = g.close.where(valid)
        ma20 = closes.rolling(20, min_periods=20).mean()
        ma112 = closes.rolling(112, min_periods=112).mean()
        ma224 = closes.rolling(224, min_periods=224).mean()
        lower20 = ma20 * .8
        lower10 = ma20 * .9
        touch20 = (g.low <= lower20) & valid & lower20.notna()
        touch10 = (g.low <= lower10) & valid & lower10.notna()
        # Previous completed daily bar must have been valid and NOT already
        # touching; missing prior-band history means new-touch is UNKNOWN.
        prev_observed = valid.shift(1, fill_value=False) & lower20.shift(1).notna()
        first20 = touch20 & prev_observed & ~touch20.shift(1, fill_value=False)
        for i, item in g.iterrows():
            day = item.date
            status = ('INVALID_BAR' if not valid.iloc[i] else
                      'INSUFFICIENT_MA20_HISTORY' if pd.isna(ma20.iloc[i]) else
                      'DAILY_POSTCLOSE_ONLY')
            results.append({
                'code': code, 'date': day.strftime('%Y-%m-%d'),
                'status': status, 'close': item.close, 'low': item.low,
                'ma20': ma20.iloc[i], 'env20_20_lower': lower20.iloc[i],
                'env20_10_lower': lower10.iloc[i],
                'env20_20_low_touch': bool(touch20.iloc[i]) if status == 'DAILY_POSTCLOSE_ONLY' else None,
                'env20_10_low_touch': bool(touch10.iloc[i]) if status == 'DAILY_POSTCLOSE_ONLY' else None,
                'env20_20_first_daily_touch': bool(first20.iloc[i]) if status == 'DAILY_POSTCLOSE_ONLY' and prev_observed.iloc[i] else None,
                'ma112': ma112.iloc[i], 'ma224': ma224.iloc[i],
                'close_below_ma112': bool(item.close < ma112.iloc[i]) if pd.notna(ma112.iloc[i]) and valid.iloc[i] else None,
                'close_below_ma224': bool(item.close < ma224.iloc[i]) if pd.notna(ma224.iloc[i]) and valid.iloc[i] else None,
                'amount_억원': float(item.amount / 1e8) if valid.iloc[i] else np.nan,
                'amount500_research_only': bool(item.amount >= 50e9) if valid.iloc[i] else None,
                'kospi200_asof': ('UNKNOWN' if membership_keys is None else
                                 'YES' if (code, day) in membership_keys else 'NO_OR_MISSING'),
                'intraday_first_touch_price': 'UNKNOWN',
                'official_source_verified': False,
            })
    return pd.DataFrame(results)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--daily', required=True, help='Daily source CSV; source must be independently validated')
    p.add_argument('--members', help='Optional verified as-of-day membership CSV: code,date,is_member')
    p.add_argument('--out', required=True, help='Isolated output directory')
    args = p.parse_args()
    output = Path(args.out)
    output.mkdir(parents=True, exist_ok=True)
    result = analyze(pd.read_csv(args.daily, dtype={'code': str}),
                     pd.read_csv(args.members, dtype={'code': str}) if args.members else None)
    result.to_csv(output / 'envelope_daily_audit.csv', index=False, encoding='utf-8-sig')
    good = result[result.status.eq('DAILY_POSTCLOSE_ONLY')]
    summary = {
        'research_only': True,
        'eligible_daily_bars': len(good),
        'env20_20_low_touch_days': int(good.env20_20_low_touch.eq(True).sum()),
        'env20_10_low_touch_days': int(good.env20_10_low_touch.eq(True).sum()),
        'legacy_10_touch_but_lecture_20_not_touch': int((good.env20_10_low_touch.eq(True) & good.env20_20_low_touch.eq(False)).sum()),
        'source_independently_verified': False,
        'intraday_executable_touch_proven': False,
        'profitability_test_performed': False,
        'warning': 'No current KOSPI200 membership substitution; membership missing vs nonmember cannot be distinguished by absent roster rows.',
    }
    (output / 'summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
