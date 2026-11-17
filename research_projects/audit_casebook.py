#!/usr/bin/env python3
"""Independent, fail-closed D0 feature audit for Stock-Hunter close betting.

This is NOT a replacement search predicate, portfolio backtest, or assertion that
the supplied prices came from KRX. It compares emitted features to independently
recomputed features from the supplied daily bars and highlights weak provenance.
Only reads input CSVs and writes a separate research-only output folder.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd


PRICE_FIELDS = ('open', 'high', 'low', 'close', 'volume', 'amount')
FEATURE_TOL = {
    'entry_price': 0.02,
    'entry_amount_b': 0.03,  # source rounds to four decimals; 0.03억원 guard
    'entry_close_loc_pct': 0.03,
    'entry_upper_wick_pct': 0.03,
    'entry_gap_pct': 0.03,
    'entry_vol20_ratio': 0.01,
    'entry_vol50_ratio': 0.01,
    'entry_amount20_ratio': 0.01,
    'entry_ma20_dist_pct': 0.03,
    'entry_ma60_dist_pct': 0.03,
}
OFFICIAL_AMOUNT_LABELS = ('KRX', 'KIS', 'OFFICIAL', 'PYKRX')
PROXY_AMOUNT_LABELS = ('CLOSE*VOLUME', 'CLOSE * VOLUME', 'CLOSE_VOLUME', 'PROXY')


def _code(x: object) -> str:
    s = str(x).strip().removesuffix('.0')
    return s.zfill(6) if s.isdigit() else s


def _number(x: object) -> float:
    try:
        return float(x)
    except (ValueError, TypeError):
        return float('nan')


def _ratio(n: float, d: float) -> float:
    return float(n / d) if np.isfinite(n) and np.isfinite(d) and d > 0 else float('nan')


def _source_class(row: pd.Series) -> str:
    label = str(row.get('amount_source', '') or '').strip().upper()
    if any(tag in label for tag in PROXY_AMOUNT_LABELS):
        return 'AMOUNT_CLOSE_TIMES_VOLUME_PROXY'
    if any(tag in label for tag in OFFICIAL_AMOUNT_LABELS):
        return 'PROVIDER_LABELED_ONLY_NEEDS_EXTERNAL_PROOF'
    return 'AMOUNT_PROVENANCE_UNKNOWN'


def audit_frames(daily: pd.DataFrame, signals: pd.DataFrame) -> pd.DataFrame:
    """Audit a signal-positive ledger; absence-of-signal/false negatives need a full census."""
    required_daily = {'code', 'date', *PRICE_FIELDS}
    required_signals = {'code', 'signal_date', 'mode'}
    if missing := required_daily - set(daily.columns):
        raise ValueError(f'DAILY_COLUMNS_MISSING:{sorted(missing)}')
    if missing := required_signals - set(signals.columns):
        raise ValueError(f'SIGNAL_COLUMNS_MISSING:{sorted(missing)}')
    d = daily.copy()
    s = signals.copy()
    d['code'] = d.code.map(_code)
    s['code'] = s.code.map(_code)
    d['date'] = pd.to_datetime(d.date, errors='coerce').dt.normalize()
    s['signal_date'] = pd.to_datetime(s.signal_date, errors='coerce').dt.normalize()
    for key in PRICE_FIELDS:
        d[key] = pd.to_numeric(d[key], errors='coerce')
    duplicates = set(zip(d.loc[d.duplicated(['code', 'date'], keep=False), 'code'],
                         d.loc[d.duplicated(['code', 'date'], keep=False), 'date']))
    results: list[dict] = []
    for item in s.to_dict('records'):
        code, day = item['code'], item['signal_date']
        rec = {'code': code, 'signal_date': str(day.date()) if pd.notna(day) else '',
               'mode': str(item.get('mode', '')), 'status': '', 'reasons': '',
               'source_class': '', 'checked_features': 0, 'mismatched_features': 0,
               'ma112': np.nan, 'ma224': np.nan, 'amount500_provided_amount_pass': '',
               'amount500_proxy_pass': '', 'amount_억원': np.nan}
        if pd.isna(day) or not code.isdigit():
            rec.update(status='INVALID_SIGNAL_IDENTITY', reasons='BAD_DATE_OR_CODE')
            results.append(rec)
            continue
        history = d[(d.code == code) & d.date.le(day)].sort_values('date')
        if history.empty or history.iloc[-1].date != day:
            rec.update(status='MISSING_SIGNAL_DAY_BAR', reasons='SIGNAL_DATE_NOT_IN_DAILY')
            results.append(rec)
            continue
        row = history.iloc[-1]
        if (code, day) in duplicates or history.duplicated('date').any():
            rec.update(status='INVALID_SOURCE_BAR', reasons='DUPLICATE_CODE_DATE_IN_HISTORY')
            results.append(rec)
            continue
        o, h, l, c, v, a = (float(row[k]) for k in PRICE_FIELDS)
        if (not np.isfinite([o, h, l, c, v, a]).all() or min(o, h, l, c) <= 0
                or v < 0 or a < 0 or h < max(o, c, l) or l > min(o, c, h)):
            rec.update(status='INVALID_SOURCE_BAR', reasons='BAD_OHLCV_AMOUNT')
            results.append(rec)
            continue
        rec['source_class'] = _source_class(row)
        rec['amount_억원'] = a / 1e8
        rec['amount500_provided_amount_pass'] = bool(a >= 50_000_000_000)
        rec['amount500_proxy_pass'] = bool(c * v >= 50_000_000_000)
        closes = history.close.to_numpy(dtype=float)
        volumes = history.volume.to_numpy(dtype=float)
        amounts = history.amount.to_numpy(dtype=float)
        for n in (112, 224):
            if len(closes) >= n and np.isfinite(closes[-n:]).all():
                rec[f'ma{n}'] = float(np.mean(closes[-n:]))
        rng = h - l
        expected = {
            'entry_price': c,
            'entry_amount_b': a / 1e8,
            'entry_close_loc_pct': (c - l) / rng * 100 if rng > 0 else np.nan,
            'entry_upper_wick_pct': (h - max(o, c)) / rng * 100 if rng > 0 else np.nan,
            'entry_gap_pct': (o / closes[-2] - 1) * 100 if len(closes) >= 2 and closes[-2] > 0 else np.nan,
            'entry_vol20_ratio': _ratio(v, np.mean(volumes[-20:])) if len(volumes) >= 20 else np.nan,
            'entry_vol50_ratio': _ratio(v, np.mean(volumes[-50:])) if len(volumes) >= 50 else np.nan,
            'entry_amount20_ratio': _ratio(a, np.mean(amounts[-20:])) if len(amounts) >= 20 else np.nan,
            'entry_ma20_dist_pct': (c / np.mean(closes[-20:]) - 1) * 100 if len(closes) >= 20 and np.mean(closes[-20:]) > 0 else np.nan,
            'entry_ma60_dist_pct': (c / np.mean(closes[-60:]) - 1) * 100 if len(closes) >= 60 and np.mean(closes[-60:]) > 0 else np.nan,
        }
        # Report *all* comparisons (including a zero observed feature value).
        mismatches: list[str] = []
        for feature, value in expected.items():
            rec[f'recomputed_{feature}'] = value
            if feature not in item or pd.isna(item[feature]) or not np.isfinite(value):
                continue
            rec['checked_features'] += 1
            claimed = _number(item[feature])
            if not np.isfinite(claimed) or abs(claimed - value) > FEATURE_TOL[feature]:
                mismatches.append(feature)
        rec['mismatched_features'] = len(mismatches)
        reasons = mismatches.copy()
        if len(closes) < 224:
            reasons.append('MA224_HISTORY_INCOMPLETE')
        if rec['source_class'] != 'PROVIDER_LABELED_ONLY_NEEDS_EXTERNAL_PROOF':
            reasons.append(rec['source_class'])
        if rec['amount500_provided_amount_pass'] != rec['amount500_proxy_pass']:
            reasons.append('500EOK_GATE_PROXY_DIFFERS')
        if mismatches:
            rec['status'] = 'FEATURE_MISMATCH'
        elif rec['checked_features'] == 0:
            rec['status'] = 'NO_EMITTED_FEATURES_FOR_COMPARISON'
        elif rec['source_class'] != 'PROVIDER_LABELED_ONLY_NEEDS_EXTERNAL_PROOF':
            rec['status'] = 'SOURCE_PROVENANCE_NOT_VERIFIED'
        else:
            rec['status'] = 'FEATURE_PARITY_ON_SUPPLIED_BARS_ONLY'
        rec['reasons'] = '|'.join(reasons)
        results.append(rec)
    return pd.DataFrame(results)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--daily', required=True, help='Independent daily OHLCV CSV with amount and amount_source')
    p.add_argument('--signals', required=True, help='Frozen emitted signal ledger (no future pnl needed)')
    p.add_argument('--out', required=True, help='New isolated research output directory')
    args = p.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    cases = audit_frames(pd.read_csv(args.daily, dtype={'code': str}),
                         pd.read_csv(args.signals, dtype={'code': str}))
    cases.to_csv(out / 'casebook.csv', index=False, encoding='utf-8-sig')
    summary = {
        'research_only': True,
        'scope': 'D0_EMITTED_POSITIVES_ONLY',
        'total_signals': len(cases),
        'status_counts': cases.status.value_counts().to_dict() if not cases.empty else {},
        '500eok_proxy_gate_disagreements': int((cases.amount500_provided_amount_pass.ne(cases.amount500_proxy_pass)
                                                 & cases.amount500_provided_amount_pass.isin([True, False])).sum()) if not cases.empty else 0,
        'known_limits': [
            'Provider labels are unverified claims: independent KRX/KIS direct source confirmation is still required.',
            'Missing signal days and false negatives require full market as-of denominator, not just emitted positives.',
            'This compares entry context only: it does not prove the full strategy predicate or trade profitability.',
            '500eok gate is evaluated for research; not asserted to be a current mandatory scanner filter.',
            'MA112/224 are context features, not independently verified buy conditions.',
        ],
        'live_search_watch_order_changed': False,
    }
    (out / 'summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
