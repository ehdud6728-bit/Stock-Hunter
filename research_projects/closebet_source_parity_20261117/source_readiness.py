#!/usr/bin/env python3
"""Read-only source inventory + optional independent daily-bar parity gate.

Does NOT certify officialness, historical completeness, signal quality, or
strategy profitability. All outputs remain in a separate research directory.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re

import numpy as np
import pandas as pd


ARCHIVES = {
    'allmarket_event_queue': 'research_inputs/R1D27D_ALLMARKET_85576_TICKER_DATES_SOURCE_QUEUE.csv.gz',
    'pilot_signal_window_300': 'research_inputs/R1D27R_PILOT_DMINUS60_DPLUS40_PRICE_CONTEXT.csv.gz',
    'post_event_outcomes_1500': 'research_inputs/R1D27E_POST15_FIXED_PRICE_BASE_1500.csv.gz',
    'trading_calendar_dates': 'research_inputs/R1D27D_ORIGINAL_KRX_INDEX_1907_DAYS.csv',
}
FIELDS = ('open', 'high', 'low', 'close', 'volume', 'amount')


def digest(path: Path) -> str:
    hash_ = hashlib.sha256()
    with path.open('rb') as stream:
        while block := stream.read(1024 * 1024):
            hash_.update(block)
    return hash_.hexdigest()


def _code(value: object) -> str:
    s = str(value).strip().upper().removesuffix('.0')
    # Some valid KRX preferred-share tickers end in a letter, e.g. 35320K.
    if not re.fullmatch(r'(?:[0-9]{1,6}|[0-9]{5}[A-Z])', s):
        raise ValueError('BAD_STOCK_CODE:' + s)
    return s.zfill(6) if s.isdigit() else s


def normalize(frame: pd.DataFrame) -> pd.DataFrame:
    needed = {'code', 'date', *FIELDS}
    if missing := needed - set(frame.columns):
        raise ValueError('MISSING_COLUMNS:' + ','.join(sorted(missing)))
    d = frame.copy()
    d['code'] = d['code'].map(_code)
    d['date'] = pd.to_datetime(d['date'], errors='coerce').dt.normalize()
    if d['date'].isna().any():
        raise ValueError('INVALID_DATE')
    if d.duplicated(['code', 'date']).any():
        raise ValueError('DUPLICATE_STOCK_DATE')
    for name in FIELDS:
        d[name] = pd.to_numeric(d[name], errors='coerce')
    if not np.isfinite(d[list(FIELDS)].to_numpy(dtype=float)).all():
        raise ValueError('NONFINITE_PRICE_VOLUME_AMOUNT')
    if (d[['open', 'high', 'low', 'close']] <= 0).any().any():
        raise ValueError('NONPOSITIVE_PRICE_OR_SUSPENDED_REQUIRES_SEPARATE_POLICY')
    if (d[['volume', 'amount']] < 0).any().any():
        raise ValueError('NEGATIVE_VOLUME_AMOUNT')
    if ((d.high < d[['open', 'low', 'close']].max(axis=1)) |
            (d.low > d[['open', 'high', 'close']].min(axis=1))).any():
        raise ValueError('INVALID_OHLC_EXTREMA')
    if 'price_basis' not in d.columns:
        d['price_basis'] = 'UNDECLARED'
    else:
        d['price_basis'] = d.price_basis.fillna('UNDECLARED').astype(str).str.upper()
    return d.sort_values(['code', 'date']).reset_index(drop=True)


def compare_daily(candidate: pd.DataFrame, reference: pd.DataFrame) -> dict:
    """Check supplied raw bars; does not establish reference-provider independence."""
    c = normalize(candidate)
    r = normalize(reference)
    left = c.set_index(['code', 'date'])
    right = r.set_index(['code', 'date'])
    common = left.index.intersection(right.index)
    only_candidate = left.index.difference(right.index)
    only_reference = right.index.difference(left.index)
    differences = []
    price_basis_unknown = 0
    basis_mismatch = 0
    for idx in common:
        a, b = left.loc[idx], right.loc[idx]
        if a.price_basis == 'UNDECLARED' or b.price_basis == 'UNDECLARED':
            price_basis_unknown += 1
        elif a.price_basis != b.price_basis:
            basis_mismatch += 1
        for col in FIELDS:
            if float(a[col]) != float(b[col]):
                differences.append({'code': idx[0], 'date': idx[1].strftime('%Y-%m-%d'),
                                    'field': col, 'candidate': float(a[col]),
                                    'reference': float(b[col])})
    exact = (not differences and not len(only_candidate) and not len(only_reference)
             and not price_basis_unknown and not basis_mismatch and len(common) > 0)
    return {
        'status': 'MATCHED_SUPPLIED_BARS_ONLY_NOT_OFFICIAL_CERTIFIED' if exact else 'BLOCKED_SOURCE_PARITY_NOT_PROVEN',
        'candidate_rows': len(c), 'reference_rows': len(r), 'same_keys': len(common),
        'candidate_only': len(only_candidate), 'reference_only': len(only_reference),
        'field_mismatches': len(differences), 'basis_unknown': price_basis_unknown,
        'basis_conflicts': basis_mismatch, 'mismatch_examples': differences[:20],
        'reference_independence_proven': False,
        'full_market_224_sessions_proven': False,
        'asof_security_universe_proven': False,
        'can_publish_strategy_performance': False,
    }


def archive_inventory(repo: Path) -> dict:
    entries = {}
    for key, relative in ARCHIVES.items():
        path = repo / relative
        if not path.is_file():
            entries[key] = {'path': relative, 'status': 'MISSING'}
            continue
        d = pd.read_csv(path, dtype={'code': str})
        metadata = {'path': relative, 'status': 'PRESENT_NOT_A_CERTIFIED_FULL_MARKET_DAILY_SOURCE',
                    'sha256': digest(path), 'file_bytes': path.stat().st_size,
                    'rows': len(d), 'columns': list(d.columns)}
        if 'code' in d.columns:
            metadata['code_count'] = int(d.code.nunique())
        if 'date' in d.columns:
            metadata['anchor_dates'] = int(d.date.nunique())
            metadata['first_anchor_date'] = str(d.date.min())
            metadata['last_anchor_date'] = str(d.date.max())
        if 'trade_date' in d.columns:
            metadata['window_trade_dates'] = int(d.trade_date.nunique())
        if key == 'pilot_signal_window_300':
            metadata['anchor_events'] = int(d[['code', 'date']].drop_duplicates().shape[0])
            metadata['duplicate_window_code_trade_date'] = int(d.duplicated(['code', 'trade_date']).sum())
            metadata['invalid_price_labeled'] = int(d.valid_price.ne(1).sum()) if 'valid_price' in d else None
        entries[key] = metadata
    return {
        'revision': 'CLOSEBET_ENV_SOURCE_INVENTORY_R1',
        'status': 'NOT_READY_FOR_VALID_MARKET_REGIME_BACKTEST',
        'research_only': True, 'archive_diagnostics': entries,
        'blocking_reasons': [
            'Event-date queues and selected pilot windows are not a full daily KRX market census.',
            'No independent official-vs-bot daily OHLCVA equality proof provided.',
            'Historical point-in-time index membership and contiguous 224-day history not proven.',
            'Historical as-of news/leader classification and intraday execution data not proven.',
        ],
        'comparison_performance_status': 'NOT_COMPUTED',
        'live_search_watch_order_changed': False,
    }


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--repo', default='.', help='Stock-Hunter root')
    p.add_argument('--out', required=True, help='New dedicated research result directory')
    p.add_argument('--candidate', help='Optional original bot daily OHLCVA CSV')
    p.add_argument('--reference', help='Optional independently obtained daily OHLCVA reference CSV')
    args = p.parse_args()
    if bool(args.candidate) != bool(args.reference):
        p.error('CANDIDATE_AND_REFERENCE_MUST_BE_PROVIDED_TOGETHER')
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    result = archive_inventory(Path(args.repo))
    if args.candidate:
        result['daily_source_parity'] = compare_daily(
            pd.read_csv(args.candidate, dtype={'code': str}),
            pd.read_csv(args.reference, dtype={'code': str}))
    (out / 'SOURCE_READINESS_RESEARCH_ONLY.json').write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps({'status': result['status'], 'source_items': len(result['archive_diagnostics']),
                      'has_pair_comparison': 'daily_source_parity' in result,
                      'can_publish_performance': False}, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
