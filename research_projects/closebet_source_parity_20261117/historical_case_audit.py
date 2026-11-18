#!/usr/bin/env python3
"""Fixed, retrospective 15+2-case source-parity audit. Research only.

This compares a frozen, outcome-agnostic sample of the old 300-event input
with a separately requested pykrx/KRX-derived unadjusted price series.
Neither a shared data lineage nor official KRX independent certification is
proved by using pykrx. The audit NEVER computes trading performance.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import importlib.metadata
import json
from pathlib import Path
import re

import numpy as np
import pandas as pd

PILOT = 'research_inputs/R1D27R_PILOT_DMINUS60_DPLUS40_PRICE_CONTEXT.csv.gz'
REVISION = 'CB_ENV_HISTORICAL_FIXED_17_CASE_SOURCE_AUDIT_R1'
PERIODS = (20, 112, 224)
FIELDS = ('open', 'high', 'low', 'close', 'volume', 'amount')
KRX_NAMES = {'시가': 'open', '고가': 'high', '저가': 'low', '종가': 'close',
             '거래량': 'volume', '거래대금': 'amount'}


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def stock_code(v: object) -> str:
    s = str(v).strip().upper().removesuffix('.0')
    # KRX listings include alphanumeric preferred-share codes (e.g. 35320K).
    if not re.fullmatch(r'(?:[0-9]{1,6}|[0-9]{5}[A-Z])', s):
        raise ValueError('INVALID_CODE')
    return s.zfill(6) if s.isdigit() else s


def load_panel(repo: Path) -> tuple[pd.DataFrame, str]:
    path = repo / PILOT
    data = path.read_bytes()
    d = pd.read_csv(path, dtype={'code': str})
    required = {'code', 'date', 'trade_date', 'D', 'year', 'liquidity_tier',
                'valid_price', 'MA20', 'MA112', 'MA224', *FIELDS}
    if missing := required - set(d.columns):
        raise ValueError('PILOT_REQUIRED_COLUMN_MISSING:' + ','.join(sorted(missing)))
    d['code'] = d.code.map(stock_code)
    for col in ('date', 'trade_date'):
        d[col] = pd.to_datetime(d[col], errors='raise').dt.strftime('%Y-%m-%d')
    if d.duplicated(['code', 'date', 'D']).any():
        raise ValueError('DUPLICATE_PANEL_EVENT_RELATIVE_DAY')
    if d.loc[d.D.eq(0), ['code', 'date']].duplicated().any():
        raise ValueError('DUPLICATE_SIGNAL_DAY_EVENT')
    return d, sha256_bytes(data)


def fixed_sample(panel: pd.DataFrame) -> pd.DataFrame:
    """Exactly one lexicographically first event per 5 years x 3 tiers.

    Selection must not use performance, source-matching, later survival or
    today's ticker availability. Missing historical quotes are a result.
    """
    events = panel.loc[panel.D.eq(0), ['year', 'liquidity_tier', 'code', 'date']]
    if events.duplicated(['code', 'date']).any():
        raise ValueError('DUPLICATE_EVENTS')
    expected = {(y, tier) for y in range(2021, 2026) for tier in ('LOW', 'MID', 'HIGH')}
    present = {(int(x), str(y)) for x, y in
               events[['year', 'liquidity_tier']].itertuples(index=False, name=None)}
    if present != expected:
        raise ValueError('PANEL_5YEAR_X_3TIER_COHORT_INCOMPLETE')
    sample = (events.sort_values(['year', 'liquidity_tier', 'code', 'date'])
              .drop_duplicates(['year', 'liquidity_tier'], keep='first')
              .reset_index(drop=True))
    if len(sample) != 15:
        raise AssertionError('SAMPLE_NOT_15')
    return sample


def add_preferred_sentinels(panel: pd.DataFrame, sample: pd.DataFrame) -> pd.DataFrame:
    """Audit both nonnumeric KRX preferred-share codes in the frozen panel."""
    preferred = (panel.loc[panel.D.eq(0) & panel.code.str.contains(r'[A-Z]', regex=True),
                           ['year', 'liquidity_tier', 'code', 'date']]
                 .sort_values(['code', 'date']).drop_duplicates('code'))
    # A changed source must not quietly make this safety sample disappear.
    if set(preferred.code) != {'35320K', '45014K'}:
        raise ValueError('EXPECTED_PREFERRED_SHARE_SENTINELS_CHANGED')
    result = pd.concat([sample, preferred], ignore_index=True)
    if result[['code', 'date']].duplicated().any() or len(result) != 17:
        raise ValueError('FROZEN_CASE_SAMPLE_17_NOT_DISTINCT')
    return result


def reference_bars(raw: pd.DataFrame) -> pd.DataFrame:
    """No fill, no resample, no repair, no Close*Volume amount proxy."""
    if not isinstance(raw, pd.DataFrame) or raw.empty:
        raise ValueError('EMPTY_PROVIDER_RESPONSE')
    if missing := set(KRX_NAMES) - set(raw.columns):
        raise ValueError('PROVIDER_MISSING_OHLCVA:' + ','.join(sorted(missing)))
    d = raw[list(KRX_NAMES)].rename(columns=KRX_NAMES).copy()
    d['trade_date'] = pd.to_datetime(d.index, errors='raise').strftime('%Y-%m-%d')
    if d.trade_date.duplicated().any():
        raise ValueError('DUPLICATE_REFERENCE_DATE')
    for key in FIELDS:
        d[key] = pd.to_numeric(d[key], errors='coerce')
    if not np.isfinite(d[list(FIELDS)].to_numpy(dtype=float)).all():
        raise ValueError('NONFINITE_REFERENCE_VALUE')
    if (d[list(FIELDS)] < 0).any().any():
        raise ValueError('NEGATIVE_REFERENCE_VALUE')
    valid = d.open.gt(0) & d.high.gt(0) & d.low.gt(0) & d.close.gt(0)
    if ((d.loc[valid, 'high'] < d.loc[valid, ['open', 'low', 'close']].max(axis=1)) |
        (d.loc[valid, 'low'] > d.loc[valid, ['open', 'high', 'close']].min(axis=1))).any():
        raise ValueError('INVALID_REFERENCE_OHLC')
    if ((~valid) & (~(d.open.eq(0) & d.high.eq(0) & d.low.eq(0) &
                        d.volume.eq(0) & d.amount.eq(0)))).any():
        raise ValueError('UNEXPLAINED_REFERENCE_ZERO_PRICE')
    return d.sort_values('trade_date').reset_index(drop=True)


def audit_one(event: dict, panel: pd.DataFrame, source: pd.DataFrame) -> tuple[dict, list[dict]]:
    code, day = event['code'], event['date']
    rows = panel.loc[panel.code.eq(code) & panel.date.eq(day) & panel.D.between(-60, 0)]
    if len(rows) != 61 or rows.D.nunique() != 61:
        raise ValueError('MISSING_DMINUS60_D0_PANEL_ROWS:' + code + ':' + day)
    invalid_candidate = rows.valid_price.ne(1)
    if invalid_candidate.all() or rows.loc[rows.D.eq(0), 'valid_price'].iloc[0] != 1:
        raise ValueError('MISSING_VALID_SIGNAL_DAY_PRICE:' + code + ':' + day)
    # Preserve IPO/no-history rows as explicit UNKNOWN. Do not compare their NaNs
    # to price 0 or fabricate prelisting bars, and do not call them a match.
    valid_rows = rows.loc[~invalid_candidate].copy()
    source = reference_bars(source)
    joined = valid_rows.merge(source, on='trade_date', how='left',
                              suffixes=('_candidate', '_krx'),
                              validate='one_to_one', indicator=True)
    missing = joined._merge.ne('both')
    if missing.all():
        raise ValueError('NO_OVERLAPPING_REFERENCE_DATES_FOR_CASE')
    examples = []
    mismatch_fields = {k: 0 for k in FIELDS}
    for name in FIELDS:
        left = joined[f'{name}_candidate']
        right = joined[f'{name}_krx']
        changed = (~missing) & (~np.isclose(left, right, atol=0, rtol=0, equal_nan=False))
        mismatch_fields[name] = int(changed.sum())
        for row in joined.loc[changed, ['trade_date', f'{name}_candidate', f'{name}_krx']].head(4).itertuples(index=False):
            examples.append({'code': code, 'event_date': day, 'date': row[0], 'field': name,
                             'candidate': float(row[1]), 'krx_adapter': float(row[2])})
    row_today = joined.loc[joined.D.eq(0)].iloc[0]
    signal = source.loc[source.trade_date.le(day)]
    averages = {}
    for n in PERIODS:
        if len(signal) < n or (signal.tail(n).close <= 0).any() or row_today['_merge'] != 'both':
            averages[str(n)] = {'status': 'UNKNOWN_NOT_ENOUGH_VALID_REFERENCE_HISTORY'}
        else:
            ref_ma = float(signal.tail(n).close.mean())
            cand_ma = float(row_today[f'MA{n}'])
            averages[str(n)] = {'candidate': cand_ma, 'krx_unadjusted': ref_ma,
                                'absolute_difference': abs(ref_ma-cand_ma),
                                'status': 'NUMERIC_MATCH_UNCERTIFIED_BASIS' if abs(ref_ma-cand_ma) <= 0.00011
                                else 'NUMERIC_MISMATCH_OR_DIFFERENT_ADJUSTMENT_BASIS'}
    result = {
        **event, 'panel_rows_dminus60_through_d0': len(rows),
        'candidate_invalid_or_prelisting_rows': int(invalid_candidate.sum()),
        'candidate_valid_price_rows': len(valid_rows),
        'matched_date_rows': int((~missing).sum()),
        'reference_missing_date_rows': int(missing.sum()),
        'field_mismatch_counts': mismatch_fields,
        'moving_average_levels': averages,
        'source_reference_data_sha256': sha256_bytes(source.to_csv(index=False).encode('utf-8')),
        'status': 'NUMERIC_MATCH_UNCERTIFIED_PROVENANCE' if not invalid_candidate.any() and
            not missing.any() and
            not any(mismatch_fields.values()) and
            all(v['status'] == 'NUMERIC_MATCH_UNCERTIFIED_BASIS' for v in averages.values())
            else 'MISMATCH_OR_INCOMPLETE_CANNOT_CERTIFY',
    }
    return result, examples


def fetch_pykrx(stock: object, code: str, day: str) -> pd.DataFrame:
    # 550 calendar days gives the 224-session warmup buffer for ordinary markets.
    # We still check the actual count rather than assuming completeness.
    end = datetime.strptime(day, '%Y-%m-%d')
    start = end - timedelta(days=550)
    return stock.get_market_ohlcv_by_date(start.strftime('%Y%m%d'),
                                           end.strftime('%Y%m%d'), code, adjusted=False)


def run(repo: Path, out: Path, stock: object | None = None) -> dict:
    panel, pilot_hash = load_panel(repo)
    sample = add_preferred_sentinels(panel, fixed_sample(panel))
    out.mkdir(parents=True, exist_ok=True)
    selection = [dict(year=int(x.year), liquidity_tier=str(x.liquidity_tier),
                      code=str(x.code), date=str(x.date)) for x in sample.itertuples(index=False)]
    (out / 'FROZEN_CASE_SELECTION.json').write_text(json.dumps({
        'revision': REVISION, 'pilot_path': PILOT, 'pilot_sha256': pilot_hash,
        'selection_rule': 'FIRST_SORTED_CODE_DATE_PER_YEAR_2021_2025_AND_LOW_MID_HIGH_PLUS_2_PREFERRED_SENTINELS',
        'events': selection, 'selection_used_future_performance': False,
    }, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
    if stock is None:
        from pykrx import stock as stock_provider
        stock = stock_provider
    try:
        provider_version = importlib.metadata.version('pykrx')
    except importlib.metadata.PackageNotFoundError:
        provider_version = 'FAKE_OFFLINE_TEST_OR_UNKNOWN'
    details, diffs = [], []
    for event in selection:
        try:
            result, examples = audit_one(event, panel, fetch_pykrx(stock, event['code'], event['date']))
            details.append(result)
            diffs.extend(examples)
        except Exception as exc:
            # Cannot substitute other stocks or omit delisted/failed tickers.
            details.append({**event, 'status': 'BLOCKED_REFERENCE_FETCH_OR_BAD_PANEL',
                            'reason': f'{type(exc).__name__}:{str(exc)[:250]}'})
    compared = [x for x in details if x.get('matched_date_rows', 0) > 0]
    fetch_blocked = sum(x['status'] == 'BLOCKED_REFERENCE_FETCH_OR_BAD_PANEL' for x in details)
    incomplete = any(x['status'] != 'NUMERIC_MATCH_UNCERTIFIED_PROVENANCE' for x in details)
    report = {
        'revision': REVISION, 'research_only': True,
        'observed_at_utc': datetime.now(timezone.utc).isoformat(),
        'github_run_id': __import__('os').getenv('GITHUB_RUN_ID', 'LOCAL_UNATTESTED'),
        'github_sha': __import__('os').getenv('GITHUB_SHA', 'LOCAL_UNATTESTED'),
        'source': 'pykrx_KRX_DERIVED_UNADJUSTED_HISTORY_NOT_OFFICIAL_API_CERTIFICATION',
        'provider_version': provider_version, 'pilot_sha256': pilot_hash,
        'fixed_event_count': len(selection), 'compared_event_count': len(compared),
        'fetch_or_panel_blocked_count': fetch_blocked,
        'reference_shared_date_rows': sum(x.get('matched_date_rows', 0) for x in details),
        'missing_reference_date_rows': sum(x.get('reference_missing_date_rows', 0) for x in details),
        'invalid_candidate_prelisting_rows': sum(x.get('candidate_invalid_or_prelisting_rows', 0)
                                                 for x in details),
        'numerically_consistent_events': sum(x['status'] == 'NUMERIC_MATCH_UNCERTIFIED_PROVENANCE'
                                            for x in details),
        'status': 'BLOCKED_INCOMPLETE_OR_MISMATCH' if incomplete else 'NUMERIC_PILOT_PARITY_ONLY',
        'officially_independent_reference_proven': False,
        'candidate_adjustment_basis_certified': False,
        'full_market_224_sessions_and_pit_membership_proven': False,
        'can_publish_strategy_performance': False,
        'comparison_performance_status': 'NOT_COMPUTED',
        'live_search_watch_order_changed': False,
        'events': details, 'difference_examples': diffs[:60],
    }
    (out / 'HISTORICAL_CASE_AUDIT_RESEARCH_ONLY.json').write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    print(json.dumps({k: report[k] for k in ('revision', 'status', 'fixed_event_count',
                                            'compared_event_count', 'fetch_or_panel_blocked_count',
                                            'numerically_consistent_events',
                                            'can_publish_strategy_performance')},
                     ensure_ascii=False, indent=2))
    return report


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--repo', default='.')
    p.add_argument('--out', required=True)
    args = p.parse_args()
    run(Path(args.repo), Path(args.out))


if __name__ == '__main__':
    main()
