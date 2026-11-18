#!/usr/bin/env python3
"""Research-only ENV10/ENV20 frozen-300 D+1-open forward-path diagnostic.

Uses the pre-existing, outcome-blind 300-event PRICE CONTEXT pilot.  It is
NOT a full-market backtest; its D+ prices are NOT independently certified.
No operational modules are imported.  No trading strategy is selected.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import sys

import pandas as pd

REVISION = 'ENV10_ENV20_FROZEN_300_FORWARD_PATH_DIAGNOSTIC_R2'
EXPECTED_PILOT_SHA256 = 'b6d1fd1f4e4fdab6680bc78705825ff8b9775b20adb14b4aa8e05b55a004742d'
HORIZONS = (1, 3, 5, 10, 20)
ROUND_TRIP_COST_BPS = (10, 20, 50)
SIGNAL_STATUS = 'D0_POSTCLOSE_ONLY'
PRICE_FIELDS = ('open', 'high', 'low', 'close')


def valid_price(row: pd.Series) -> bool:
    try:
        values = [float(row[field]) for field in PRICE_FIELDS]
        o, h, l, c = values
        return (int(row['valid_price']) == 1 and all(math.isfinite(v) and v > 0 for v in values)
                and l <= min(o, h, c) and h >= max(o, l, c))
    except (KeyError, ValueError, TypeError, OverflowError):
        return False


def ma20(rows: pd.DataFrame, end: int) -> float | None:
    window = rows.loc[rows.D.between(end-19, end)]
    if len(window) != 20 or window.D.nunique() != 20 or not all(
        valid_price(row) for _, row in window.iterrows()
    ):
        return None
    mean = float(window.close.mean())
    return mean if math.isfinite(mean) and mean > 0 else None


def classify(event: dict, panel: pd.DataFrame) -> dict:
    """Signal depends on D<=0 only; future bars affect only forward paths."""
    e = {**event, 'signal_status': None, 'env10_touch': None,
         'env20_touch': None, 'env10_first_touch': None,
         'env20_first_touch': None, 'signal_group': 'UNKNOWN',
         'forward_paths': {}}
    code, date = event['code'], event['date']
    rows = panel.loc[panel.code.eq(code) & panel.date.eq(date)].copy()
    if rows.D.duplicated().any():
        e['signal_status'] = 'UNKNOWN_DUPLICATED_D'
        return e
    d0 = rows.loc[rows.D.eq(0)]
    if len(d0) != 1 or not valid_price(d0.iloc[0]):
        e['signal_status'] = 'UNKNOWN_D0'
        return e
    base = ma20(rows, 0)
    if base is None:
        e['signal_status'] = 'UNKNOWN_MA20'
        return e
    low = float(d0.iloc[0]['low'])
    ten, twenty = low <= 0.90 * base, low <= 0.80 * base
    if twenty and not ten:
        raise AssertionError('ENV20_MUST_BE_SUBSET_ENV10')
    e.update(signal_status=SIGNAL_STATUS, env10_touch=bool(ten),
             env20_touch=bool(twenty),
             signal_group=('ENV20_BOTH' if twenty else 'ENV10_ONLY' if ten else 'NO_TOUCH'))
    previous = rows.loc[rows.D.eq(-1)]
    prev_ma = ma20(rows, -1)
    if len(previous) == 1 and valid_price(previous.iloc[0]) and prev_ma is not None:
        previous_low = float(previous.iloc[0]['low'])
        e['env10_first_touch'] = bool(ten and previous_low > 0.90 * prev_ma)
        e['env20_first_touch'] = bool(twenty and previous_low > 0.80 * prev_ma)
    # All D+ path checks are downstream of the independently frozen D0 signal.
    for horizon in HORIZONS:
        e['forward_paths'][str(horizon)] = forward_path(rows, date, horizon)
    return e


def forward_path(rows: pd.DataFrame, signal_date: str, horizon: int) -> dict:
    """D+1 OPEN entry, D+h CLOSE exit; unknowns never become zero returns."""
    d = rows.loc[rows.D.between(1, horizon)].sort_values('D')
    if len(d) != horizon or list(d.D) != list(range(1, horizon+1)):
        return {'status': 'UNKNOWN_MISSING_PATH_BARS'}
    if not all(valid_price(row) for _, row in d.iterrows()):
        return {'status': 'UNKNOWN_INVALID_PATH_BARS'}
    dates = d.trade_date.astype(str).tolist()
    if dates != sorted(set(dates)) or dates[0] <= signal_date:
        return {'status': 'UNKNOWN_OUT_OF_ORDER_PRICE_DATES'}
    entry, exit_close = float(d.iloc[0]['open']), float(d.iloc[-1]['close'])
    gross = 100.0 * (exit_close/entry - 1.0)
    lows, highs = [float(v) for v in d.low], [float(v) for v in d.high]
    # Constant round-trip bps are explicit modeling assumptions, not fills.
    return {
        'status': 'PROVISIONAL_FROZEN_PILOT_PRICES_NOT_INDEPENDENTLY_VERIFIED',
        'entry_date': dates[0], 'exit_date': dates[-1],
        'entry_open': entry, 'exit_close': exit_close,
        'gross_return_pct': round(gross, 6),
        'net_return_pct_by_total_cost_bps': {
            str(bps): round(gross-bps/100.0, 6) for bps in ROUND_TRIP_COST_BPS
        },
        'mae_gross_pct': round(100.0*(min(lows)/entry-1.0), 6),
        'mfe_gross_pct': round(100.0*(max(highs)/entry-1.0), 6),
    }


def aggregate(events: list[dict]) -> dict:
    result = {}
    for group in ('ENV10_ONLY', 'ENV20_BOTH', 'NO_TOUCH', 'UNKNOWN'):
        member = [r for r in events if r['signal_group'] == group]
        out = {'event_count': len(member), 'horizons': {}}
        for h in HORIZONS:
            known = [e['forward_paths'][str(h)] for e in member
                     if e['forward_paths'].get(str(h), {}).get('status', '').startswith('PROVISIONAL_')]
            values = [r['net_return_pct_by_total_cost_bps']['20'] for r in known]
            out['horizons'][str(h)] = {
                'available': len(known), 'unknown': len(member)-len(known),
                'provisional_mean_net_20bps_pct': round(float(pd.Series(values).mean()), 6) if values else None,
                'provisional_median_net_20bps_pct': round(float(pd.Series(values).median()), 6) if values else None,
                'provisional_positive_net_20bps_count': sum(v > 0 for v in values),
                'provisional_mean_mae_gross_pct': round(float(pd.Series(
                    [r['mae_gross_pct'] for r in known]).mean()), 6) if known else None,
                'provisional_mean_net_pct_by_cost_bps': {
                    str(b): round(float(pd.Series([
                        r['net_return_pct_by_total_cost_bps'][str(b)] for r in known]).mean()), 6)
                    if known else None for b in ROUND_TRIP_COST_BPS
                },
            }
        result[group] = out
    return result


def study(panel: pd.DataFrame, pilot_hash: str, fullmarket_manifest: dict) -> dict:
    if pilot_hash != EXPECTED_PILOT_SHA256:
        raise ValueError('PILOT_SOURCE_HASH_CHANGED_RE_REVIEW_REQUIRED')
    records = panel.loc[panel.D.eq(0), ['code','date','year','liquidity_tier']]
    if len(records) != 300 or records[['code','date']].duplicated().any():
        raise ValueError('EXPECTED_FROZEN_300_DISTINCT_EVENTS')
    events = [classify({'code': str(e.code), 'date': str(e.date), 'year': int(e.year),
                        'liquidity_tier': str(e.liquidity_tier)}, panel)
              for e in records.sort_values(['date','code']).itertuples(index=False)]
    signal_known = [e for e in events if e['signal_status'] == SIGNAL_STATUS]
    if any(e['env20_touch'] and not e['env10_touch'] for e in signal_known):
        raise AssertionError('NON_NESTED_SIGNALS')
    return {
        'revision': REVISION, 'research_only': True,
        'observed_at_utc': datetime.now(timezone.utc).isoformat(),
        'frozen_pilot_sha256': pilot_hash,
        'frozen_event_count': len(events), 'signal_eligible': len(signal_known),
        'signal_unknown': len(events)-len(signal_known),
        'env10_touch_count': sum(e['env10_touch'] is True for e in events),
        'env20_touch_count': sum(e['env20_touch'] is True for e in events),
        'first_touch_env10': sum(e['env10_first_touch'] is True for e in events),
        'first_touch_env20': sum(e['env20_first_touch'] is True for e in events),
        'signal_definition': 'D0_LOW_LE_D0_FINISHED_MA20_TIMES_0P90_OR_0P80_POSTCLOSE',
        'entry_exit_definition': 'D_PLUS_1_OPEN_TO_D_PLUS_H_CLOSE',
        'horizons': list(HORIZONS), 'assumed_total_round_trip_cost_bps': list(ROUND_TRIP_COST_BPS),
        'same_300_frozen_events_for_all_comparisons': True,
        'sample_origin': 'OUTCOME_BLIND_PRICE_ONLY_300_EVENT_PILOT_NOT_UNBIASED_FULL_MARKET',
        'fullmarket_source_queue_requests': fullmarket_manifest.get('n_source_requests'),
        'fullmarket_source_retrieved': fullmarket_manifest.get('data_retrieved'),
        'fullmarket_pit_certified': not fullmarket_manifest.get('not_pit_certified', True),
        'market_regime_status': 'UNKNOWN_NO_PIT_KOSPI_KOSDAQ_INDEX_LEVELS',
        'officially_independent_D0_and_Dplus_reference_certified': False,
        'intraday_order_fills_estimated': False, 'portfolio_backtest_computed': False,
        'source_and_sample_block_strategy_conclusion': True,
        'minimum_independent_env20_events_for_comparison': 30,
        'minimum_sample_met_for_env20': sum(e['env20_touch'] is True for e in events) >= 30,
        'out_of_sample_verification': False,
        'performance_comparison_status': 'DESCRIPTIVE_UNCERTIFIED_300_PILOT_ONLY',
        'strategy_winner': None, 'can_publish_strategy_performance': False,
        'can_modify_live_search_watch': False,
        'group_descriptive_stats': aggregate(events), 'events': events,
    }


def run(repo: Path, out: Path) -> dict:
    sys.path.insert(0, str(repo / 'research_projects' / 'closebet_source_parity_20261117'))
    from historical_case_audit import load_panel
    panel, sha = load_panel(repo)
    manifest_file = repo / 'research_inputs/R1D27D_FULLMARKET_SOURCE_QUEUE_MANIFEST.json'
    manifest = json.loads(manifest_file.read_text(encoding='utf-8'))
    if manifest.get('data_retrieved') is not False or manifest.get('not_pit_certified') is not True:
        raise ValueError('FULL_MARKET_MANIFEST_CHANGED_RE_REVIEW_REQUIRED')
    report = study(panel, sha, manifest)
    out.mkdir(parents=True, exist_ok=True)
    (out / 'ENV10_ENV20_FROZEN_300_FORWARD_PATH_RESEARCH.json').write_text(
        json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False) + '\n', encoding='utf-8')
    flat = []
    for event in report['events']:
        for h in HORIZONS:
            path = event['forward_paths'].get(str(h), {'status': 'UNKNOWN_SIGNAL_NO_FORWARD_PATH'})
            flat.append({k: v for k, v in event.items() if k != 'forward_paths'} |
                        {'horizon': h, 'path_status': path['status'],
                         'net20bps_pct': path.get('net_return_pct_by_total_cost_bps', {}).get('20'),
                         'mae_pct': path.get('mae_gross_pct')})
    pd.DataFrame(flat).to_csv(out/'ENV10_ENV20_FROZEN_300_FORWARD_PATHS.csv',
                              index=False, encoding='utf-8-sig')
    print(json.dumps({k:v for k,v in report.items() if k not in ('events','observed_at_utc')},
                     ensure_ascii=False, indent=2, allow_nan=False))
    return report


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--repo', default='.')
    p.add_argument('--out', required=True)
    a = p.parse_args()
    run(Path(a.repo), Path(a.out))
