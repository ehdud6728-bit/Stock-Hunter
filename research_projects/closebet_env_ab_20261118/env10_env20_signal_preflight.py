#!/usr/bin/env python3
"""Frozen 77-case ENV(20,-10%) vs ENV(20,-20%) signal-only preflight.

This code never computes returns, reads post-D0 bars, chooses a winner, imports
an operational bot, or modifies live/search/watch authority.  Daily D0 low
touches are **post-close observations**, not intraday executable entries.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import math
from pathlib import Path

import pandas as pd

REVISION = 'ENV10_ENV20_FROZEN_77_D0_PAIRED_SIGNAL_PREFLIGHT_R1'
PERIOD = 20
WINDOW = tuple(range(-20, 1))
FIELDS = ('open', 'high', 'low', 'close')


def _valid_price(row: pd.Series) -> bool:
    try:
        o, h, l, c = (float(row[k]) for k in FIELDS)
        return (int(row['valid_price']) == 1 and all(math.isfinite(v) and v > 0
                for v in (o, h, l, c)) and l <= min(o, h, c)
                and h >= max(o, l, c))
    except (TypeError, ValueError, KeyError, OverflowError):
        return False


def _average(rows: pd.DataFrame, end_day: int) -> float | None:
    selected = rows.loc[rows.D.between(end_day - (PERIOD - 1), end_day)]
    if len(selected) != PERIOD or selected.D.nunique() != PERIOD:
        return None
    if not all(_valid_price(r) for _, r in selected.iterrows()):
        return None
    result = float(pd.to_numeric(selected.close, errors='coerce').mean())
    return result if math.isfinite(result) and result > 0 else None


def classify_one(event: dict, panel: pd.DataFrame) -> dict:
    """Classify only D-20..D0; D+ values cannot affect this function."""
    required = {'code', 'date', 'D', 'valid_price', *FIELDS}
    if missing := required - set(panel.columns):
        raise ValueError('PANEL_REQUIRED_FIELDS_MISSING:' + ','.join(sorted(missing)))
    code, event_date = str(event['code']), str(event['date'])
    rows = panel.loc[(panel.code.astype(str).eq(code)) &
                     (panel.date.astype(str).eq(event_date)) &
                     (panel.D.between(-20, 0))].copy()
    out = {**event, 'signal_basis': 'D0_LOW_VS_D0_FINISHED_MA20_POSTCLOSE_ONLY',
           'source_certified': False, 'intraday_entry_certified': False,
           'status': None, 'env10_touch': None, 'env20_touch': None,
           'env10_first_touch': None, 'env20_first_touch': None,
           'ma20_d0': None, 'ma20_dminus1': None}
    if rows.D.duplicated().any() or not (rows.D == 0).any():
        out['status'] = 'UNKNOWN_MISSING_OR_DUPLICATED_D0'
        return out
    today = rows.loc[rows.D.eq(0)].iloc[0]
    if not _valid_price(today):
        out['status'] = 'UNKNOWN_INVALID_D0_PRICE'
        return out
    ma = _average(rows, 0)
    if ma is None:
        out['status'] = 'UNKNOWN_INSUFFICIENT_20_VALID_DAILY_BARS'
        return out
    out['ma20_d0'] = round(ma, 8)
    low_today = float(today['low'])
    ten = low_today <= .90 * ma
    twenty = low_today <= .80 * ma
    if twenty and not ten:
        raise AssertionError('IMPOSSIBLE_20_TOUCH_WITHOUT_10_TOUCH')
    out['env10_touch'], out['env20_touch'] = bool(ten), bool(twenty)
    out['status'] = 'D0_POSTCLOSE_OBSERVATION_ONLY'
    prev = rows.loc[rows.D.eq(-1)]
    prev_ma = _average(rows, -1)
    if len(prev) == 1 and prev_ma is not None and _valid_price(prev.iloc[0]):
        prev_low = float(prev.iloc[0]['low'])
        out['ma20_dminus1'] = round(prev_ma, 8)
        out['env10_first_touch'] = bool(ten and prev_low > .90 * prev_ma)
        out['env20_first_touch'] = bool(twenty and prev_low > .80 * prev_ma)
    # If D-20 is invalid, the current touch is still known but first-touch
    # remains UNKNOWN; a missing previous day is never assumed non-touch.
    return out


def summarize(rows: list[dict], pilot_hash: str) -> dict:
    if len(rows) != 77 or len({(r['code'], r['date']) for r in rows}) != 77:
        raise ValueError('FROZEN_77_IDENTITY_COUNT_MISMATCH')
    good = [x for x in rows if x['status'] == 'D0_POSTCLOSE_OBSERVATION_ONLY']
    return {
        'revision': REVISION, 'research_only': True,
        'observed_at_utc': datetime.now(timezone.utc).isoformat(),
        'pilot_sha256': pilot_hash, 'fixed_sample_count': 77,
        'eligible_signal_count': len(good), 'unknown_signal_count': 77-len(good),
        'env10_low_touch_count': sum(x['env10_touch'] is True for x in good),
        'env20_low_touch_count': sum(x['env20_touch'] is True for x in good),
        'env10_only_count': sum(x['env10_touch'] is True and x['env20_touch'] is False for x in good),
        'both_touch_count': sum(x['env20_touch'] is True for x in good),
        'neither_touch_count': sum(x['env10_touch'] is False for x in good),
        'env10_first_touch_count': sum(x['env10_first_touch'] is True for x in good),
        'env20_first_touch_count': sum(x['env20_first_touch'] is True for x in good),
        'first_touch_unknown_count': sum(x['env10_first_touch'] is None for x in good),
        'denominator_origin': 'FROZEN_77_FROM_PRIOR_300_EVENT_PILOT_NOT_FULL_MARKET',
        'sample_is_outcome_blind_but_not_unbiased_full_market': True,
        'source_status': 'UNCERTIFIED_PILOT_77_EARLIER_70_NUMERIC_MATCH_7_UNRESOLVED',
        'daily_touch_not_intraday_fill': True,
        'legacy_scanner_20_40_MINUS10_CLOSE_NEAR_NOT_THE_SAME_DEFINITION': True,
        'return_horizons_computed': [], 'performance_comparison_status': 'NOT_COMPUTED',
        'strategy_winner': None, 'can_publish_strategy_performance': False,
        'can_modify_live_search_watch': False,
        'events': rows,
    }


def run(repo: Path, output: Path) -> dict:
    # Import the existing frozen loader and predeclared identity selection from
    # the neighboring research project; do not alter their source or algorithm.
    import sys
    base = repo / 'research_projects' / 'closebet_source_parity_20261117'
    sys.path.insert(0, str(base))
    from historical_case_audit import load_panel
    from expanded_historical_case_audit import expanded_sample
    panel, pilot_hash = load_panel(repo)
    frozen = expanded_sample(panel)
    selection = [dict(year=int(x.year), liquidity_tier=str(x.liquidity_tier),
                      code=str(x.code), date=str(x.date)) for x in frozen.itertuples(index=False)]
    events = [classify_one(e, panel) for e in selection]
    report = summarize(events, pilot_hash)
    output.mkdir(parents=True, exist_ok=True)
    (output / 'ENV10_ENV20_PAIRED_SIGNAL_ONLY_RESEARCH.json').write_text(
        json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False) + '\n', encoding='utf-8')
    pd.DataFrame(events).to_csv(output / 'ENV10_ENV20_PAIRED_SIGNAL_ONLY_77.csv',
                                index=False, encoding='utf-8-sig')
    print(json.dumps({k: v for k, v in report.items() if k not in ('events', 'observed_at_utc')},
                     ensure_ascii=False, indent=2, allow_nan=False))
    return report


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--repo', default='.')
    p.add_argument('--out', required=True)
    a = p.parse_args()
    run(Path(a.repo), Path(a.out))


if __name__ == '__main__':
    main()
