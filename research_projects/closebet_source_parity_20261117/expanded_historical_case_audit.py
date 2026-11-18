#!/usr/bin/env python3
"""Outcomes-blind 75+2 price-source audit; does not run a strategy or orders.

The previously frozen 17 events are a strict subset.  This is a second,
separate sample, not a replacement or an independent KRX certification.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import importlib.metadata
import json
from pathlib import Path

import pandas as pd

from historical_case_audit import (PILOT, add_preferred_sentinels, audit_one,
                                   fetch_pykrx, fixed_sample, load_panel)

REVISION = 'CB_ENV_HISTORICAL_75_PLUS_2_STRATIFIED_SOURCE_AUDIT_R1'
# Reserve rank 19 for the separately frozen preferred-share sentinels (both
# fall at this rank in their own strata); keep all 77 identities distinct.
RANKS = (0, 4, 9, 14, 18)
TIERS = ('LOW', 'MID', 'HIGH')
YEARS = tuple(range(2021, 2026))


def expanded_sample(panel: pd.DataFrame) -> pd.DataFrame:
    """Pick five *predefined* sorted ranks in every year/liquidity stratum.

    D0 identifiers only; never select from source availability, returns, or
    outcomes.  Do not replace invalid, delisted, or young listings.
    """
    events = panel.loc[panel.D.eq(0), ['year', 'liquidity_tier', 'code', 'date']]
    if events[['code', 'date']].duplicated().any():
        raise ValueError('DUPLICATE_D0_IDENTITIES')
    expected = {(y, t) for y in YEARS for t in TIERS}
    actual = set(zip(events.year.astype(int), events.liquidity_tier.astype(str)))
    if actual != expected:
        raise ValueError('EXPANDED_YEAR_TIER_COHORT_INCOMPLETE')
    selections = []
    for year in YEARS:
        for tier in TIERS:
            group = (events.loc[events.year.eq(year) & events.liquidity_tier.eq(tier)]
                     .sort_values(['code', 'date']).reset_index(drop=True))
            if len(group) != 20:
                raise ValueError(f'EXPECTED_20_PILOT_EVENTS_PER_STRATUM:{year}:{tier}:{len(group)}')
            selections.append(group.iloc[list(RANKS)])
    main = pd.concat(selections, ignore_index=True)
    frozen = add_preferred_sentinels(panel, fixed_sample(panel))
    sentinels = frozen.loc[frozen.code.isin(('35320K', '45014K'))].copy()
    if len(sentinels) != 2:
        raise ValueError('FROZEN_PREFERRED_SENTINELS_MISSING')
    result = pd.concat([main, sentinels], ignore_index=True)
    if len(result) != 77 or result[['code', 'date']].duplicated().any():
        raise ValueError('EXPANDED_SAMPLE_NOT_77_DISTINCT')
    frozen_keys = set(zip(frozen.code, frozen.date))
    expanded_keys = set(zip(result.code, result.date))
    if not frozen_keys <= expanded_keys:
        raise ValueError('FROZEN_17_EVENTS_NOT_PRESERVED')
    return result


def run(repo: Path, out: Path, stock: object | None = None) -> dict:
    panel, input_hash = load_panel(repo)
    sample = expanded_sample(panel)
    selection = [dict(year=int(e.year), liquidity_tier=str(e.liquidity_tier),
                      code=str(e.code), date=str(e.date)) for e in sample.itertuples(index=False)]
    out.mkdir(parents=True, exist_ok=True)
    (out / 'EXPANDED_CASE_SELECTION.json').write_text(json.dumps({
        'revision': REVISION, 'pilot_path': PILOT, 'pilot_sha256': input_hash,
        'selection_rule': 'SORTED_D0_CODE_DATE_RANKS_0_4_9_14_18_PER_YEAR_2021_2025_X_LOW_MID_HIGH_PLUS_2_FROZEN_PREFERRED',
        'fixed_ranks_zero_based': list(RANKS), 'events': selection,
        'frozen_17_events_included': True, 'selection_used_future_performance': False,
    }, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    if stock is None:
        from pykrx import stock as stock_provider
        stock = stock_provider
    try:
        provider_version = importlib.metadata.version('pykrx')
    except importlib.metadata.PackageNotFoundError:
        provider_version = 'FAKE_OFFLINE_TEST_OR_UNKNOWN'
    details, differences = [], []
    for event in selection:
        try:
            result, examples = audit_one(event, panel, fetch_pykrx(stock, event['code'], event['date']))
            details.append(result)
            differences.extend(examples)
        except Exception as exc:
            details.append({**event, 'status': 'BLOCKED_REFERENCE_FETCH_OR_BAD_PANEL',
                            'reason': f'{type(exc).__name__}:{str(exc)[:250]}'})
    per_stratum = []
    for year in YEARS:
        for tier in TIERS:
            cohort = [x for x in details if x['year'] == year and x['liquidity_tier'] == tier]
            # Preferred-share sentinels may be additional cases in this stratum.
            per_stratum.append({
                'year': year, 'liquidity_tier': tier, 'case_count_including_sentinels': len(cohort),
                'reference_matched_date_rows': sum(x.get('matched_date_rows', 0) for x in cohort),
                'unavailable_reference_date_rows': sum(x.get('reference_missing_date_rows', 0) for x in cohort),
                'candidate_invalid_prelisting_rows': sum(x.get('candidate_invalid_or_prelisting_rows', 0) for x in cohort),
                'fully_numerically_consistent_cases': sum(x['status'] == 'NUMERIC_MATCH_UNCERTIFIED_PROVENANCE' for x in cohort),
                'fetch_or_bad_panel_blocked_cases': sum(x['status'] == 'BLOCKED_REFERENCE_FETCH_OR_BAD_PANEL' for x in cohort),
            })
    blocked = sum(x['status'] == 'BLOCKED_REFERENCE_FETCH_OR_BAD_PANEL' for x in details)
    complete_count = sum(x['status'] == 'NUMERIC_MATCH_UNCERTIFIED_PROVENANCE' for x in details)
    report = {
        'revision': REVISION, 'research_only': True,
        'observed_at_utc': datetime.now(timezone.utc).isoformat(),
        'github_run_id': __import__('os').getenv('GITHUB_RUN_ID', 'LOCAL_UNATTESTED'),
        'github_sha': __import__('os').getenv('GITHUB_SHA', 'LOCAL_UNATTESTED'),
        'source': 'pykrx_KRX_DERIVED_UNADJUSTED_HISTORY_NOT_OFFICIAL_API_CERTIFICATION',
        'provider_version': provider_version, 'pilot_sha256': input_hash,
        'fixed_event_count': len(selection), 'frozen_17_subset_preserved': True,
        'reference_compared_event_count': sum(x.get('matched_date_rows', 0) > 0 for x in details),
        'fetch_or_panel_blocked_count': blocked, 'numerically_consistent_events': complete_count,
        'reference_shared_date_rows': sum(x.get('matched_date_rows', 0) for x in details),
        'missing_reference_date_rows': sum(x.get('reference_missing_date_rows', 0) for x in details),
        'invalid_candidate_prelisting_rows': sum(x.get('candidate_invalid_or_prelisting_rows', 0) for x in details),
        'status': ('NUMERIC_EXPANDED_PARITY_ONLY' if complete_count == len(details)
                   else 'BLOCKED_INCOMPLETE_OR_MISMATCH'),
        'officially_independent_reference_proven': False,
        'candidate_adjustment_basis_certified': False,
        'full_market_224_sessions_and_pit_membership_proven': False,
        'can_publish_strategy_performance': False, 'comparison_performance_status': 'NOT_COMPUTED',
        'live_search_watch_order_changed': False,
        'strata': per_stratum, 'events': details, 'difference_examples': differences[:100],
    }
    (out / 'EXPANDED_HISTORICAL_CASE_AUDIT_RESEARCH_ONLY.json').write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    print(json.dumps({k: report[k] for k in (
        'revision', 'status', 'fixed_event_count', 'reference_compared_event_count',
        'numerically_consistent_events', 'fetch_or_panel_blocked_count',
        'reference_shared_date_rows', 'can_publish_strategy_performance')}, ensure_ascii=False, indent=2))
    return report


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--repo', default='.')
    p.add_argument('--out', required=True)
    a = p.parse_args()
    run(Path(a.repo), Path(a.out))


if __name__ == '__main__':
    main()
