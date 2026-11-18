import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from historical_case_audit import (add_preferred_sentinels, audit_one, fixed_sample,
                                   reference_bars, run, stock_code)


def frame(n=230):
    dates = pd.bdate_range('2025-01-01', periods=n)
    source = pd.DataFrame({
        '시가': [100] * n, '고가': [105] * n, '저가': [95] * n,
        '종가': [100] * n, '거래량': [1000] * n,
        '거래대금': [100050] * n,
    }, index=dates)
    return source


def case():
    r = reference_bars(frame())
    selected = r.tail(61)
    event_date = selected.trade_date.iloc[-1]
    panel = selected.copy()
    panel['code'] = '005930'
    panel['date'] = event_date
    panel['D'] = list(range(-60, 1))
    panel['valid_price'] = 1
    panel['MA20'] = 100.0
    panel['MA112'] = 100.0
    panel['MA224'] = 100.0
    return {'code': '005930', 'date': event_date, 'year': 2025, 'liquidity_tier': 'HIGH'}, panel, frame()


class HistoricalCaseAuditTests(unittest.TestCase):
    def test_code_keeps_leading_zero(self):
        self.assertEqual(stock_code('5930'), '005930')

    def test_preferred_share_code_remains_six_characters(self):
        self.assertEqual(stock_code('35320k'), '35320K')
        self.assertEqual(stock_code('45014K'), '45014K')

    def test_selects_first_code_per_year_tier_no_future_fields(self):
        rows = []
        for year in range(2021, 2026):
            for group, tier in enumerate(('HIGH', 'LOW', 'MID')):
                for code in (f'9{year-2020:01d}{group:01d}001', f'0{year-2020:01d}{group:01d}123'):
                    rows.append({'year': year, 'liquidity_tier': tier,
                                 'code': code, 'date': f'{year}-02-03', 'D': 0,
                                 'future_return': 100 if code.startswith('9') else -100})
        s = fixed_sample(pd.DataFrame(rows))
        self.assertEqual(len(s), 15)
        self.assertTrue(s.code.str.startswith('0').all())

    def test_incomplete_cohort_fails_before_fetch(self):
        with self.assertRaisesRegex(ValueError, 'COHORT_INCOMPLETE'):
            fixed_sample(pd.DataFrame([{'year': 2025, 'liquidity_tier': 'HIGH',
                                        'code': '005930', 'date': '2025-11-17', 'D': 0}]))

    def test_preferred_share_sentinels_are_mandatory(self):
        base = pd.DataFrame([{'year': 2025, 'liquidity_tier': 'HIGH',
                              'code': '005930', 'date': '2025-01-01'}])
        preferred = pd.DataFrame([
            {'year': 2021, 'liquidity_tier': 'HIGH', 'code': '35320K', 'date': '2021-05-24', 'D': 0},
            {'year': 2023, 'liquidity_tier': 'HIGH', 'code': '45014K', 'date': '2023-02-08', 'D': 0},
        ])
        with self.assertRaisesRegex(ValueError, 'SAMPLE_17'):
            add_preferred_sentinels(preferred, base)
        with self.assertRaisesRegex(ValueError, 'SENTINELS_CHANGED'):
            add_preferred_sentinels(preferred.iloc[[0]], base)

    def test_source_has_true_amount_not_proxy(self):
        d = reference_bars(frame())
        self.assertEqual(float(d.iloc[0].amount), 100050)
        self.assertNotEqual(float(d.iloc[0].close * d.iloc[0].volume), float(d.iloc[0].amount))

    def test_source_missing_amount_blocks(self):
        with self.assertRaisesRegex(ValueError, 'PROVIDER_MISSING_OHLCVA'):
            reference_bars(frame().drop(columns='거래대금'))

    def test_source_duplicate_days_blocked(self):
        d = frame()
        d = pd.concat([d, d.iloc[[0]]])
        with self.assertRaisesRegex(ValueError, 'DUPLICATE_REFERENCE_DATE'):
            reference_bars(d)

    def test_source_bad_extrema_blocked(self):
        d = frame()
        d.iloc[0, d.columns.get_loc('고가')] = 90
        with self.assertRaisesRegex(ValueError, 'INVALID_REFERENCE_OHLC'):
            reference_bars(d)

    def test_same_numbers_are_still_uncertified(self):
        event, panel, ref = case()
        r, examples = audit_one(event, panel, ref)
        self.assertEqual(r['matched_date_rows'], 61)
        self.assertEqual(r['reference_missing_date_rows'], 0)
        self.assertEqual(r['status'], 'NUMERIC_MATCH_UNCERTIFIED_PROVENANCE')
        self.assertEqual(r['moving_average_levels']['224']['status'], 'NUMERIC_MATCH_UNCERTIFIED_BASIS')
        self.assertFalse(examples)

    def test_actual_amount_mismatch_is_detected(self):
        event, panel, ref = case()
        panel.loc[panel.D.eq(0), 'amount'] = 100000
        r, examples = audit_one(event, panel, ref)
        self.assertEqual(r['field_mismatch_counts']['amount'], 1)
        self.assertEqual(r['status'], 'MISMATCH_OR_INCOMPLETE_CANNOT_CERTIFY')
        self.assertTrue(any(x['field'] == 'amount' for x in examples))

    def test_missing_reference_day_is_not_imputed(self):
        event, panel, ref = case()
        day = panel.loc[panel.D.eq(0), 'trade_date'].iloc[0]
        ref = ref.loc[ref.index.strftime('%Y-%m-%d') != day]
        r, _ = audit_one(event, panel, ref)
        self.assertEqual(r['reference_missing_date_rows'], 1)
        self.assertEqual(r['status'], 'MISMATCH_OR_INCOMPLETE_CANNOT_CERTIFY')

    def test_zero_date_overlap_is_fetch_blocker(self):
        event, panel, _ = case()
        with self.assertRaisesRegex(ValueError, 'NO_OVERLAPPING_REFERENCE_DATES'):
            audit_one(event, panel, frame(50))

    def test_ma224_requires_full_history(self):
        event, panel, ref = case()
        r, _ = audit_one(event, panel, ref.iloc[-210:])
        self.assertEqual(r['moving_average_levels']['224']['status'],
                         'UNKNOWN_NOT_ENOUGH_VALID_REFERENCE_HISTORY')
        self.assertEqual(r['status'], 'MISMATCH_OR_INCOMPLETE_CANNOT_CERTIFY')

    def test_ma224_difference_is_flagged(self):
        event, panel, ref = case()
        panel['MA224'] = 101.0
        r, _ = audit_one(event, panel, ref)
        self.assertEqual(r['moving_average_levels']['224']['status'],
                         'NUMERIC_MISMATCH_OR_DIFFERENT_ADJUSTMENT_BASIS')
        self.assertEqual(r['status'], 'MISMATCH_OR_INCOMPLETE_CANNOT_CERTIFY')

    def test_prelisting_na_stays_unknown_not_zero(self):
        event, panel, ref = case()
        mask = panel.D.lt(-6)
        panel.loc[mask, 'valid_price'] = 0
        panel.loc[mask, ['open', 'high', 'low', 'close', 'volume', 'amount']] = float('nan')
        r, _ = audit_one(event, panel, ref)
        self.assertEqual(r['candidate_invalid_or_prelisting_rows'], 54)
        self.assertEqual(r['matched_date_rows'], 7)
        self.assertEqual(r['status'], 'MISMATCH_OR_INCOMPLETE_CANNOT_CERTIFY')

    def test_end_to_end_frozen_panel_emits_all_17_even_when_provider_is_unusable(self):
        class FakeProvider:
            def get_market_ohlcv_by_date(self, start, end, ticker, adjusted):
                self_last_unadjusted = adjusted is False
                if not self_last_unadjusted:
                    raise ValueError('ADJUSTED_FALSE_REQUIRED')
                # Constant mock not containing the historical 2021-2024 dates:
                # should be BLOCKED, not converted into a fake success.
                return frame()

        repo = Path(__file__).resolve().parents[2]
        with TemporaryDirectory(prefix='closebet_case_audit_test_') as temp:
            r = run(repo, Path(temp), stock=FakeProvider())
            self.assertEqual(r['fixed_event_count'], 17)
            self.assertEqual(len(r['events']), 17)
            self.assertEqual(r['status'], 'BLOCKED_INCOMPLETE_OR_MISMATCH')
            self.assertGreater(r['fetch_or_panel_blocked_count'], 0)
            self.assertFalse(r['can_publish_strategy_performance'])
            self.assertTrue((Path(temp) / 'FROZEN_CASE_SELECTION.json').is_file())
            self.assertTrue((Path(temp) / 'HISTORICAL_CASE_AUDIT_RESEARCH_ONLY.json').is_file())


if __name__ == '__main__':
    unittest.main()
