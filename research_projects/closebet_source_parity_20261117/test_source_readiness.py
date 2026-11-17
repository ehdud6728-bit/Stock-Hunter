import unittest

import pandas as pd

from source_readiness import compare_daily, normalize


def daily():
    return pd.DataFrame([
        dict(code='005930', date='2026-11-16', open=100, high=108, low=98,
             close=105, volume=1_000, amount=104_000, price_basis='RAW'),
        dict(code='000660', date='2026-11-16', open=50, high=52, low=49,
             close=50, volume=800, amount=40_100, price_basis='RAW'),
    ])


class SourceReadinessTests(unittest.TestCase):
    def test_matching_supplied_bars_is_not_official_certification(self):
        d = daily()
        result = compare_daily(d, d.copy())
        self.assertEqual(result['status'], 'MATCHED_SUPPLIED_BARS_ONLY_NOT_OFFICIAL_CERTIFIED')
        self.assertFalse(result['reference_independence_proven'])
        self.assertFalse(result['can_publish_strategy_performance'])

    def test_money_mismatch_does_not_pass_even_with_matching_ohlc(self):
        a, b = daily(), daily()
        a.loc[0, 'amount'] = a.loc[0, 'close'] * a.loc[0, 'volume']
        result = compare_daily(a, b)
        self.assertEqual(result['status'], 'BLOCKED_SOURCE_PARITY_NOT_PROVEN')
        self.assertEqual(result['field_mismatches'], 1)
        self.assertEqual(result['mismatch_examples'][0]['field'], 'amount')

    def test_missing_bar_is_not_a_match(self):
        result = compare_daily(daily().iloc[[0]].copy(), daily())
        self.assertEqual(result['reference_only'], 1)
        self.assertEqual(result['status'], 'BLOCKED_SOURCE_PARITY_NOT_PROVEN')

    def test_undeclared_price_adjustment_basis_fail_closed(self):
        d = daily().drop(columns='price_basis')
        result = compare_daily(d, d.copy())
        self.assertEqual(result['basis_unknown'], 2)
        self.assertEqual(result['status'], 'BLOCKED_SOURCE_PARITY_NOT_PROVEN')

    def test_mismatched_adjustment_basis_fail_closed(self):
        d1, d2 = daily(), daily()
        d2['price_basis'] = 'ADJUSTED'
        result = compare_daily(d1, d2)
        self.assertEqual(result['basis_conflicts'], 2)
        self.assertEqual(result['status'], 'BLOCKED_SOURCE_PARITY_NOT_PROVEN')

    def test_duplicate_source_bar_rejected(self):
        with self.assertRaisesRegex(ValueError, 'DUPLICATE'):
            normalize(pd.concat([daily(), daily().iloc[[0]]], ignore_index=True))

    def test_corrupted_ohlc_rejected(self):
        d = daily()
        d.loc[0, 'high'] = 90
        with self.assertRaisesRegex(ValueError, 'INVALID_OHLC'):
            normalize(d)


if __name__ == '__main__':
    unittest.main()
