import unittest

import pandas as pd

from audit_casebook import audit_frames


def example(amount=50_000_000_000, amount_source='KRX_OFFICIAL'):
    dates = pd.bdate_range('2025-01-01', periods=230)
    daily = pd.DataFrame({
        'code': '005930', 'date': dates, 'open': 100, 'high': 110,
        'low': 90, 'close': 105, 'volume': 500_000_000,
        'amount': amount, 'amount_source': amount_source,
    })
    signals = pd.DataFrame([{
        'code': '005930', 'signal_date': dates[-1], 'mode': 'A',
        'entry_price': 105.0, 'entry_amount_b': amount / 1e8,
        'entry_close_loc_pct': 75.0, 'entry_upper_wick_pct': 25.0,
        'entry_vol20_ratio': 1.0, 'entry_vol50_ratio': 1.0,
    }])
    return daily, signals


class ForensicCasebookTest(unittest.TestCase):
    def test_matching_features_on_supplied_bars_not_independent_proof(self):
        d, s = example()
        q = audit_frames(d, s).iloc[0]
        self.assertEqual(q.status, 'FEATURE_PARITY_ON_SUPPLIED_BARS_ONLY')
        self.assertEqual(q.checked_features, 6)
        self.assertAlmostEqual(q.ma224, 105.0)

    def test_emitted_feature_disagreement_is_rejected(self):
        d, s = example()
        s.loc[0, 'entry_close_loc_pct'] = 95.0
        q = audit_frames(d, s).iloc[0]
        self.assertEqual(q.status, 'FEATURE_MISMATCH')
        self.assertIn('entry_close_loc_pct', q.reasons)

    def test_500eok_proxy_may_reverse_provided_amount_gate(self):
        d, s = example(amount=48_000_000_000)
        q = audit_frames(d, s).iloc[0]
        self.assertFalse(q.amount500_provided_amount_pass)
        self.assertTrue(q.amount500_proxy_pass)
        self.assertIn('500EOK_GATE_PROXY_DIFFERS', q.reasons)

    def test_fallback_amount_provenance_is_not_certified(self):
        d, s = example(amount_source='Close*Volume')
        q = audit_frames(d, s).iloc[0]
        self.assertEqual(q.status, 'SOURCE_PROVENANCE_NOT_VERIFIED')

    def test_duplicate_bar_cannot_silently_pass(self):
        d, s = example()
        d = pd.concat([d, d.iloc[[-1]]], ignore_index=True)
        self.assertEqual(audit_frames(d, s).iloc[0].status, 'INVALID_SOURCE_BAR')

    def test_duplicate_warmup_bar_cannot_shift_long_average(self):
        d, s = example()
        d = pd.concat([d, d.iloc[[5]]], ignore_index=True)
        self.assertEqual(audit_frames(d, s).iloc[0].status, 'INVALID_SOURCE_BAR')

    def test_missing_signal_day_cannot_pass(self):
        d, s = example()
        s.loc[0, 'signal_date'] = '2026-11-17'
        self.assertEqual(audit_frames(d, s).iloc[0].status, 'MISSING_SIGNAL_DAY_BAR')


if __name__ == '__main__':
    unittest.main()
