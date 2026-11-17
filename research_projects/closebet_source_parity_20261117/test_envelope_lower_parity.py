import unittest

import pandas as pd

from envelope_lower_parity import analyze


def sample(length=23):
    dates = pd.bdate_range('2026-01-02', periods=length)
    return pd.DataFrame({
        'code': '005930', 'date': dates, 'open': 100., 'high': 101.,
        'low': 99., 'close': 100., 'volume': 100000., 'amount': 10e9,
    })


class EnvelopeParityTests(unittest.TestCase):
    def test_lecture_20_percent_not_the_scanner_10_percent(self):
        d = sample()
        d.loc[20, 'low'] = 85
        d.loc[21, 'low'] = 75
        out = analyze(d)
        self.assertFalse(out.iloc[20].env20_20_low_touch)
        self.assertTrue(out.iloc[20].env20_10_low_touch)
        self.assertTrue(out.iloc[21].env20_20_low_touch)
        self.assertTrue(out.iloc[21].env20_20_first_daily_touch)
        self.assertAlmostEqual(out.iloc[21].env20_20_lower, 80)
        self.assertEqual(out.iloc[21].intraday_first_touch_price, 'UNKNOWN')

    def test_future_bar_cannot_change_earlier_indicators(self):
        d = sample()
        a = analyze(d.iloc[:21].copy())
        d.loc[21, 'close'] = 20
        d.loc[21, 'low'] = 19
        b = analyze(d)
        self.assertEqual(a.iloc[20].env20_20_lower, b.iloc[20].env20_20_lower)
        self.assertEqual(a.iloc[20].env20_10_low_touch, b.iloc[20].env20_10_low_touch)

    def test_history_and_membership_do_not_default_to_pass(self):
        d = sample(19)
        out = analyze(d)
        self.assertEqual(out.iloc[-1].status, 'INSUFFICIENT_MA20_HISTORY')
        self.assertEqual(out.iloc[-1].kospi200_asof, 'UNKNOWN')
        self.assertIsNone(out.iloc[-1].env20_20_low_touch)

    def test_duplicate_days_fail_closed(self):
        d = sample(21)
        with self.assertRaisesRegex(ValueError, 'DUPLICATED'):
            analyze(pd.concat([d, d.iloc[[0]]], ignore_index=True))

    def test_invalid_bar_does_not_generate_a_touch(self):
        d = sample(25)
        d.loc[22, 'low'] = 105  # invalid OHLC
        out = analyze(d)
        self.assertEqual(out.iloc[22].status, 'INVALID_BAR')
        self.assertIsNone(out.iloc[22].env20_20_low_touch)
        self.assertEqual(out.iloc[23].status, 'INSUFFICIENT_MA20_HISTORY')


if __name__ == '__main__':
    unittest.main()
