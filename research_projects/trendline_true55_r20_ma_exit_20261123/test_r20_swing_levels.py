"""Offline causal pivot-confirmation and pre-entry target tests."""
import unittest

import pandas as pd

import r20_swing_levels as swing


def frame(n=100):
    return pd.DataFrame({"open": [10.] * n, "close": [10.] * n,
                         "high": [11.] * n, "low": [9.] * n})


class SwingTests(unittest.TestCase):
    def test_pivot_not_available_before_two_right_bars(self):
        data = frame()
        data.loc[25, "high"] = 20.
        highs, _ = swing.confirmed_pivots(data)
        self.assertIn((27, 20.), highs)
        self.assertIsNone(swing.frozen_upper_target(highs, 26, 12.))
        self.assertEqual(swing.frozen_upper_target(highs, 27, 12.), 20.)

    def test_upper_target_must_be_above_entry_not_already_broken(self):
        highs = [(10, 25.), (20, 11.)]
        self.assertEqual(swing.frozen_upper_target(highs, 25, 12.), 25.)
        self.assertIsNone(swing.frozen_upper_target(highs, 25, 30.))

    def test_confirmed_swing_low_breach_exits_following_open(self):
        data = frame()
        data.loc[22, "low"] = 7.
        data.loc[25, "close"] = 15.
        data.loc[26, "open"] = 12.
        data.loc[26, "close"] = 6.
        data.loc[27, "open"] = 8.
        result = swing.swing_exits(data, 25, swing.confirmed_pivots(data))
        self.assertEqual(result["swing_low_only"]["exit_day"], 2)
        self.assertEqual(result["swing_low_only"]["net_pct"], round(100 * (8 / 12 - 1) - .2, 6))
        self.assertIsNone(result["swing_high_only_if_eligible"])

    def test_upper_target_uses_close_not_intraday_high(self):
        data = frame()
        data.loc[15, "high"] = 20.
        data.loc[25, "close"] = 15.
        data.loc[26, "open"] = 12.
        data.loc[26, "high"] = 22.
        data.loc[26, "close"] = 19.
        result = swing.swing_exits(data, 25, swing.confirmed_pivots(data))
        self.assertEqual(result["frozen_pre_entry_upper_target"], 20.)
        self.assertFalse(result["target_hit"])

    def test_upper_target_close_hit_trades_next_open(self):
        data = frame()
        data.loc[15, "high"] = 20.
        data.loc[25, "close"] = 15.
        data.loc[26, "open"] = 12.
        data.loc[26, "close"] = 21.
        data.loc[27, "open"] = 22.
        result = swing.swing_exits(data, 25, swing.confirmed_pivots(data))
        self.assertTrue(result["target_hit"])
        self.assertEqual(result["swing_low_or_upper_target"]["exit_day"], 2)
        self.assertEqual(result["swing_low_or_upper_target"]["net_pct"], round(100*(22/12-1)-.2, 6))

    def test_future_swing_high_cannot_set_entry_target(self):
        data = frame()
        data.loc[26, "high"] = 100.
        data.loc[25, "close"] = 15.
        data.loc[26, "open"] = 12.
        result = swing.swing_exits(data, 25, swing.confirmed_pivots(data))
        self.assertIsNone(result["frozen_pre_entry_upper_target"])


if __name__ == "__main__":
    unittest.main()
