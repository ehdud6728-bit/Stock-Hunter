"""Synthetic causality and conservative execution tests for R10."""
import sys
from pathlib import Path
import unittest

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r10_resistance_targets as r10


def bars(length=150, *, high=100., amount=1000.):
    dates = pd.bdate_range("2024-01-01", periods=length)
    return pd.DataFrame({"date": dates, "open": [100.]*length,
                         "high": [high]*length, "low": [95.]*length,
                         "close": [100.]*length, "volume": [100.]*length,
                         "amount": [amount]*length})


class ResistanceTests(unittest.TestCase):
    def test_no_prior_history_is_unknown(self):
        for method in r10.METHODS:
            self.assertTrue(r10.resistance_at_d0(bars(), 119, method)["status"].startswith("UNKNOWN"))

    def test_confirmed_two_pivots_prior_only(self):
        data = bars()
        data.loc[12, "high"] = 110.
        data.loc[35, "high"] = 110.5
        a = r10.resistance_at_d0(data, 125, "PRIOR_PIVOT_120")
        self.assertEqual(a["status"], "KNOWN_AFTER_D0_CLOSE")
        self.assertEqual(a["target"], 110.)
        # Any post-D0 future shock must have zero impact on frozen levels.
        data.loc[126:149, "high"] = 10000.
        self.assertEqual(r10.resistance_at_d0(data, 125, "PRIOR_PIVOT_120"), a)

    def test_recent_pivot_not_yet_confirmed(self):
        data = bars()
        data.loc[123, "high"] = 110.
        data.loc[124, "high"] = 110.5
        self.assertTrue(r10.resistance_at_d0(data, 125, "PRIOR_PIVOT_120")["status"].startswith("UNKNOWN"))

    def test_daily_amount_proxy_is_explicitly_not_volume_profile(self):
        data = bars(high=106., amount=1000.)
        # 120 occupied days in a bin at HLC3 ~ 104. Better than median amount.
        # Make the overhead bin heavy against a mostly low bin history.
        data.loc[:119, ["high", "low", "close"]] = [101., 99., 100.]
        data.loc[100:109, ["high", "low", "close"]] = [109., 108., 108.]
        data.loc[100:109, "amount"] = 100000.
        data.loc[125, "close"] = 100.
        value = r10.resistance_at_d0(data, 125, "DAILY_AMOUNT_HLC3_PROXY_120")
        self.assertEqual(value["status"], "KNOWN_AFTER_D0_CLOSE")
        self.assertFalse(value["actual_volume_at_price"])
        data.loc[126:, "amount"] = 1e12
        self.assertEqual(r10.resistance_at_d0(data, 125, "DAILY_AMOUNT_HLC3_PROXY_120"), value)

    def test_missing_amount_unknown(self):
        data = bars()
        data.loc[5, "amount"] = float("nan")
        self.assertEqual(r10.resistance_at_d0(data, 125, "DAILY_AMOUNT_HLC3_PROXY_120")["status"],
                         "UNKNOWN_INVALID_OR_MISSING_TRADING_AMOUNT")


class ExitTests(unittest.TestCase):
    def test_target_limit_touch_less_20bps(self):
        data = bars(20)
        data.loc[0, "high"] = 111.
        x = r10.simulate_target(data, 100., 110.)
        self.assertEqual(x["reason"], "FROZEN_TARGET_LIMIT_TOUCH")
        self.assertAlmostEqual(x["net_total20bps_pct"], 9.8)

    def test_entry_above_frozen_target_marked_unknown(self):
        self.assertEqual(r10.simulate_target(bars(20), 105., 104.)["status"],
                         "UNKNOWN_ENTRY_ALREADY_AT_OR_ABOVE_FROZEN_TARGET")

    def test_same_bar_ambiguity_uses_stop_first(self):
        data = bars(20)
        data.loc[0, ["high", "low"]] = [115., 80.]
        x = r10.simulate_target(data, 100., 110., atr_d0=5., with_atr_stop=True)
        self.assertEqual(x["exit_price"], 85.)
        self.assertTrue(x["same_bar_stop_target_ambiguous"])

    def test_gap_through_stop_fills_open(self):
        data = bars(20)
        data.loc[0, ["open", "high", "low", "close"]] = [80., 100., 79., 90.]
        x = r10.simulate_target(data, 100., 110., atr_d0=5., with_atr_stop=True)
        self.assertEqual(x["exit_price"], 80.)
        self.assertTrue(x["gap_stop"])

    def test_gap_above_target_conservatively_fills_target(self):
        data = bars(20)
        data.loc[1, ["open", "high", "low", "close"]] = [120., 122., 110., 121.]
        x = r10.simulate_target(data, 100., 110.)
        self.assertEqual(x["exit_price"], 110.)
        self.assertEqual(x["exit_day"], 2)

    def test_fallback_d20_close(self):
        data = bars(20)
        x = r10.simulate_target(data, 100., 120.)
        self.assertEqual(x["exit_day"], 20)
        self.assertEqual(x["reason"], "TIME_D20_FALLBACK")
        self.assertAlmostEqual(x["net_total20bps_pct"], -.2)

    def test_incomplete_path_unknown(self):
        self.assertEqual(r10.simulate_target(bars(19), 100., 105.)["status"],
                         "UNKNOWN_INCOMPLETE_DPLUS20_PATH")


if __name__ == "__main__":
    unittest.main()
