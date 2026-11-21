"""Causal, synthetic tests: no fixed percentage thresholds, no future peeking."""
import hashlib
import unittest
from pathlib import Path

import pandas as pd

from r14_structure_exits import (
    MODELS, R12_REL, R12_SHA256, R7_REL, R7_SHA256, ROOT,
    confirmed_low_before_day, simulate_structure,
)


def prices(*, n=30, open_=110.0, high=113.0, low=108.0, close=111.0):
    return pd.DataFrame({
        "date": pd.bdate_range("2024-01-02", periods=n),
        "open": [open_] * n, "high": [high] * n,
        "low": [low] * n, "close": [close] * n,
    })


class R14StructureExitTests(unittest.TestCase):
    def test_canonical_ancestor_hashes_pinned(self):
        for relative, sha in ((R12_REL, R12_SHA256), (R7_REL, R7_SHA256)):
            self.assertEqual(hashlib.sha256((ROOT / relative).read_bytes()).hexdigest(), sha)

    def test_skips_entry_if_original_line_not_held(self):
        df = prices(open_=99, low=98, high=101, close=100)
        for model in MODELS:
            got = simulate_structure(df, 3, 100, 0, model)
            self.assertTrue(got["status"].startswith("SKIPPED_"))

    def test_intraday_touches_frozen_line_without_percent_stop(self):
        df = prices()
        df.loc[4, "low"] = 99  # t=3, first entry day
        got = simulate_structure(df, 3, 100, 0, MODELS[0])
        self.assertEqual(got["exit_reason"], "TOUCHED_PREKNOWN_STRUCTURE_STOP")
        self.assertEqual(got["exit_day"], 1)
        self.assertEqual(got["exit_price"], 100)
        self.assertAlmostEqual(got["net_total20bps_pct"], (100 / 110 - 1) * 100 - .2, places=5)

    def test_gap_through_stop_fills_at_actual_open(self):
        df = prices()
        df.loc[5, ["open", "high", "low", "close"]] = [92, 99, 90, 93]
        got = simulate_structure(df, 3, 100, 0, MODELS[0])
        self.assertEqual(got["exit_reason"], "GAP_BELOW_PREKNOWN_STRUCTURE_STOP_OPEN")
        self.assertEqual(got["exit_day"], 2)
        self.assertEqual(got["exit_price"], 92)

    def test_close_based_break_is_executed_next_open(self):
        df = prices()
        df.loc[4, ["open", "high", "low", "close"]] = [110, 112, 98, 99]
        df.loc[5, ["open", "high", "low", "close"]] = [96, 100, 93, 98]
        got = simulate_structure(df, 3, 100, 0, MODELS[1])
        self.assertEqual(got["exit_reason"], "PRIOR_CLOSE_BELOW_STRUCTURE_NEXT_OPEN")
        self.assertEqual(got["exit_day"], 2)
        self.assertEqual(got["exit_price"], 96)

    def test_pivot_not_confirmed_until_two_right_bars_complete(self):
        df = prices()
        df.loc[5, "low"] = 103  # lower than low=108 on either side
        self.assertIsNone(confirmed_low_before_day(df, 5, 6))
        self.assertEqual(confirmed_low_before_day(df, 5, 7), 103)

    def test_pivot_raise_can_trigger_only_after_confirmation(self):
        df = prices(low=106)
        df.loc[5, ["open", "high", "low", "close"]] = [110, 113, 103, 111]
        df.loc[6, "low"] = 106
        df.loc[7, "low"] = 106
        df.loc[8, ["open", "high", "low", "close"]] = [110, 113, 102, 111]
        got = simulate_structure(df, 3, 100, 0, MODELS[0])
        self.assertEqual(got["exit_day"], 5)
        self.assertEqual(got["stop_before_open"], 103)
        self.assertEqual(got["confirmed_pivot_raises_before_exit"], 1)

    def test_future_path_changes_cannot_change_prior_exit(self):
        df = prices()
        df.loc[5, ["open", "high", "low", "close"]] = [92, 99, 90, 93]
        for model in MODELS:
            reference = simulate_structure(df, 3, 100, 0, model)
            altered = df.copy()
            altered.loc[10:, ["open", "high", "low", "close"]] *= 3
            got = simulate_structure(altered, 3, 100, 0, model)
            self.assertEqual(got["exit_day"], reference["exit_day"])
            self.assertEqual(got["exit_price"], reference["exit_price"])


if __name__ == "__main__":
    unittest.main()
