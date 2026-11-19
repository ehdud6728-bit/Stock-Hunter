"""Regression / causal integrity checks of independent R5 study."""
import sys
import unittest
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import r5_breakout_paths as r5
import samsung_2023_same_line_audit as focal


class R5FrozenContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.raw = focal.load_frozen()
        cls.events = r5.scan_stock(cls.raw)

    def test_exact_screenshot_adjustment_factor(self):
        _, _, audit = focal.replay(self.raw)
        self.assertEqual(audit["event_year"], 2023)
        self.assertTrue(audit["line_identity_same"])
        self.assertEqual(audit["adjustment_residuals_krw"]["close"], 0)
        self.assertEqual(audit["adjustment_residuals_krw"]["high"], 0)
        self.assertFalse(audit["historical_adjustment_provenance_certified"])

    def test_nov6_canonical_medium_line_and_price(self):
        selected = [x for x in self.events if x["date"] == "2023-11-06" and x["scale"] == "MEDIUM"]
        self.assertEqual(len(selected), 1)
        e = selected[0]
        self.assertEqual(e["line_id"], focal.EXPECTED_ID)
        self.assertEqual(e["line_anchor1"], "2023-09-19")
        self.assertEqual(e["line_anchor2"], "2023-10-18")
        self.assertEqual(e["break_line_level_d0"], 486250)
        self.assertEqual(e["cross"], r5.BREAK_CROSS)

    def test_appending_future_does_not_change_nov6_event(self):
        short = self.raw.loc[self.raw.date.le("2023-11-06")].copy()
        prefix = r5.scan_stock(short)
        keys = ("date", "scale", "line_id", "break_line_level_d0", "d0_volume_vs_pre20")
        full_event = [x for x in self.events if x["date"] == "2023-11-06"]
        prefix_event = [x for x in prefix if x["date"] == "2023-11-06"]
        self.assertEqual(
            [{k:e.get(k) for k in keys} for e in full_event],
            [{k:e.get(k) for k in keys} for e in prefix_event],
        )
        for event in prefix_event:
            for horizon in r5.HORIZONS:
                self.assertEqual(
                    event["future_paths_EXPOST"][str(horizon)]["status"],
                    "UNKNOWN_INCOMPLETE_FUTURE_PATH",
                )

    def test_post_events_never_control_source_line(self):
        for event in self.events:
            self.assertLess(event["line_known_idx"], event["idx"])
            for horizon, path in event["future_paths_EXPOST"].items():
                self.assertIn(path["status"].split("_")[0], {"PROVISIONAL", "UNKNOWN"})
            self.assertEqual(event["features_asof"], "AFTER_D0_CLOSE_NO_FUTURE_BARS")

    def test_d0_moving_average_has_full_window_or_missing(self):
        for event in self.events:
            for n in (20, 60, 112, 224):
                if event.get(f"d0_close_vs_ma{n}_pct") is None:
                    self.assertNotEqual(event.get(f"ma{n}_status"), "EXACT_RAW_D0")

    def test_separate_research_track_contract(self):
        self.assertEqual(set(r5.SOURCES), {"006400", "039840", "071840"})
        self.assertEqual(r5.COSTS, (10, 20, 50))
        self.assertEqual(r5.HORIZONS, (1, 3, 5, 10, 20))
        self.assertNotIn("ENV10", r5.BREAK_CROSS)


if __name__ == "__main__":
    unittest.main()
