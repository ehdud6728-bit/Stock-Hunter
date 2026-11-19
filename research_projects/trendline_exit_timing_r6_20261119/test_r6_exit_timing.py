"""R6 causal entry, trailing-stop and optimistic intrabar safeguards."""
import sys
import unittest
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r6_exit_timing as r6


def bars(override=None):
    overrides = override or {}
    out = []
    for i, date in enumerate(pd.bdate_range("2023-11-07", periods=20), start=1):
        vals = {"open": 100., "high": 101., "low": 99., "close": 100.}
        vals.update(overrides.get(i, {}))
        vals["date"] = date
        out.append(vals)
    return pd.DataFrame(out)


class R6SimulationTests(unittest.TestCase):
    def test_expost_peak_earliest_tie_and_giveback(self):
        b = bars({3:{"high":120., "close":116.},
                  4:{"high":120., "close":108.}})
        x = r6.opportunity(b, 100.)
        self.assertEqual(x["peak_day"], 3)
        self.assertEqual(x["mfe_gross_pct"], 20.)
        self.assertEqual(x["dplus20_giveback_percentage_points"], 20.)
        self.assertEqual(x["first_target_touch_day_expost"]["15"], 3)
        self.assertFalse(x["intraday_order_of_peak_and_low_known"])

    def test_time_rule_uses_dplus20_close_not_mfe(self):
        b = bars({4:{"high":125., "close":115.},20:{"close":96.}})
        m = r6.simulate_exit(b, 100., "TIME_D20")
        self.assertEqual(m["day"], 20)
        self.assertEqual(m["net_total20bps_pct"], -4.2)

    def test_fixed_target_touch_price_conservative(self):
        b = bars({2:{"open":112., "high":115., "low":108., "close":110.}})
        m = r6.simulate_exit(b, 100., "TARGET_10")
        self.assertEqual(m["day"], 2)
        self.assertEqual(m["price"], 110.)  # Not 112 gap price
        self.assertEqual(m["reason"], "TARGET_LIMIT_CONSERVATIVE_PRICE_ON_GAP_UP")

    def test_trailing_uses_yesterdays_completed_high(self):
        b = bars({1:{"open":100., "high":120., "low":96., "close":115.},
                  2:{"open":116., "high":117., "low":106., "close":110.}})
        m = r6.simulate_exit(b, 100., "TRAIL_10")
        self.assertEqual(m["day"], 2)
        self.assertEqual(m["price"], 108.)
        self.assertEqual(m["reason"], "TRAILING_STOP")

    def test_same_bar_stop_target_chooses_stop_and_flags(self):
        b = bars({1:{"high":100.},
                  2:{"open":100., "high":115., "low":90., "close":105.}})
        m = r6.simulate_exit(b, 100., "TARGET_10_TRAIL_8")
        self.assertEqual(m["day"], 2)
        self.assertEqual(m["price"], 92.)
        self.assertEqual(m["net_total20bps_pct"], -8.2)
        self.assertEqual(m["reason"], "STOP_FIRST_ON_DAILY_BAR")
        self.assertTrue(m["same_bar_stop_target_ambiguous"])

    def test_gap_below_stop_fills_worse_open(self):
        b = bars({1:{"open":100., "high":120., "low":95., "close":110.},
                  2:{"open":100., "high":106., "low":94., "close":102.}})
        m = r6.simulate_exit(b, 100., "TRAIL_10")
        self.assertEqual(m["day"], 2)
        self.assertEqual(m["price"], 100.)
        self.assertTrue(m["gap_through_stop_open_fill"])

    def test_line_reloss_close_exits_only_next_open(self):
        b = bars({1:{"open":100., "high":101., "low":99., "close":99.},
                  2:{"open":93., "high":97., "low":90., "close":96.}})
        m = r6.simulate_exit(b, 100., "LINE_RELOSS_NEXT_OPEN",
                              line_level_d0=102., line_slope=-1.)
        self.assertEqual(m["day"], 2)
        self.assertEqual(m["price"], 93.)

    def test_incomplete_future_unknown_not_filled(self):
        b = bars().head(5)
        for rule in r6.EXIT_RULES:
            self.assertEqual(r6.simulate_exit(b, 100., rule)["status"],
                             "UNKNOWN_INCOMPLETE_DPLUS20_PATH")
        self.assertEqual(r6.opportunity(b, 100.)["status"],
                         "UNKNOWN_INCOMPLETE_DPLUS20_PATH")

    def test_primary_dedup_not_based_on_future_performance(self):
        fake = [
            {"code":"006400", "date":"2023-11-06", "scale":"SHORT", "line_id":"S", "return":100},
            {"code":"006400", "date":"2023-11-06", "scale":"MEDIUM", "line_id":"M", "return":-100},
            {"code":"006400", "date":"2023-11-07", "scale":"LONG", "line_id":"L", "return":20},
        ]
        selected = r6.primary_events(fake)
        self.assertEqual([x["line_id"] for x in selected], ["M", "L"])

    def test_integrated_r5_parity_and_research_guards(self):
        result = r6.study()
        self.assertEqual(result["line_events"], 506)
        self.assertEqual(result["unique_stock_dates"], 368)
        self.assertEqual(result["all_lines_summary_OVERLAPS"]["complete_dplus20"], 500)
        self.assertIsNone(result["strategy_winner"])
        self.assertFalse(result["out_of_sample"])
        self.assertFalse(result["can_modify_live_search_watch"])
        self.assertFalse(result["daily_ohlc_intrabar_order_known"])


if __name__ == "__main__":
    unittest.main()
