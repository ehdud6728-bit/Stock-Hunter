"""R7 causal invariants and source-limited reproducibility guards."""
import sys
import unittest
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import r7_tail_audit as r7


def sample(override=None):
    overrides = override or {}
    rows = []
    for i, date in enumerate(pd.bdate_range("2023-01-02", periods=290)):
        row = {"date": date, "open":100., "high":101., "low":99.,
               "close":100., "volume":100., "amount":10000.}
        row.update(overrides.get(i-250, {}))
        rows.append(row)
    return pd.DataFrame(rows), 250


EVENT = {"break_line_level_d0": 100., "line_slope_per_bar": 0.}


class R7AuditTests(unittest.TestCase):
    def test_01_nonoverlap_by_stock_and_full_20_day_window(self):
        events = [
            {"code":"A", "idx":1, "line_id":"z", "future_return":-100},
            {"code":"A", "idx":21, "line_id":"a", "future_return":+100},
            {"code":"A", "idx":22, "line_id":"x", "future_return":0},
            {"code":"B", "idx":21, "line_id":"b", "future_return":-1},
        ]
        selected, removed = r7.nonoverlap(events)
        self.assertEqual([(e["code"], e["idx"]) for e in selected],
                         [("A", 1), ("A", 22), ("B", 21)])
        self.assertEqual(len(removed), 1)

    def test_02_selection_never_consults_return_or_future_completion(self):
        a = [{"code":"A", "idx":i, "line_id":"a", "future_return":i}
             for i in (1, 2, 22, 43)]
        before, _ = r7.nonoverlap(a)
        for x in a:
            x["future_return"] = -x["future_return"] * 500
            x["path_complete"] = False
        after, _ = r7.nonoverlap(a)
        self.assertEqual([x["idx"] for x in before], [x["idx"] for x in after])

    def test_03_time_d20_cost_and_path_alignment(self):
        full, t = sample({20:{"close": 96.}})
        result = r7.simulate(full, t, EVENT, "TIME_D20")
        self.assertEqual(result["exit_day"], 20)
        self.assertEqual(result["net_total20bps_pct"], -4.2)

    def test_04_static_atr_stop_gap_uses_worse_open(self):
        full, t = sample({2:{"open":90., "low":88., "high":92., "close":91.}})
        result = r7.simulate(full, t, EVENT, "INITIAL_3ATR_STOP")
        self.assertEqual(result["exit_day"], 2)
        self.assertEqual(result["exit_price"], 90.)
        self.assertTrue(result["gap_through_stop_open_fill"])

    def test_05_atr_trail_does_not_use_todays_high_to_stop_today(self):
        full, t = sample({1:{"high":120., "low":95., "close":115.}})
        result = r7.simulate(full, t, EVENT, "ATR_TRAIL_3")
        self.assertGreater(result["exit_day"], 1)

    def test_06_atr_trail_uses_prior_completed_high_and_atr(self):
        full, t = sample({1:{"high":120., "low":95., "close":115.},
                          2:{"open":112., "high":115., "low":102., "close":105.}})
        result = r7.simulate(full, t, EVENT, "ATR_TRAIL_3")
        self.assertEqual(result["exit_day"], 2)
        self.assertEqual(result["reason"], "INTRADAY_STOP_ASSUMED_FILL")
        self.assertLess(result["exit_price"], 120.)

    def test_07_volume_falling_close_sells_next_open_not_same_close(self):
        full, t = sample({1:{"close":98., "low":97., "volume":200.},
                          2:{"open":95., "low":94., "close":97.}})
        result = r7.simulate(full, t, EVENT, "VOLUME_DOWN_NEXT_OPEN")
        self.assertEqual(result["exit_day"], 2)
        self.assertEqual(result["exit_price"], 95.)
        self.assertIn("HIGH_VOLUME_DOWN_CLOSE", result["causal_votes_after_close"])

    def test_08_two_of_three_requires_two_at_same_completed_close(self):
        # Single volume vote: no same-day action.
        one, t = sample({1:{"close":99.9, "volume":200.}})
        one_result = r7.simulate(one, t, EVENT, "TWO_OF_THREE_NEXT_OPEN")
        self.assertEqual(one_result["exit_day"], 20)
        # Volume + frozen line loss: the earliest valid exit is D+2 opening.
        two, t = sample({1:{"close":97., "low":96., "volume":200.},
                         2:{"open":94., "low":93., "close":96.}})
        two_result = r7.simulate(two, t, EVENT, "TWO_OF_THREE_NEXT_OPEN")
        self.assertEqual(two_result["exit_day"], 2)
        self.assertGreaterEqual(len(two_result["causal_votes_after_close"]), 2)

    def test_09_missing_geometry_and_incomplete_path_never_filled_zero(self):
        full, t = sample()
        self.assertEqual(r7.simulate(full, t, {}, "TWO_OF_THREE_NEXT_OPEN")["status"],
                         "UNKNOWN_MISSING_ORIGINAL_LINE_GEOMETRY")
        self.assertEqual(r7.simulate(full.iloc[:t+7], t, EVENT, "TIME_D20")["status"],
                         "UNKNOWN_INCOMPLETE_DPLUS20_PATH")

    def test_10_tail_statistics_treat_losses_as_losses(self):
        x = r7.stats([-20., -5., 0., 2., 4.])
        self.assertEqual(x["n"], 5)
        self.assertEqual(x["median_pct"], 0.)
        self.assertEqual(x["worst_10pct_mean_pct"], -20.)
        self.assertEqual(x["loss_at_least_10pct_count"], 1)

    def test_11_full_frozen_population_integrity_and_research_only_gates(self):
        result = r7.study()
        self.assertEqual(result["primary_stock_dates"], 368)
        self.assertEqual(result["nonoverlap_selected"], 94)
        self.assertEqual(result["same_scenario_comparable"], 90)
        self.assertEqual(result["returns_distribution"]["TIME_D20"]["n"], 90)
        self.assertIsNone(result["strategy_winner"])
        self.assertFalse(result["can_modify_live_search_watch"])
        self.assertFalse(result["r7_screenshot_rule_formulas_available"])


if __name__ == "__main__":
    unittest.main()
