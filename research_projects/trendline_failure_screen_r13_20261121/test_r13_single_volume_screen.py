"""R13 fixed-filter and no-lookahead invariants."""
import importlib.util
import unittest
from pathlib import Path

SOURCE = Path(__file__).with_name("r13_single_volume_screen.py")
spec = importlib.util.spec_from_file_location("r13_single_volume_screen", SOURCE)
r13 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(r13)


def event(t, ratio, ret, *, code="A"):
    is_target = ret > 0
    return {
        "code": code, "signal_date": f"2023-01-{t+1:02d}",
        "idx": t, "year": 2023, "line_id": "frozen",
        "asof_d0_features": {"d0_volume_vs_pre20": ratio},
        "horizons_EXPOST": {"20": {
            "status": "DESCRIPTIVE_COMPLETE_NOT_LIVE_EXECUTION",
            "predeclared_bracket_models": {"TAKE_10_STOP_5": {
                "net_total20bps_pct": ret,
                "hit_target": is_target, "hit_stop": not is_target,
                "exit_reason": "TARGET_INTRADAY" if is_target else "STOP_INTRADAY",
            }},
        }},
    }


class R13Tests(unittest.TestCase):
    def test_filter_is_at_least_one_not_optimized(self):
        self.assertFalse(r13.passes_signal_day_screen(event(1, .999999, 1)))
        self.assertTrue(r13.passes_signal_day_screen(event(1, 1.0, 1)))
        self.assertTrue(r13.passes_signal_day_screen(event(1, 1.000001, 1)))
        self.assertEqual(r13.FILTER_THRESHOLD, 1.0)

    def test_missing_signal_day_feature_is_not_imputed(self):
        self.assertFalse(r13.passes_signal_day_screen(event(1, None, 1)))
        x = event(1, .5, 1)
        x["asof_d0_features"].clear()
        self.assertFalse(r13.passes_signal_day_screen(x))

    def test_future_return_cannot_affect_screen_decision(self):
        a = event(1, 1.1, -5.2)
        b = event(1, 1.1, 9.8)
        self.assertEqual(r13.passes_signal_day_screen(a), r13.passes_signal_day_screen(b))

    def test_screen_before_nonoverlap_can_shift_selected_event(self):
        r12 = r13.r12_module(SOURCE.parents[2])
        rows = [event(0, .8, -5.2), event(10, 1.2, 9.8), event(21, 1.2, 9.8)]
        result = r13.compare_cohort(rows, r12)
        self.assertEqual(result["baseline_20bar_nonoverlap"]["n_events"], 2)
        self.assertEqual(result["screened_then_20bar_nonoverlap"]["n_events"], 1)
        self.assertEqual(result["screened_event_keys"][0][1], "2023-01-11")
        self.assertFalse(result["same_selected_stock_dates_after_screen"])

    def test_report_retains_stop_target_counts_and_assumed_net(self):
        s = r13.metric([event(0, 1., -5.2), event(25, 1.1, 9.8)])
        self.assertEqual(s["n_complete"], 2)
        self.assertEqual(s["target_hit"], 1)
        self.assertEqual(s["stop_hit"], 1)
        self.assertEqual(s["mean_net_pct"], 2.3)

    def test_frozen_r12_source_hash_checked(self):
        self.assertEqual(r13.r12_module(SOURCE.parents[2]).COST_BPS, 20)


if __name__ == "__main__":
    unittest.main()
