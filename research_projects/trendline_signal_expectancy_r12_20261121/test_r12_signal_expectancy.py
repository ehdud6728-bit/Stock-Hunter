"""Focused invariants for R12; synthetic prices never replace frozen research inputs."""
import importlib.util
import json
import math
import unittest
from pathlib import Path

import pandas as pd

MODULE_PATH = Path(__file__).with_name("r12_signal_expectancy.py")
SPEC = importlib.util.spec_from_file_location("r12_signal_expectancy", MODULE_PATH)
r12 = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(r12)


def bars(n=27):
    return pd.DataFrame({
        "date": pd.date_range("2024-01-02", periods=n, freq="B"),
        "open": [100.0] * n, "high": [110.0] * n,
        "low": [90.0] * n, "close": [100.0] * n,
    })


def signal(idx, code="A", scale="MEDIUM", line_id="line"):
    return {"code": code, "date": f"t{idx}", "idx": idx,
            "scale": scale, "line_id": line_id}


class R12Invariants(unittest.TestCase):
    def test_entry_is_next_open_exit_is_h_close_and_cost_only_once(self):
        p = bars()
        p.loc[0, "close"] = 50.0
        p.loc[1, "open"] = 80.0
        p.loc[5, "close"] = 88.0
        result = r12.path_at_close(p, 0, 5)
        self.assertEqual(result["entry_open"], 80.0)
        self.assertEqual(result["exit_close"], 88.0)
        self.assertEqual(result["gross_pct"], 10.0)
        self.assertEqual(result["net_total20bps_pct"], 9.8)
        self.assertEqual(result["gap_from_signal_close_pct"], 60.0)
        self.assertFalse(result["same_day_high_low_order_known"])

    def test_d0_close_cannot_change_entry_or_future_return(self):
        p = bars()
        a = r12.path_at_close(p, 0, 5)
        p.loc[0, "close"] = 25.0
        b = r12.path_at_close(p, 0, 5)
        self.assertEqual(a["net_total20bps_pct"], b["net_total20bps_pct"])
        self.assertNotEqual(a["gap_from_signal_close_pct"], b["gap_from_signal_close_pct"])

    def test_mfe_mae_expost_and_not_entry_day_signal_extrema(self):
        p = bars()
        p.loc[0, "high"] = 1000
        p.loc[0, "low"] = 1
        p.loc[1, "open"] = 100
        p.loc[1, "high"] = 115
        p.loc[2, "low"] = 70
        res = r12.path_at_close(p, 0, 5)
        self.assertEqual(res["mfe_high_from_entry_pct_EXPOST"], 15.0)
        self.assertEqual(res["mae_low_from_entry_pct_EXPOST"], -30.0)

    def test_target_and_stop_touch_sequence_is_not_a_peak_sale(self):
        p = bars()
        p.loc[1:5, "high"] = 103
        p.loc[1:5, "low"] = 98
        p.loc[2, "high"] = 106
        p.loc[4, "low"] = 94
        q = r12.path_at_close(p, 0, 5)
        self.assertEqual(q["first_target_touch_day_EXPOST"]["5"], 2)
        self.assertEqual(q["first_stop_touch_day_EXPOST"]["5"], 4)
        self.assertEqual(q["target_vs_stop_touch_order_EXPOST"]["TAKE_5_STOP_5"], "TARGET_BEFORE_STOP")
        self.assertEqual(q["predeclared_bracket_models"]["TAKE_5_STOP_5"]["exit_reason"], "TARGET_INTRADAY")
        self.assertEqual(q["predeclared_bracket_models"]["TAKE_5_STOP_5"]["net_total20bps_pct"], 4.8)

    def test_same_day_target_stop_ambiguous_and_conservative(self):
        p = bars()
        q = r12.path_at_close(p, 0, 5)
        self.assertEqual(q["target_vs_stop_touch_order_EXPOST"]["TAKE_10_STOP_5"], "SAME_DAILY_BAR_ORDER_UNKNOWN")
        self.assertEqual(q["predeclared_bracket_models"]["TAKE_10_STOP_5"]["exit_reason"],
                         "STOP_SAME_BAR_TARGET_AMBIGUOUS_PESSIMISTIC")
        self.assertEqual(q["predeclared_bracket_models"]["TAKE_10_STOP_5"]["net_total20bps_pct"], -5.2)

    def test_gap_below_stop_exits_at_open_not_stop(self):
        p = bars()
        p.loc[1, ["high", "low"]] = [103, 98]
        p.loc[2, ["open", "high", "low"]] = [90, 92, 88]
        q = r12.path_at_close(p, 0, 5)
        self.assertEqual(q["predeclared_bracket_models"]["TAKE_10_STOP_5"]["exit_reason"], "STOP_GAP_DOWN_OPEN")
        self.assertEqual(q["predeclared_bracket_models"]["TAKE_10_STOP_5"]["net_total20bps_pct"], -10.2)

    def test_no_touch_exits_on_predeclared_horizon_close(self):
        p = bars()
        p.loc[:, "high"] = 102
        p.loc[:, "low"] = 98
        p.loc[5, "close"] = 101
        q = r12.path_at_close(p, 0, 5)
        self.assertEqual(q["predeclared_bracket_models"]["TAKE_10_STOP_5"]["exit_reason"], "TIME_CLOSE_NO_BARRIER")
        self.assertEqual(q["predeclared_bracket_models"]["TAKE_10_STOP_5"]["net_total20bps_pct"], 0.8)

    def test_future_right_censor_at_each_horizon(self):
        p = bars(7)
        self.assertTrue(r12.path_at_close(p, 1, 5)["status"].startswith("DESCRIPTIVE_"))
        self.assertEqual(r12.path_at_close(p, 1, 10)["status"], "UNKNOWN_RIGHT_CENSORED_FUTURE")
        self.assertEqual(r12.summarize([{"horizons_EXPOST": {"20": r12.path_at_close(p, 1, 20)}}], 20)["n_complete"], 0)

    def test_dedup_priority_is_not_future_outcome(self):
        e = [signal(8, scale="SHORT", line_id="z"),
             signal(8, scale="MEDIUM", line_id="b"),
             signal(8, scale="MEDIUM", line_id="a"),
             signal(8, scale="LONG", line_id="q")]
        self.assertEqual(len(r12.primary_signals(e)), 1)
        self.assertEqual(r12.primary_signals(e)[0]["line_id"], "a")

    def test_nonoverlap_per_stock_starts_earliest(self):
        e = [signal(t) for t in (4, 7, 23, 24, 25, 45, 46)] + [signal(5, code="B")]
        selected = r12.nonoverlap_signals(e)
        self.assertEqual([x["idx"] for x in selected if x["code"] == "A"], [4, 25, 46])
        self.assertEqual([x["idx"] for x in selected if x["code"] == "B"], [5])

    def test_reference_excludes_same_day_signals_and_has_fixed_warmup(self):
        p = bars(85)
        refs = r12.reference_days(p, {60, 62}, "A")
        self.assertEqual(refs[0]["idx"], 61)
        self.assertNotIn(62, [x["idx"] for x in refs])

    def test_reference_is_event_weighted_and_no_trial_claim(self):
        h = "20"
        def row(code, year, val):
            return {"code": code, "year": year, "horizons_EXPOST": {
                h: {"status": "DESCRIPTIVE_COMPLETE_NOT_LIVE_EXECUTION", "net_total20bps_pct": val}}}
        x = r12.year_stock_reference([row("A", 2022, 2), row("A", 2022, 4)],
                                     [row("A", 2022, 1), row("A", 2022, 3)], 20)
        self.assertEqual(x["reference_mean_net_pct"], 2)
        self.assertEqual(x["signal_minus_reference_percentage_points"], 1)
        self.assertFalse(x["is_independent_control_trial"])

    def test_invalid_and_missing_ohlc_are_not_silent_winners(self):
        p = bars()
        p.loc[3, "low"] = float("nan")
        self.assertEqual(r12.path_at_close(p, 0, 5)["status"], "UNKNOWN_INVALID_FUTURE_OHLC")
        p = bars()
        p.loc[1, "open"] = 0
        self.assertEqual(r12.path_at_close(p, 0, 5)["status"], "UNKNOWN_INVALID_D0_OR_ENTRY")

    def test_frozen_code_and_data_hashes_are_explicit(self):
        repo = MODULE_PATH.parents[2]
        r5 = r12.r5_module(repo)
        self.assertEqual(sorted(r5.SOURCES), ["006400", "039840", "071840"])
        self.assertEqual(sorted(r12.ADDITIONAL_SOURCES), ["053800", "220260"])
        for code in r12.ADDITIONAL_SOURCES:
            self.assertFalse(r12.extra_frozen_prices(repo, code).empty)


if __name__ == "__main__":
    unittest.main()
