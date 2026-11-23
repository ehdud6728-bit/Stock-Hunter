"""Offline, deterministic, hindsight-safe tests for R21 research-only exits."""
import unittest

import pandas as pd

import r21_phase_exit as r21


def frame(n=130, t=60):
    f = pd.DataFrame({"date":pd.date_range("2022-01-03",periods=n,freq="B").strftime("%Y-%m-%d"),
        "open":[10.0]*n,"high":[11.0]*n,"low":[9.0]*n,
        "close":[10.0]*n,"volume":[1000]*n})
    f.loc[t,"low"]=8.0
    return f


class R21Tests(unittest.TestCase):
    def test_base_ma5_close_is_next_open_not_same_close(self):
        f=frame()
        f.loc[61,"close"]=9.0
        f.loc[62,"open"]=7.4
        got=r21.simulate(f,60,r21.BASE)
        self.assertEqual(got["exit_day"],2)
        self.assertEqual(got["net_pct"],round(100*(7.4/10-1)-.2,6))

    def test_two_consecutive_must_wait_for_second_close(self):
        f=frame()
        f.loc[61,"close"]=9.0
        f.loc[62,"close"]=9.0
        f.loc[63,"open"]=8.5
        got=r21.simulate(f,60,"MA5_TWO_CONSECUTIVE_CLOSES")
        self.assertEqual(got["exit_day"],3)
        self.assertEqual(got["exit_reason"],"TWO_CONSECUTIVE_MA5_BREAKS")

    def test_one_close_below_then_recovery_does_not_trigger_confirmed_exit(self):
        f=frame()
        f.loc[61,"close"]=9.0
        self.assertEqual(r21.simulate(f,60,"MA5_TWO_CONSECUTIVE_CLOSES")["exit_day"],60)

    def test_d0_low_is_structural_emergency_exit_at_next_open(self):
        f=frame()
        f.loc[61,"close"]=7.0
        f.loc[62,"open"]=6.8
        for m in r21.METHODS[2:]:
            got=r21.simulate(f,60,m)
            self.assertEqual(got["exit_day"],2)
            self.assertEqual(got["exit_reason"],"D0_LOW_CLOSE_BREAK")
            self.assertEqual(got["net_pct"],round(100*(6.8/10-1)-.2,6))

    def test_atr_phase_switch_uses_signal_day_history_and_next_close(self):
        f=frame()
        f.loc[61,"close"]=13.0
        f.loc[62,"close"]=9.0
        f.loc[63,"open"]=8.3
        got=r21.simulate(f,60,"D0_LOW_THEN_MA5_AFTER_1ATR")
        self.assertEqual(got["switch_day"],1)
        self.assertEqual(got["exit_day"],3)
        self.assertEqual(got["exit_reason"],"POST_CONFIRMATION_MA5_CLOSE_BREAK")

    def test_new_higher_low_needs_two_right_bars_confirmation(self):
        f=frame()
        f.loc[61:66,"low"]=[10.0,10.0,9.5,9.6,9.7,9.9]
        f.loc[65,"close"]=11.0
        self.assertFalse(r21.confirmed_higher_low_at_close(f,60,64,8.0))
        self.assertTrue(r21.confirmed_higher_low_at_close(f,60,65,8.0))
        f.loc[66,"close"]=9.0
        f.loc[67,"open"]=8.6
        got=r21.simulate(f,60,"D0_LOW_THEN_MA5_AFTER_CONFIRMED_HIGHER_LOW")
        self.assertEqual(got["switch_day"],5)
        self.assertEqual(got["exit_day"],7)

    def test_future_price_cannot_rewrite_first_exit(self):
        f=frame()
        f.loc[61,"close"]=9.0
        f.loc[62,"close"]=9.0
        f.loc[63,"open"]=8.5
        first=r21.simulate(f,60,"MA5_TWO_CONSECUTIVE_CLOSES")
        f.loc[64:125,"close"]=1000.0
        f.loc[64:125,"high"]=9999.0
        f.loc[64:125,"low"]=0.01
        self.assertEqual(first,r21.simulate(f,60,"MA5_TWO_CONSECUTIVE_CLOSES"))

    def test_early_features_do_not_include_future_outcome_labels(self):
        f=frame()
        first=r21.early_path_diagnostics(f,60)["first5_known_flags"]
        f.loc[67:120,"high"]=9999.0
        f.loc[67:120,"close"]=10000.0
        self.assertEqual(first,r21.early_path_diagnostics(f,60)["first5_known_flags"])

    def test_incomplete_history_fails_closed(self):
        with self.assertRaises(ValueError):
            r21.simulate(frame(n=100),60,r21.BASE)
        with self.assertRaises(ValueError):
            r21.simulate(frame(),60,"FUTURE_MFE_TARGET")


if __name__=="__main__":
    unittest.main()
