"""Deterministic R23 as-of-D0 features, missingness and anti-hindsight tests."""
import unittest

import numpy as np
import pandas as pd

import r23_entry_quality as r23


def history(n=360,t=260):
    dates=pd.date_range("2020-01-02",periods=n,freq="B").strftime("%Y-%m-%d")
    g=pd.DataFrame({"code":["T0001"]*n,"date":dates,
       "open":[9.0]*n,"high":[10.0]*n,"low":[8.0]*n,
       "close":[9.0]*n,"volume":[100.0]*n,"amount":[100.0]*n})
    g.loc[t,["open","high","low","close"]]=[12.0,15.0,11.0,14.0]
    g.loc[t,"volume"]=300.0
    g.loc[t,"amount"]=300.0
    return g


class R23CausalTests(unittest.TestCase):
    def test_true55_breakout_is_based_on_pre_d0_highs(self):
        g=history()
        f=r23.feature_asof_d0(g,260)
        self.assertEqual(f["prior55_high_frozen"],10.0)
        self.assertEqual(f["asof"],g.date.iloc[260])
        self.assertEqual(f["factors"]["d0_breakout_amount_spike"],"YES")

    def test_future_prices_volume_amount_never_change_d0_features(self):
        g=history()
        initial=r23.feature_asof_d0(g,260)
        g.loc[261:359,["open","high","low","close","volume","amount"]]=[
            10000.0,10001.0,9000.0,9999.0,99999.0,9999999.0]
        self.assertEqual(initial,r23.feature_asof_d0(g,260))

    def test_d0_volume_changes_spike_but_not_pre20_accumulation(self):
        g=history()
        old=r23.feature_asof_d0(g,260)
        g.loc[260,"amount"]=50.0
        nxt=r23.feature_asof_d0(g,260)
        self.assertEqual(old["pre20_amount_vs_previous60_ratio"],
                         nxt["pre20_amount_vs_previous60_ratio"])
        self.assertEqual(old["factors"]["d0_breakout_amount_spike"],"YES")
        self.assertEqual(nxt["factors"]["d0_breakout_amount_spike"],"NO")

    def test_fixed_pre_windows_capture_wave_pullback_accumulation(self):
        g=history()
        t=260
        # First wave highs in D-30..D-11, after the D-60..D-31 base.
        g.loc[t-30:t-11,"high"]=11.5
        g.loc[t-30:t-11,"low"]=9.6
        g.loc[t-10:t-1,["open","high","low","close"]]=[10.7,10.9,10.4,10.6]
        # D-20..D-11 turnover elevated, pullback volume weak.
        g.loc[t-20:t-11,"volume"]=290.0
        g.loc[t-10:t-1,"volume"]=90.0
        g.loc[t-20:t-1,"amount"]=250.0
        feat=r23.feature_asof_d0(g,t)
        self.assertEqual(feat["factors"]["first_wave_then_pre_d0_pullback"],"YES")
        self.assertEqual(feat["factors"]["prior20_volume_and_amount_accumulation"],"YES")
        self.assertEqual(feat["factors"]["pre_d0_pullback_volume_contraction"],"YES")
        self.assertEqual(feat["factors"]["below_ma224_at_d0"],"NO")

    def test_below_224_is_d0_close_known_even_if_55_high_is_lower(self):
        g=history()
        g.loc[:199,["open","high","low","close"]]=[101.,102.,99.,100.]
        # The most recent 55 days at t=260 remain at high=10.
        feat=r23.feature_asof_d0(g,260)
        self.assertEqual(feat["prior55_high_frozen"],10.0)
        self.assertEqual(feat["factors"]["below_ma224_at_d0"],"YES")

    def test_insufficient_224_history_marks_unknown_not_false(self):
        g=history(n=150,t=65)
        feat=r23.feature_asof_d0(g,65)
        self.assertEqual(feat["factors"]["below_ma224_at_d0"],"UNKNOWN")
        self.assertEqual(feat["d0_close_vs_ma224_pct"],None)
        self.assertEqual(feat["factors"]["original_224_accum_wave_pullback_proxy"],"UNKNOWN")

    def test_insufficient_accumulation_history_not_zero(self):
        g=history(n=145,t=60)
        feat=r23.feature_asof_d0(g,60)
        self.assertEqual(feat["pre20_volume_vs_previous60_ratio"],None)
        self.assertEqual(feat["factors"]["prior20_volume_and_amount_accumulation"],"UNKNOWN")

    def test_zero_prior_volume_amount_mark_unknown(self):
        g=history()
        g.loc[180:239,["volume","amount"]]=[0.0,0.0]
        feat=r23.feature_asof_d0(g,260)
        self.assertEqual(feat["factors"]["prior20_volume_and_amount_accumulation"],"UNKNOWN")

    def test_future_change_does_not_rewrite_first_wave(self):
        g=history()
        g.loc[235,"high"]=12
        before=r23.feature_asof_d0(g,260)["factors"]["first_wave_then_pre_d0_pullback"]
        g.loc[261:350,"high"]=10000.
        after=r23.feature_asof_d0(g,260)["factors"]["first_wave_then_pre_d0_pullback"]
        self.assertEqual(before,after)

    def test_non_breakout_rejected(self):
        g=history()
        g.loc[259,"high"]=30.0
        with self.assertRaises(ValueError):
            r23.feature_asof_d0(g,260)

    def test_empty_or_single_subgroup_stats_are_valid(self):
        empty=r23.describe_outcomes([],r23.MA5)
        self.assertEqual(empty["n"],0)
        self.assertIsNone(empty["mean_net_pct"])
        only=[{"outcomes":{r23.MA5:{"net_pct":-2.0}}}]
        one=r23.describe_outcomes(only,r23.MA5)
        self.assertEqual(one["n"],1)
        self.assertIsNone(one["mean_excluding_best_pct"])

    def test_event_identity_mismatch_fails_closed(self):
        g=history()
        with self.assertRaises(ValueError):
            r23.build(g,[],[],{"same_frozen_r19_events":149})

    def test_state_missing_is_never_interpreted_as_negative(self):
        self.assertEqual(r23.state_bool(None),"UNKNOWN")
        self.assertEqual(r23.state_bool(False),"NO")
        self.assertEqual(r23.state_bool(True),"YES")


if __name__=="__main__":
    unittest.main()
