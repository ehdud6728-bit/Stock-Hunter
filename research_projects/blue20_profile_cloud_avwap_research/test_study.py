from types import SimpleNamespace
import unittest

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

from study import add_cloud, anchor_avwap, cleared, features_for_event, nonoverlap, policies, volume_profile


def synthetic(n=290):
    close = pd.Series(100 + np.sin(np.arange(n)/8)*5 + np.arange(n)*.1)
    d = pd.DataFrame(dict(close=close, open=close-1, low=close-2, high=close+2,
                          volume=100+np.arange(n), candle_shape="OTHER", amount_ratio20=2.0))
    d["date"] = pd.date_range("2023-01-01", periods=n).strftime("%Y-%m-%d")
    for days in [60,112,224]:
        d[f"ma{days}"] = close.rolling(days).mean()
    return add_cloud(d)


class PITTests(unittest.TestCase):
    def test_profile_conserves_volume_and_value_area(self):
        d = pd.DataFrame(dict(low=[0., 1., 4.], high=[4., 3., 4.], volume=[8., 6., 5.],
                              close=[2., 2., 4.]))
        for method in ["range", "typical"]:
            p = volume_profile(d, 4, method=method)
            self.assertAlmostEqual(p["weights"].sum(), 19)
            in_area = (p["centers"] >= p["val"]) & (p["centers"] <= p["vah"])
            self.assertGreaterEqual(p["weights"][in_area].sum(), 19*.70)
            self.assertLess(p["val"], p["poc"])
            self.assertGreater(p["vah"], p["poc"])
        np.testing.assert_allclose(volume_profile(d,4)["weights"], [2,5,5,7])

    def test_empty_trade_volume(self):
        self.assertIsNone(volume_profile(pd.DataFrame(dict(low=[10],high=[11],volume=[0]))))

    def test_gap_is_separate_from_body_break(self):
        bar = SimpleNamespace(open=12., close=14.)
        self.assertEqual(cleared(bar, 10, 11), "GAP_CLEAR")
        self.assertEqual(cleared(bar, 10, 12), "BODY_CROSS")
        self.assertEqual(cleared(bar, 10, 14), "NO_CLEAR")

    def test_cloud_value_is_displayed_26_bars_later(self):
        d = synthetic()
        j = 230
        prior = d.iloc[j-26-51:j-26+1]
        self.assertAlmostEqual(d.span_b.iloc[j], (prior.high.max()+prior.low.min())/2)
        modified = d.copy()
        modified.loc[j-25:, ["high","low"]] *= 50
        assert_frame_equal(add_cloud(modified).iloc[:j+1][["span_a","span_b"]],
                           d.iloc[:j+1][["span_a","span_b"]])

    def test_avwap_anchor_excludes_d0_low(self):
        d = synthetic()
        j = 230
        d.loc[j, "low"] = 1
        anchor, line = anchor_avwap(d.iloc[:j+1], j)
        self.assertLess(anchor, j)
        prior = d.iloc[j-60:j]
        self.assertEqual(anchor, prior.index[prior.low.eq(prior.low.min())][-1])
        trail = d.iloc[anchor:j+1]
        typical = (trail.high+trail.low+trail.close)/3
        self.assertAlmostEqual(line.iloc[-1], np.average(typical, weights=trail.volume))

    def test_future_candles_do_not_change_d0_profile_or_features(self):
        d = synthetic()
        j = 230
        e = SimpleNamespace(d0_body_abs_prior_atr20=3., d0_amount_ratio20=10.)
        before, profile = features_for_event(d,j,e)
        altered = d.copy()
        altered.loc[j+1:, ["close","open","low","high","volume"]] *= 30
        after, other = features_for_event(add_cloud(altered),j,e)
        for k in before:
            if not k.startswith("d1_"):
                if isinstance(before[k], float) and np.isnan(before[k]):
                    self.assertTrue(np.isnan(after[k]))
                else:
                    self.assertEqual(before[k], after[k], k)
        self.assertNotEqual(before["d1_close_recomputed"], after["d1_close_recomputed"])
        np.testing.assert_array_equal(profile["weights"], other["weights"])

    def test_policy_keeps_skips_and_delayed_exit_reasons(self):
        d = pd.DataFrame(dict(year=[2023]*3,band=["UPPER"]*3,gc_offset=[0,0,1],
                              baseline_net_pct=[9.75,-5.25,9.75],baseline_tp=[True,False,True],
                              baseline_exit_reason=["TP","SL","TP"],d2_net=[-5.25,9.75,np.nan],
                              d2_exit_reason=["SL_BOTH_HIT","TP",None],
                              d1_support=["NEITHER","BOTH","BOTH"]))
        result = policies(d)
        r = result[(result.year=="ALL") & result.policy.eq("D2_MID_AND_PRIOR_VAH")].iloc[0]
        self.assertEqual((r.opportunities,r.trades,r.skips,r.missed_baseline_tp,r.tp,r.sl), (2,1,1,1,1,0))
        self.assertEqual(r.net_opportunity_pct, 4.875)

    def test_nonoverlap_crosses_band_and_year(self):
        d = pd.DataFrame(dict(code=["000001"]*3,band=["UPPER","LOWER","UPPER"],
                              date=["2023-12-21","2024-01-03","2024-02-01"],
                              d20_date=["2024-01-22","2024-02-01","2024-03-04"]))
        self.assertEqual(nonoverlap(d).index.tolist(), [0,2])


if __name__ == "__main__":
    unittest.main()

