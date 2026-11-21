"""R15 causality and ex-post scoring invariants on synthetic daily bars."""
import hashlib
import unittest

import pandas as pd

from r15_capture_regret import R14_REL, R14_SHA256, ROOT, ex_post_capture, label_incomplete_horizon, r14_module


def sample(n=70):
    return pd.DataFrame({
        "date":pd.bdate_range("2024-01-02",periods=n),
        "open":[110.0]*n,"high":[112.0]*n,
        "low":[105.0]*n,"close":[110.0]*n,
    })


class R15CaptureTests(unittest.TestCase):
    def test_parent_policy_sha_frozen(self):
        self.assertEqual(hashlib.sha256((ROOT/R14_REL).read_bytes()).hexdigest(),R14_SHA256)

    def test_incomplete_path_reports_correct_horizon_without_modifying_r14_policy(self):
        original={"status":"UNKNOWN_INCOMPLETE_DPLUS20_PATH"}
        tagged=label_incomplete_horizon(original,60)
        self.assertEqual(tagged["status"],"UNKNOWN_INCOMPLETE_DPLUS60_PATH")
        self.assertEqual(tagged["r14_original_status"],original["status"])
        self.assertEqual(original["status"],"UNKNOWN_INCOMPLETE_DPLUS20_PATH")

    def test_later_high_and_low_both_matter_not_in_preexit_order(self):
        r14=r14_module()
        d=sample()
        d.loc[5,["open","high","low","close"]]=[98.,99.,97.,98.]
        d.loc[8,["open","high","low","close"]]=[110.,140.,70.,110.]
        result=r14.simulate_structure(d,3,100,0,r14.MODELS[0],horizon=20)
        self.assertEqual(result["exit_day"],2)
        score=ex_post_capture(d,3,result,20)
        self.assertGreater(score["post_exit_later_upside_missed_pp_EXPOST"],0)
        self.assertGreater(score["post_exit_later_downside_avoided_pp_EXPOST"],0)

    def test_future_rebound_changes_regret_not_earlier_exit(self):
        r14=r14_module()
        d=sample()
        d.loc[5,["open","high","low","close"]]=[98.,99.,97.,98.]
        original=r14.simulate_structure(d,3,100,0,r14.MODELS[0],horizon=20)
        modified=d.copy()
        modified.loc[9,["open","high","low","close"]]=[110,150,90,110]
        new=r14.simulate_structure(modified,3,100,0,r14.MODELS[0],horizon=20)
        self.assertEqual(original,new)
        self.assertGreater(ex_post_capture(modified,3,new,20)["post_exit_later_upside_missed_pp_EXPOST"],
                           ex_post_capture(d,3,original,20)["post_exit_later_upside_missed_pp_EXPOST"])

    def test_same_day_extremes_not_counted_after_exit(self):
        d=sample()
        d.loc[5,["open","high","low","close"]]=[105,200,90,110]
        d.loc[6:23,["high","low"]]=[115,100]
        result={"status":"PROVISIONAL_SYNTHETIC","exit_day":2,"gross_pct":0.,
                "net_total20bps_pct":-.2}
        s=ex_post_capture(d,3,result,20)
        # The 200 intraday high on exit day affects ex-post MFE, but cannot
        # be claimed as post-exit recovery: same-day high/low ordering unknown.
        self.assertAlmostEqual(s["max_favorable_excursion_pct_EXPOST"],(200/110-1)*100,places=6)
        self.assertAlmostEqual(s["post_exit_later_upside_missed_pp_EXPOST"],(115/110-1)*100,places=6)

    def test_no_future_days_after_horizon_exit(self):
        r14=r14_module()
        d=sample()
        result=r14.simulate_structure(d,3,100,0,r14.MODELS[1],horizon=20)
        self.assertEqual(result["exit_day"],20)
        s=ex_post_capture(d,3,result,20)
        self.assertIsNone(s["post_exit_later_upside_missed_pp_EXPOST"])
        self.assertIsNone(s["post_exit_later_downside_avoided_pp_EXPOST"])

    def test_capture_never_divides_by_non_profitable_peak(self):
        d=sample()
        d.loc[:,"high"]=110.1
        d.loc[:,"low"]=109.0
        d.loc[:,"open"]=110.0
        d.loc[:,"close"]=110.0
        result={"status":"PROVISIONAL_SYNTHETIC","exit_day":2,"gross_pct":0.,
                "net_total20bps_pct":-.2}
        s=ex_post_capture(d,3,result,20)
        self.assertTrue(s["no_positive_theoretical_net_opportunity"])
        self.assertIsNone(s["net_capture_of_theoretical_positive_mfe_pct_EXPOST"])

    def test_horizon_60_keeps_same_causal_early_exit(self):
        r14=r14_module()
        d=sample()
        d.loc[5,["open","high","low","close"]]=[95,99,92,96]
        a=r14.simulate_structure(d,3,100,0,r14.MODELS[0],horizon=20)
        b=r14.simulate_structure(d,3,100,0,r14.MODELS[0],horizon=60)
        self.assertEqual(a,b)


if __name__=="__main__":
    unittest.main()
