import unittest
import numpy as np
import pandas as pd

from analyze import structure_features,observation,annotate,cluster_interval


class ValidationTests(unittest.TestCase):
    def test_two_tests_leave_gap_and_trough_unobserved(self):
        prices=pd.DataFrame(dict(date=[f"d{i:02}" for i in range(20)],low=[80.]*20))
        trials=pd.DataFrame(dict(date=["d02","d10"],drop_atr=[2.,1.]))
        got=structure_features(trials,prices)
        self.assertTrue(got["drop_shrinking"])
        self.assertTrue(pd.isna(got["reapproach_faster"]))
        self.assertTrue(pd.isna(got["compression"]))

    def test_three_trials_can_show_compression_and_control(self):
        d=pd.DataFrame(dict(date=[f"d{i:02}" for i in range(20)],low=[80.]*20))
        d.loc[11:14,"low"]=85.
        trials=pd.DataFrame(dict(date=["d02","d10","d15"],drop_atr=[3.,2.,1.]))
        got=structure_features(trials,d)
        self.assertTrue(got["compression"])
        self.assertEqual(got["gap_delta"],-3)
        trials.loc[2,"drop_atr"]=4.
        self.assertFalse(structure_features(trials,d)["compression"])

    def test_later_prices_do_not_change_confirmation(self):
        d=pd.DataFrame(dict(open=[102.]*21,high=[106.]*21,low=[101.]*21,close=[105.]*21))
        before=observation(d,0,100,104,4,5)
        d.loc[6:,"close"]=70.
        self.assertEqual(observation(d,0,100,104,4,5),before)
        self.assertEqual(observation(d.iloc[:6],0,100,104,4,5),before)

    def test_trial_date_artifact_recovers_both_event_dates(self):
        dates=pd.date_range("2023-01-01",periods=70).strftime("%Y-%m-%d")
        prices=pd.DataFrame(dict(date=dates,high=[95.]*70,close=[90.]*70,
                                  low=[85.]*70,open=[89.]*70))
        prices.loc[[5,15],"high"]=110.
        trial=pd.DataFrame([dict(code="000001",band="UPPER",window=120,
                                date=dates[i],known_date=dates[i+3],high=110.,close=90.,atr=5.,
                                after_min_close=90.,drop_atr=4.,drop_pct=18.181818,
                                close_to_min_pct=0.,zone_low=100.,zone_high=110.)
                            for i in [5,15,5,15]])
        events=pd.DataFrame([dict(code="000001",date=dates[i],band="UPPER",window=120,
                                  state="OVERHEAD",low=100.,high=110.,prior_atr=5.,
                                  count=2.,test_dates=dates[5]+"|"+dates[15],d0_close=90.,
                                  d1_hold=False,d3_hold=False,d5_hold=False)
                            for i in [30,35]])
        result,repaired=annotate(events,trial,{"000001":prices})
        self.assertEqual(len(repaired),4)
        self.assertEqual(set(repaired.event_date),set(dates[[30,35]]))
        self.assertEqual(set(repaired.test_date),set(dates[[5,15]]))
        self.assertEqual(result.nonoverlap.tolist(),[True,False])

    def test_empty_comparison_is_not_zero_effect(self):
        d=pd.DataFrame(dict(code=["A"],flag=[True],value=[.5]))
        self.assertTrue(np.isnan(cluster_interval(d,"flag","value")["delta_pp"]))


if __name__=="__main__":
    unittest.main()
