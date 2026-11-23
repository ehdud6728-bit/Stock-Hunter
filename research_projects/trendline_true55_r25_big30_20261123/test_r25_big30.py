"""Synthetic R25 no-lookahead, opportunity versus fill and train-only tests."""
import copy
import unittest

import numpy as np
import pandas as pd

import r25_big30 as r25


def frame(t=70,n=145):
    dates=pd.date_range("2022-01-03",periods=n,freq="B").strftime("%Y-%m-%d")
    g=pd.DataFrame({"code":["F00"]*n,"date":dates,
       "open":[100.0]*n,"high":[110.0]*n,
       "low":[90.0]*n,"close":[100.0]*n,
       "volume":[1000.0]*n,"amount":[100000.0]*n})
    g.loc[t,["open","high","low","close"]]=[119.,130.,100.,125.]
    g.loc[t+1:,["open","high","low","close"]]=[120.,145.,110.,125.]
    return g


def mockrows():
    out=[]
    for i in range(149):
        if i<84:year,date=2022,"2022-06-01"
        elif i<90:year,date=2023,"2023-11-01"
        else:year,date=2024+(i%2),"2024-06-01" if i%2==0 else "2025-06-01"
        x=[.2+(i%7)*.1,.5+(i%6)*.3,.6+(i%5)*.2,(i%9)-4.,
           .1+(i%4)*.5]
        big=((i<84 and i%5==0) or (i>=90 and i%3==0))
        val=-2. if i%3==0 else 7.
        out.append({"code":f"C{i%26:04d}","date":date+"-"+str(i),
          "year":year,"d0_features_only":x,
          "outcomes":{m:{"net_pct":val+(j*.1),"exit_day":7}
                      for j,m in enumerate(r25.METHODS)},
          "labels_EXPOST_NOT_FEATURES":{
            "big30_full60":big,"full60_mfe_pct":50. if big else 7.,
            "first_intraday_high30_day":10 if big else None}})
    return out


class R25ResearchTests(unittest.TestCase):
    def test_frozen_r20_and_r22_baseline_replayed(self):
        g=frame()
        self.assertEqual(r25.simulate(g,70,r25.MA5),
                         r25.r20.ma_close_break_next_open(g,70,5))
        self.assertEqual(r25.simulate(g,70,r25.LEVEL_TWO),
                         r25.r22.policy(g,70,r25.LEVEL_TWO))

    def test_intraday_high30_is_not_a_take_profit_fill(self):
        g=frame()
        # First day high > entry * 1.3 but close nowhere near +30%.
        g.loc[71,"high"]=190.
        self.assertEqual(r25.first_high30_day(g,70,120.),1)
        x=r25.simulate(g,70,r25.FLOOR_30)
        self.assertEqual(x["exit_reason"],"WINDOW_END_CLOSE")
        self.assertFalse(x["close30_trigger"])

    def test_30pct_close_sells_only_following_open(self):
        g=frame()
        g.loc[71,"close"]=158.
        g.loc[72,"open"]=141.
        x=r25.simulate(g,70,r25.FLOOR_30)
        self.assertEqual(x["exit_day"],2)
        self.assertEqual(x["exit_reason"],"CLOSE30_TARGET")
        self.assertEqual(x["net_pct"],round(100*(141/120-1)-.2,6))
        # Close target hit doesn't prove +30% realized profit.
        self.assertLess(x["net_pct"],30)

    def test_frozen_d0_floor_is_close_based_next_open(self):
        g=frame()
        g.loc[71,"low"]=1.
        self.assertEqual(r25.simulate(g,70,r25.FLOOR_30)["exit_day"],60)
        g.loc[71,"close"]=99.
        g.loc[72,"open"]=98.
        x=r25.simulate(g,70,r25.FLOOR_30)
        self.assertEqual(x["exit_reason"],"D0_LOW_CLOSE_BREAK")
        self.assertEqual(x["exit_day"],2)

    def test_partial_15pct_next_open_weighted_two_legs_cost_once(self):
        g=frame()
        g.loc[71,["close","high"]]=[140.,142.]
        g.loc[72,"open"]=138.
        g.loc[72,["close","low"]]=[95.,90.]
        g.loc[73,"open"]=93.
        x=r25.simulate(g,70,r25.HALF_15)
        self.assertEqual(x["first_take_day"],2)
        self.assertEqual(x["exit_day"],3)
        expected=.5*(100*(138/120-1)-.2)+.5*(100*(93/120-1)-.2)
        self.assertEqual(x["net_pct"],round(expected,6))

    def test_partial_preprofit_ma5_is_identical_to_baseline(self):
        g=frame()
        g.loc[71,"close"]=98.
        a=r25.simulate(g,70,r25.MA5)
        b=r25.simulate(g,70,r25.HALF_15)
        self.assertEqual(a["net_pct"],b["net_pct"])
        self.assertEqual(a["exit_day"],b["exit_day"])
        self.assertIsNone(b["first_take_day"])

    def test_mutating_future_after_exit_cannot_change_result(self):
        g=frame()
        g.loc[71,"close"]=99.
        g.loc[72,"open"]=88.
        fixed=r25.simulate(g,70,r25.FLOOR_30)
        g.loc[76:133,["high","close"]]=[9000.,8000.]
        self.assertEqual(fixed,r25.simulate(g,70,r25.FLOOR_30))

    def test_future_prices_do_not_change_landmark_known_facts(self):
        g=frame()
        first=r25.landmark_asof(g,70,3,120.,110.)
        g.loc[74:134,["open","high","low","close"]]=[
            4000.,5000.,3000.,4100.]
        changed=r25.landmark_asof(g,70,3,120.,110.)
        for key in ("asof_return_from_d1_open_pct","asof_close_above_entry",
                    "asof_close_above_frozen55","asof_close_above_ma5"):
            self.assertEqual(first[key],changed[key])
        self.assertNotEqual(first["future_mfe_from_next_open_pct_EXPOST"],
                            changed["future_mfe_from_next_open_pct_EXPOST"])

    def test_landmark_d1_features_knowable_at_d1_close(self):
        g=frame()
        f=r25.landmark_asof(g,70,1,120.,110.)
        self.assertEqual(f["known_at"],"DPLUS1_CLOSE")
        self.assertTrue(f["asof_close_above_entry"])
        self.assertTrue(f["asof_close_above_frozen55"])

    def test_landmark_rejects_invalid_day_or_future(self):
        g=frame()
        for k in (0,2,7):
            with self.assertRaises(ValueError):
                r25.landmark_asof(g,70,k,120.,110.)

    def test_first_touch_does_not_look_at_d0_high(self):
        g=frame()
        g.loc[70,"high"]=500.
        g.loc[71:130,"high"]=125.
        self.assertIsNone(r25.first_high30_day(g,70,120.))
        g.loc[75,"high"]=180.
        self.assertEqual(r25.first_high30_day(g,70,120.),5)

    def test_source_identity_gate_rejects_non149(self):
        with self.assertRaisesRegex(ValueError,"FROZEN149"):
            r25.build(frame(),[],[],[])

    def test_training_score_is_not_review_target_leakage(self):
        rows=mockrows()
        train=rows[:84];review=rows[90:]
        clf,cut=r25.train_opportunity_model(train)
        first=r25.score_review(clf,cut,review)
        edited=copy.deepcopy(review)
        for x in edited:
            x["labels_EXPOST_NOT_FEATURES"]["big30_full60"]=not x["labels_EXPOST_NOT_FEATURES"]["big30_full60"]
            x["outcomes"][r25.MA5]["net_pct"]=99999.
        second=r25.score_review(clf,cut,edited)
        self.assertEqual([x["d0_opportunity_score_TRAINONLY"] for x in first],
                         [x["d0_opportunity_score_TRAINONLY"] for x in second])
        self.assertEqual([x["d0_selected_TRAINONLY"] for x in first],
                         [x["d0_selected_TRAINONLY"] for x in second])

    def test_selection_threshold_fixed_train25pct_not_review(self):
        rows=mockrows()
        clf,cut=r25.train_opportunity_model(rows[:84])
        val=clf.predict_proba(np.array([x["d0_features_only"] for x in rows[:84]]))[:,1]
        self.assertAlmostEqual(cut,float(np.quantile(val,.25)),places=12)

    def test_insufficient_or_single_class_train_fails(self):
        train=mockrows()[:84]
        with self.assertRaisesRegex(ValueError,"TRAIN_TOO_SMALL"):
            r25.train_opportunity_model(train[:3])
        only=copy.deepcopy(train)
        for r in only:r["labels_EXPOST_NOT_FEATURES"]["big30_full60"]=False
        with self.assertRaisesRegex(ValueError,"SINGLE_CLASS"):
            r25.train_opportunity_model(only)

    def test_recall_baseline_always_100pct_and_selected_misses_count(self):
        rows=mockrows()[90:]
        scored=[{**r,"d0_selected_TRAINONLY":i%2==0} for i,r in enumerate(rows)]
        all_=r25.recall_stats(scored)
        subset=r25.recall_stats(scored,"d0_selected_TRAINONLY")
        self.assertEqual(all_["recall_pct"],100.)
        self.assertEqual(subset["missed_opportunities_EXPOST_n"],
                         all_["opportunities_EXPOST_n"]-
                         subset["captured_opportunities_EXPOST_n"])

    def test_capture_metrics_expost_opportunity_not_realized_return(self):
        rows=mockrows()
        x=r25.opportunity_exit_capture(rows)
        n=sum(r["labels_EXPOST_NOT_FEATURES"]["big30_full60"] for r in rows)
        self.assertEqual(x[r25.MA5]["opportunity_events_n"],n)
        self.assertEqual(x[r25.MA5]["realized_net30_n"],0)
        self.assertTrue(x[r25.MA5]["not_a_limit_fill_simulation"])

    def test_empty_descriptive_never_fills_zero_mean(self):
        self.assertIsNone(r25.descriptive([],r25.MA5)["mean_net_pct"])

    def test_score_review_empty_safe(self):
        train=mockrows()[:84]
        clf,threshold=r25.train_opportunity_model(train)
        self.assertEqual(r25.score_review(clf,threshold,[]),[])


if __name__=="__main__":
    unittest.main()
