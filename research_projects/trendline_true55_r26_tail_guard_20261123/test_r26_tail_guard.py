"""R26 synthetic evidence: no future exits, gaps, large-loss tails, +30 opportunities."""
import copy
import unittest

import numpy as np
import pandas as pd

import r26_tail_guard as r26


def frame(t=70,n=140):
    g=pd.DataFrame({
      "code":["000001"]*n,
      "date":pd.date_range("2022-01-03",periods=n,freq="B").strftime("%Y-%m-%d"),
      "open":[100.]*n,"high":[110.]*n,"low":[90.]*n,
      "close":[100.]*n,"volume":[1000.]*n,"amount":[100000.]*n})
    g.loc[t,["open","high","low","close"]]=[119.,130.,100.,125.]
    g.loc[t+1:,["open","high","low","close"]]=[120.,130.,110.,125.]
    return g


def row(code="A",date="2024-01-01",big=True,first=8,
        baseline=-25.,new=-8.,exitday=4):
    out={
      r26.BASE:{"net_pct":baseline,"exit_day":exitday,
                "exit_reason":"D0_LOW_CLOSE_BREAK"},
      r26.MA10_TARGET30:{"net_pct":new,"exit_day":2,
                         "exit_reason":"MA10_CLOSE_BREAK",
                         "fill_gap_from_trigger_close_pct":-5.},
    }
    for m in r26.POLICIES+r26.REFERENCE:
        if m not in out:
            out[m]={"net_pct":baseline,"exit_day":exitday,
                    "exit_reason":"D0_LOW_CLOSE_BREAK",
                    "fill_gap_from_trigger_close_pct":-2.}
    return {"code":code,"date":date,"year":int(date[:4]),
            "outcomes":out,
            "labels_EXPOST_NOT_FEATURES":{
              "big30_full60":big,"first_intraday_high30_day":first,
              "full60_mfe_pct":40. if big else 5.},
            "mae_through_held_sessions_EXPOST_ONLY":{m:-28. for m in out}}


class TestR26FixedCausalGuards(unittest.TestCase):
    def test_baseline_r25_replay_unchanged(self):
        g=frame()
        self.assertEqual(r26.simulate(g,70,r26.BASE),
                         r26.r25.simulate(g,70,r26.BASE))

    def test_every_nonbaseline_guard_has_fixed_method(self):
        self.assertEqual(len(r26.POLICIES),6)
        self.assertEqual(len(set(r26.POLICIES)),6)
        self.assertEqual(r26.TRAIL_ATR_MULTIPLIER,2.)

    def test_intraday_30_high_not_executable_as_exit(self):
        g=frame()
        g.loc[71,"high"]=200.
        r=r26.simulate(g,70,r26.MA20_TARGET30)
        self.assertEqual(r["exit_reason"],"WINDOW_END_CLOSE")

    def test_close30_signal_sells_next_open_even_if_gap_below_30(self):
        g=frame()
        g.loc[71,["high","close"]]=[168.,160.]
        g.loc[72,"open"]=140.
        r=r26.simulate(g,70,r26.MA10_TARGET30)
        self.assertEqual(r["exit_reason"],"CLOSE30_TARGET")
        self.assertEqual(r["exit_day"],2)
        self.assertEqual(r["net_pct"],round(100*(140/120-1)-.2,6))
        self.assertLess(r["net_pct"],30.)

    def test_stop_signal_sells_next_open_after_large_gap(self):
        g=frame()
        g.loc[71,"close"]=99.
        g.loc[72,"open"]=70.
        r=r26.simulate(g,70,r26.MA5_TARGET30)
        self.assertEqual(r["exit_day"],2)
        self.assertEqual(r["exit_reason"],"MA5_CLOSE_BREAK")
        self.assertLess(r["net_pct"],-40.)
        self.assertLess(r["fill_gap_from_trigger_close_pct"],-20.)

    def test_fixed_55_level_needs_two_consecutive_lost_closes(self):
        g=frame()
        # Prior55 high=110; 1st closes below; day2 recovers; 2 later closes below.
        for k,v in [(1,108.),(2,115.),(3,108.),(4,107.)]:
            g.loc[70+k,"close"]=v
        g.loc[75,"open"]=99.
        r=r26.simulate(g,70,r26.LEVEL2_TARGET30)
        self.assertEqual(r["exit_day"],5)
        self.assertEqual(r["exit_reason"],"FROZEN55_TWO_CLOSES_BREAK")

    def test_trailing_ATR_uses_d0_frozen_ATR_and_closes_not_high(self):
        g=frame()
        g.loc[71,["high","close"]]=[170.,155.] # +41% intraday but < +30% close on entry120
        g.loc[72,"close"]=111.
        g.loc[73,"open"]=105.
        r=r26.simulate(g,70,r26.ATR2_TARGET30)
        self.assertEqual(r["exit_reason"],"D0_FLOOR_OR_2ATR_CLOSE_TRAIL")
        self.assertEqual(r["exit_day"],3)
        self.assertEqual(r["net_pct"],round(100*(105/120-1)-.2,6))

    def test_d0low_only_floor_not_same_day_intraday_low(self):
        g=frame()
        g.loc[71,"low"]=1.
        self.assertEqual(r26.simulate(g,70,r26.ATR2_TARGET30)["exit_day"],60)
        g.loc[71,"close"]=98.
        g.loc[72,"open"]=95.
        r=r26.simulate(g,70,r26.ATR2_TARGET30)
        self.assertEqual(r["exit_day"],2)

    def test_high_after_exit_cannot_change_realized_exit(self):
        g=frame()
        g.loc[71,"close"]=90.
        g.loc[72,"open"]=85.
        first=r26.simulate(g,70,r26.ATR2_TARGET30)
        g.loc[75:130,["open","high","low","close"]]=[500.,800.,400.,700.]
        self.assertEqual(first,r26.simulate(g,70,r26.ATR2_TARGET30))

    def test_incomplete_horizon_fails_closed(self):
        with self.assertRaisesRegex(ValueError,"HORIZON"):
            r26.simulate(frame(n=100),70,r26.MA10_TARGET30)

    def test_unknown_policy_fails_closed(self):
        with self.assertRaisesRegex(ValueError,"POLICY"):
            r26.simulate(frame(),70,"FUTURE_HIGH_SELL")

    def test_wrong_event_count_fails_source_identity(self):
        with self.assertRaisesRegex(ValueError,"149"):
            r26.build(frame(),[],[])

    def test_first_high_day_exit_at_open_counts_as_missed(self):
        r=row(first=4,exitday=4)
        self.assertTrue(r26.before_first30(r,r26.BASE))
        r["outcomes"][r26.BASE]["exit_day"]=5
        self.assertFalse(r26.before_first30(r,r26.BASE))

    def test_window_end_close_still_in_position_through_day60_high(self):
        r=row(first=60,exitday=60)
        r["outcomes"][r26.BASE]["exit_reason"]="WINDOW_END_CLOSE"
        self.assertFalse(r26.before_first30(r,r26.BASE))

    def test_describe_counts_tail_loss_and_realized_30_separately(self):
        a=row("A",big=True,baseline=-22.,new=-12.)
        b=row("B",big=True,baseline=36.,new=31.)
        c=row("C",big=False,baseline=-13.,new=-16.)
        x=r26.describe([a,b,c],r26.BASE)
        self.assertEqual(x["loss_le_20_n"],1)
        self.assertEqual(x["loss_le_15_n"],1)
        self.assertEqual(x["loss_le_10_n"],2)
        self.assertEqual(x["realized_net30_n"],1)
        self.assertEqual(x["big30_opportunities_n_EXPOST"],2)

    def test_paired_prevented_loss_vs_newly_caused(self):
        a=row("A",big=False,baseline=-22.,new=-12.)
        b=row("B",big=True,baseline=-13.,new=-16.)
        c=row("C",big=True,baseline=35.,new=29.)
        p=r26.paired([a,b,c],r26.MA10_TARGET30)
        self.assertEqual(p["baseline_loss_le15_but_new_above_minus15_n"],1)
        self.assertEqual(p["baseline_above_minus15_but_new_loss_le15_n"],1)
        self.assertEqual(p["big30_realized_30_lost_n"],1)

    def test_empty_group_returns_nones_not_misleading_zeros(self):
        self.assertEqual(r26.describe([],r26.MA5_TARGET30)["n"],0)
        self.assertIsNone(r26.describe([],r26.MA5_TARGET30)["mean_net_pct"])

    def test_survivor_landmark_excludes_imminent_exit(self):
        a=row("A",first=8,exitday=2)
        b=row("B",first=9,exitday=3)
        a["outcomes"][r26.BASE]["exit_day"]=2
        b["outcomes"][r26.BASE]["exit_day"]=5
        old=[]
        for r in (a,b):
            old.append({"code":r["code"],"date":r["date"],
              "landmarks_DIAGNOSTIC_ONLY":{
                "D1":{"asof_close_above_entry":True,
                      "asof_close_above_frozen55":True,
                      "asof_close_above_ma5":True},
                "D3":{"asof_close_above_entry":True,
                      "asof_close_above_frozen55":True,
                      "asof_close_above_ma5":True},
                "D5":{"asof_close_above_entry":True,
                      "asof_close_above_frozen55":True,
                      "asof_close_above_ma5":True}}})
        d=r26.survivor_landmark([a,b],old,1)
        self.assertEqual(d["still_in_position_and_no_pending_next_open_exit_n"],1)
        self.assertTrue(d["conditional_survivorship_bias_warning"])

    def test_reference_preserves_original_net_and_day(self):
        g=frame()
        x=r26.r25.simulate(g,70,r26.MA5)
        y=r26.normalize_reference(x)
        self.assertEqual(y["net_pct"],x["net_pct"])
        self.assertEqual(y["exit_day"],x["exit_day"])


if __name__=="__main__":
    unittest.main()
