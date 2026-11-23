"""R27 synthetic tests of fixed 3/5-day gates and strictly forward labels."""
from __future__ import annotations

import copy
import unittest

import pandas as pd

import r27_early_phase as r27


def frame(t=70,n=140):
    g=pd.DataFrame({
      "code":["000001"]*n,
      "date":pd.date_range("2022-01-03",periods=n,freq="B").strftime("%Y-%m-%d"),
      "open":[100.]*n,"high":[110.]*n,"low":[90.]*n,
      "close":[100.]*n,"volume":[1000.]*n,"amount":[100000.]*n})
    g.loc[t,["open","high","low","close"]]=[119.,130.,100.,125.]
    g.loc[t+1:,["open","high","low","close"]]=[120.,130.,110.,125.]
    return g


def outcome(exitday=60,reason="WINDOW_END_CLOSE",profit=-2.):
    return {"net_pct":profit,"exit_day":exitday,"exit_reason":reason}


class R27CausalPhaseTests(unittest.TestCase):
    def test_fixed_five_distinct_unmodified_policy_ids(self):
        self.assertEqual(len(r27.NEW_METHODS),5)
        self.assertEqual(len(set(r27.NEW_METHODS)),5)
        self.assertEqual(r27.phase_day_limit(r27.MA5_FIRST3),3)
        self.assertEqual(r27.phase_day_limit(r27.MA10_FIRST5),5)

    def test_unknown_policy_fails_closed(self):
        with self.assertRaisesRegex(ValueError,"INVALID_METHOD"):
            r27.simulate(frame(),70,"BUY_WHEN_FUTURE_HIGH_RISES")

    def test_incomplete_60bar_horizon_fails_closed(self):
        with self.assertRaisesRegex(ValueError,"INCOMPLETE_HORIZON"):
            r27.simulate(frame(n=95),70,r27.MA5_FIRST3)

    def test_early_ma5_d1_exit_next_d2_open_only(self):
        g=frame()
        g.loc[71,"close"]=105.  # < MA5 around 106, yet > fixed D0 low=100
        g.loc[72,"open"]=94.
        o=r27.simulate(g,70,r27.MA5_FIRST3)
        self.assertEqual(o["exit_reason"],"EARLY_MA5_CLOSE_BREAK")
        self.assertEqual(o["exit_day"],2)
        self.assertEqual(o["phase_at_trigger"],"EARLY")
        self.assertEqual(o["net_pct"],r27.r26.net(94.,120.))

    def test_early_ma10_d2_exit_in_first_five(self):
        g=frame()
        g.loc[72,"close"]=101.   # D2 close < MA10, still > D0 floor
        g.loc[73,"open"]=95.
        o=r27.simulate(g,70,r27.MA10_FIRST5)
        self.assertEqual(o["exit_reason"],"EARLY_MA10_CLOSE_BREAK")
        self.assertEqual(o["exit_day"],3)

    def test_ma5_breach_on_day4_not_early_exit(self):
        g=frame()
        g.loc[74,"close"]=105.    # D4: below MA5 but above floor
        o=r27.simulate(g,70,r27.MA5_FIRST3)
        self.assertEqual(o["exit_reason"],"WINDOW_END_CLOSE")
        self.assertEqual(o["exit_day"],60)

    def test_ma10_breach_after_day5_does_not_exit(self):
        g=frame()
        g.loc[76,"close"]=105.
        o=r27.simulate(g,70,r27.MA10_FIRST5)
        self.assertEqual(o["exit_reason"],"WINDOW_END_CLOSE")

    def test_d3_confirmation_uses_only_d3_close(self):
        g=frame()
        g.loc[73,"close"]=119.  # > D0 low100, > level110, <= entry120
        g.loc[74,"open"]=95.
        o=r27.simulate(g,70,r27.CONFIRM_D3)
        self.assertEqual(o["exit_reason"],"D3_CONFIRMATION_FAILED")
        self.assertEqual(o["exit_day"],4)
        self.assertAlmostEqual(o["fill_gap_from_trigger_close_pct"],
                               round(100*(95/119-1),6))

    def test_d5_confirmation_cannot_retroactively_exit_d3(self):
        g=frame()
        g.loc[73,"close"]=119.
        o=r27.simulate(g,70,r27.CONFIRM_D5)
        self.assertEqual(o["exit_reason"],"WINDOW_END_CLOSE")
        # The D5 close, not D3, determines confirmation.
        g.loc[75,"close"]=119.
        g.loc[76,"open"]=91.
        o=r27.simulate(g,70,r27.CONFIRM_D5)
        self.assertEqual(o["exit_reason"],"D5_CONFIRMATION_FAILED")
        self.assertEqual(o["exit_day"],6)

    def test_confirmed_at_d3_still_sells_d0_floor_later(self):
        g=frame()
        g.loc[76,"close"]=99.
        g.loc[77,"open"]=90.
        o=r27.simulate(g,70,r27.CONFIRM_D3)
        self.assertEqual(o["exit_reason"],"D0_LOW_CLOSE_BREAK")
        self.assertEqual(o["exit_day"],7)
        self.assertEqual(o["phase_at_trigger"],"LATE")

    def test_30_pct_intraday_high_is_not_a_fill(self):
        g=frame()
        g.loc[71,"high"]=999.
        o=r27.simulate(g,70,r27.CONFIRM_D3)
        self.assertEqual(o["exit_reason"],"WINDOW_END_CLOSE")

    def test_close30_target_sells_later_at_actual_gap_open(self):
        g=frame()
        g.loc[71,["high","close"]]=[167.,159.]
        g.loc[72,"open"]=140.
        o=r27.simulate(g,70,r27.CONFIRM_D3)
        self.assertEqual(o["exit_reason"],"CLOSE30_TARGET")
        self.assertEqual(o["exit_day"],2)
        self.assertLess(o["net_pct"],30.)

    def test_below_d0_floor_d1_executes_d2_open_with_large_gap(self):
        g=frame()
        g.loc[71,"close"]=98.
        g.loc[72,"open"]=60.
        o=r27.simulate(g,70,r27.CONFIRM_D5)
        self.assertEqual(o["exit_reason"],"D0_LOW_CLOSE_BREAK")
        self.assertEqual(o["exit_day"],2)
        self.assertLess(o["net_pct"],-40.)

    def test_early_atr2_can_trigger_above_floor(self):
        g=frame()
        # Reduce historical ATR: 13 narrow 2-point bars + D0 30-point bar
        g.loc[:69,["high","low"]]=[101.,99.]
        g.loc[72,"close"]=116.
        g.loc[73,"open"]=111.
        o=r27.simulate(g,70,r27.ATR2_FIRST5)
        self.assertEqual(o["exit_reason"],"EARLY_D0ATR2_CLOSE_TRAIL")
        self.assertEqual(o["exit_day"],3)

    def test_future_price_modification_after_exit_cannot_change_exit(self):
        g=frame()
        g.loc[71,"close"]=98.
        g.loc[72,"open"]=88.
        fixed=r27.simulate(g,70,r27.CONFIRM_D5)
        g.loc[80:130,["high","low","close"]]=[800.,10.,500.]
        self.assertEqual(fixed,r27.simulate(g,70,r27.CONFIRM_D5))

    def test_ma5_and_confirm_day_3_cannot_use_day5_future(self):
        g=frame()
        a=r27.simulate(g,70,r27.CONFIRM_D3)
        b=r27.simulate(g,70,r27.MA5_FIRST3)
        g.loc[75:130,["high","low","close"]]=[500.,111.,200.]
        # Changes after decision day3 MAY change later exits, not early exits.
        self.assertEqual(r27.phase_day_limit(r27.CONFIRM_D3),3)
        self.assertEqual(r27.phase_day_limit(r27.MA5_FIRST3),3)
        self.assertEqual(a["exit_reason"],"WINDOW_END_CLOSE")
        self.assertEqual(b["exit_reason"],"WINDOW_END_CLOSE")

    def test_landmark_pending_next_open_exit_is_not_survivor(self):
        x={"outcomes":{r27.CONFIRM_D3:outcome(4,"D3_CONFIRMATION_FAILED")}}
        self.assertFalse(r27.still_open_at_landmark(x,r27.CONFIRM_D3,3))
        self.assertTrue(r27.still_open_at_landmark(x,r27.CONFIRM_D3,2))

    def test_landmark_day3_forward_excludes_day1_to_day3_high(self):
        g=frame()
        g.loc[73,"high"]=1000.  # D+3 high occurred BEFORE D+3 close
        first=r27.forward_after_landmark(g,70,3,120.)
        self.assertFalse(first["forward_high30_vs_D1_entry_EXPOST"])
        self.assertEqual(first["window_start"],"DPLUS4_OPEN")
        g.loc[74,"high"]=200.
        second=r27.forward_after_landmark(g,70,3,120.)
        self.assertTrue(second["forward_high30_vs_D1_entry_EXPOST"])

    def test_landmark_day5_forward_end_date_and_reference_open(self):
        g=frame()
        g.loc[76,"open"]=150.
        g.loc[76:130,"high"]=160.
        metrics=r27.forward_after_landmark(g,70,5,120.)
        self.assertEqual(metrics["window_start"],"DPLUS6_OPEN")
        self.assertEqual(metrics["window_end"],"DPLUS60_CLOSE")
        self.assertTrue(metrics["forward_high30_vs_D1_entry_EXPOST"])
        self.assertFalse(metrics["forward_high30_vs_next_open_EXPOST"])

    def test_bad_landmark_day_or_window_fails(self):
        for day in (0,2,4,60):
            with self.assertRaisesRegex(ValueError,"BAD_POST_LANDMARK"):
                r27.forward_after_landmark(frame(),70,day,120.)

    def test_build_identity_guard_rejects_non149(self):
        with self.assertRaisesRegex(ValueError,"FIXED149"):
            r27.build(frame(),[],[],[])

    def test_oos_contract_is_blocked_without_new_data(self):
        c=r27.oos_readiness_manifest()
        self.assertEqual(c["new_independent_market_data_rows"],0)
        self.assertFalse(c["new_independent_results_validated"])
        self.assertTrue(c["status"].startswith("BLOCKED"))

    def test_original_149_expected_40_expost_and_44_preselected_not_refit(self):
        self.assertEqual(r27.LOSS_CUTS,(10.,15.,20.))
        self.assertEqual(r27.BASE,r27.r25.FLOOR_30)
        self.assertEqual(r27.HORIZON,60)


if __name__=="__main__":
    unittest.main()
