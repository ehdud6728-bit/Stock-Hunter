"""Synthetic offline timing and no-lookahead tests for R22. No KIS credentials."""
import unittest

import pandas as pd

import r22_level_reclaim as r22


def frame(t=70, n=155):
    dates = pd.date_range("2022-01-03", periods=n, freq="B").strftime("%Y-%m-%d")
    g = pd.DataFrame({
        "code": ["TEST"] * n, "date": dates,
        "open": [12.0] * n, "high": [13.0] * n,
        "low": [10.0] * n, "close": [12.0] * n,
        "volume": [1000] * n,
    })
    g.loc[t, ["open", "high", "low", "close"]] = [12.0, 15.0, 11.0, 14.0]
    g.loc[t+1:, ["open", "high", "low", "close"]] = [14.5, 16.0, 11.0, 14.2]
    return g


class R22FrozenLevelTests(unittest.TestCase):

    def test_d0_excluded_from_frozen_55_day_high(self):
        g = frame()
        self.assertEqual(r22.frozen_prior55_level(g, 70), 13.0)
        g.loc[70, "high"] = 10000.0
        self.assertEqual(r22.frozen_prior55_level(g, 70), 13.0)

    def test_level_one_close_equals_level_triggers_next_open(self):
        g = frame()
        g.loc[71, "close"] = 13.0
        g.loc[72, "open"] = 12.5
        x = r22.policy(g, 70, r22.LEVEL_ONE)
        self.assertEqual(x["exit_day"], 2)
        self.assertEqual(x["net_pct"], round(100*(12.5/14.5-1)-.2,6))

    def test_level_two_can_reclaim_between_losses(self):
        g = frame()
        g.loc[71, "close"] = 12.8  # lost day 1
        g.loc[72, "close"] = 14.2  # regained day 2
        g.loc[73, "close"] = 12.7  # lost day 3
        g.loc[74, "close"] = 12.6  # lost again day 4
        g.loc[75, "open"] = 11.9
        one = r22.policy(g,70,r22.LEVEL_ONE)
        two = r22.policy(g,70,r22.LEVEL_TWO)
        self.assertEqual(one["exit_day"],2)
        self.assertEqual(two["exit_day"],5)
        self.assertEqual(two["net_pct"], round(100*(11.9/14.5-1)-.2,6))

    def test_level_one_exit_ignores_intraday_break_if_close_recovers(self):
        g = frame()
        g.loc[71, "low"] = 1.0
        self.assertEqual(r22.policy(g,70,r22.LEVEL_ONE)["exit_day"],60)

    def test_guard_exits_on_level_loss_even_without_two_ma5_closes(self):
        g = frame()
        g.loc[71, "close"] = 12.8
        self.assertEqual(r22.policy(g,70,r22.GUARD_TWO)["exit_day"],2)

    def test_guard_exits_after_two_ma5_closes_while_frozen_level_holds(self):
        g = frame()
        for day, c in enumerate([15.0,17.0,18.0,19.0,19.5,18.0,17.5], 71):
            g.loc[day,"close"] = c
            g.loc[day,"high"] = max(c, 19.5)
        g.loc[78,"open"] = 17.0
        x = r22.policy(g,70,r22.GUARD_TWO)
        self.assertEqual(x["exit_day"],8)
        self.assertEqual(x["exit_reason"],"TWO_CONSECUTIVE_MA5_BREAKS")

    def test_control_is_exact_original_r20_not_approximation(self):
        g = frame()
        g.loc[72, "close"] = 12.0
        self.assertEqual(r22.policy(g,70,r22.MA5),
                         r22.r20.ma_close_break_next_open(g,70,5))

    def test_control_is_exact_original_r21_two_ma5(self):
        g = frame()
        g.loc[72:73,"close"] = 12.1
        self.assertEqual(r22.policy(g,70,r22.MA5_TWO),
                         r22.r21.simulate(g,70,r22.MA5_TWO))

    def test_future_mutation_cannot_change_earliest_exit(self):
        g = frame()
        g.loc[71,"close"] = 12.8
        g.loc[72,"open"] = 11.8
        first = r22.policy(g,70,r22.LEVEL_ONE)
        g.loc[74:130,"high"] = 10000.0
        g.loc[74:130,"close"] = 9000.0
        self.assertEqual(r22.policy(g,70,r22.LEVEL_ONE),first)

    def test_day5_state_distinguishes_never_lost_reclaim_and_still_below(self):
        a=frame()
        self.assertEqual(r22.day5_landmark(a,70)["asof_d5_state"],"NEVER_LOST_BY_D5")
        b=frame()
        b.loc[72,"close"]=12.8
        self.assertEqual(r22.day5_landmark(b,70)["asof_d5_state"],
                         "LOST_THEN_RECLAIMED_BY_D5")
        c=frame()
        c.loc[75,"close"]=12.5
        self.assertEqual(r22.day5_landmark(c,70)["asof_d5_state"],
                         "AT_OR_BELOW_LEVEL_ON_D5")

    def test_future_changes_only_expost_not_day5_state(self):
        g = frame()
        first = r22.day5_landmark(g,70)
        g.loc[78:120,"high"] = 1000.0
        g.loc[78:120,"close"] = 999.0
        changed=r22.day5_landmark(g,70)
        self.assertEqual(first["asof_d5_state"],changed["asof_d5_state"])
        self.assertNotEqual(
            first["d6_to_d60_best_intraday_mfe_pct_EXPOST"],
            changed["d6_to_d60_best_intraday_mfe_pct_EXPOST"])

    def test_incomplete_history_bad_entry_and_unknown_policy_fail(self):
        with self.assertRaises(ValueError):
            r22.policy(frame(n=100),70,r22.LEVEL_ONE)
        g=frame()
        g.loc[71,"open"]=13.0
        with self.assertRaises(ValueError):
            r22.policy(g,70,r22.LEVEL_ONE)
        with self.assertRaises(ValueError):
            r22.policy(frame(),70,"FUTURE_PEAK_SELECTOR")

    def test_identity_mismatch_fails_closed(self):
        with self.assertRaises(ValueError):
            r22.audit(frame(),[],[],[])


if __name__ == "__main__":
    unittest.main()
