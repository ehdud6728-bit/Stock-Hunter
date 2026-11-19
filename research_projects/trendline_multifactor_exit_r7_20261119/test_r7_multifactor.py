"""R7 precommitted causality and inherited population tests."""
import math
import unittest

import pandas as pd

from r7_multifactor import (
    RULES, components_at_close, feature_at_d0, nonoverlap_by_stock,
    simulate, study, wilder_atr,
)


def bars(count=80):
    df=pd.DataFrame({
        "date":pd.date_range("2023-01-02",periods=count,freq="B"),
        "open":[100.] * count,
        "high":[102.] * count,
        "low":[98.] * count,
        "close":[100.] * count,
        "volume":[100.] * count,
        "amount":[10000.] * count,
    })
    return df


class R7CausalResearchTests(unittest.TestCase):
    def test_wilder_seed_first_valid_day_and_gap(self):
        d=bars(30)
        v=wilder_atr(d)
        self.assertTrue(math.isnan(float(v.iloc[12])))
        self.assertAlmostEqual(float(v.iloc[13]),4.)
        d.loc[14,["open","high","low","close"]]=[114.,116.,113.,115.]
        g=wilder_atr(d)
        self.assertAlmostEqual(float(g.iloc[14]),(13*4.+16.)/14.)

    def test_d0_inputs_do_not_peek_at_future(self):
        d=bars()
        t=30
        first=feature_at_d0(d,t,wilder_atr(d))
        d.loc[35,["high","volume","amount"]]=[500.,100000.,100000.]
        second=feature_at_d0(d,t,wilder_atr(d))
        self.assertEqual(first,second)
        self.assertEqual(first["actual_volume_at_price"],
                         "NOT_AVAILABLE_FROM_DAILY_OHLCV")

    def test_volume_weakness_is_close_known_only(self):
        d=bars()
        t=30
        d.loc[t+1,["open","high","low","close","volume"]]=[101.,103.,97.,99.,300.]
        k=components_at_close(d,t,1,wilder_atr(d))
        self.assertTrue(k["volume_down"])
        self.assertFalse(k["structure"])
        self.assertEqual(k["signal_count"],1)
        self.assertEqual(k["asof"],"AFTER_DPLUS_1_CLOSE")

    def test_composite_executes_at_next_open(self):
        d=bars()
        t=30
        d.loc[t+1,["open","high","low","close","volume"]]=[101.,102.,88.,90.,300.]
        d.loc[t+2,"open"]=94.
        a=wilder_atr(d)
        k=components_at_close(d,t,1,a)
        self.assertTrue(k["structure"])
        self.assertTrue(k["volume_down"])
        m=simulate(d,t,a,"CONFLUENCE_2OF3_CLOSE_NEXT_OPEN")
        self.assertEqual(m["exit_day"],2)
        self.assertEqual(m["exit_price"],94.)
        self.assertEqual(m["trigger_day"],1)
        self.assertEqual(m["reason"],"PRIOR_CLOSE_SIGNAL_NEXT_OPEN")

    def test_gap_through_preset_stop_uses_worse_open(self):
        d=bars()
        t=30
        d.loc[t+2,["open","high","low","close"]]=[80.,82.,79.,81.]
        m=simulate(d,t,wilder_atr(d),"ATR_INITIAL_3")
        self.assertEqual(m["exit_day"],2)
        self.assertEqual(m["exit_price"],80.)
        self.assertEqual(m["reason"],"GAP_BELOW_PRESET_STOP_OPEN")
        self.assertTrue(m["gap_stop"])

    def test_current_high_does_not_raise_current_day_stop(self):
        d=bars()
        t=30
        d.loc[t+1,["open","high","low","close"]]=[100.,160.,95.,120.]
        d.loc[t+2,["open","high","low","close"]]=[100.,104.,96.,98.]
        m=simulate(d,t,wilder_atr(d),"ATR_CHANDELIER_3")
        self.assertEqual(m["exit_day"],2)
        self.assertEqual(m["reason"],"GAP_BELOW_PRESET_STOP_OPEN")

    def test_no_fabricated_exit_after_incomplete_20days(self):
        d=bars(35)
        out=simulate(d,30,wilder_atr(d),"ATR_INITIAL_3")
        self.assertEqual(out["status"],"UNKNOWN_INCOMPLETE_DPLUS20")

    def test_20day_embargo_uses_earliest_event_only(self):
        events=[
            {"code":"006400","idx":10,"date":"2023-01-02","line_id":"a"},
            {"code":"006400","idx":30,"date":"2023-02-01","line_id":"b"},
            {"code":"006400","idx":31,"date":"2023-02-02","line_id":"c"},
            {"code":"039840","idx":12,"date":"2023-01-04","line_id":"d"},
        ]
        got=nonoverlap_by_stock(events)
        self.assertEqual([(x["code"],x["idx"]) for x in got],
                         [("006400",10),("006400",31),("039840",12)])

    def test_integrated_frozen_r6_parity_and_permissions(self):
        s=study()
        self.assertEqual(s["unique_stock_dates"],368)
        self.assertLess(s["nonoverlap_20d_count"],368)
        self.assertEqual(set(s["overall_primary"]["rules"]),set(RULES))
        self.assertEqual(s["volume_at_price_actual"],
                         "UNAVAILABLE_DAILY_OHLCV_NOT_VOLUME_PROFILE")
        self.assertIsNone(s["strategy_winner"])
        self.assertFalse(s["out_of_sample"])
        self.assertFalse(s["can_modify_live_search_watch"])
        self.assertFalse(s["can_publish_strategy_performance"])
        b=s["overall_primary"]["rules"]["R6_TIME_D20_BASELINE"]
        self.assertEqual(b["complete_count"],363)
        self.assertAlmostEqual(b["mean_pct"],-1.907511,places=6)
        paired=s["overall_primary"]["same_events_only"]
        self.assertEqual(paired["n"],359)
        self.assertEqual(paired["mean_difference_vs_d20_percentage_points"]
                         ["R6_TIME_D20_BASELINE"],0.)


if __name__=="__main__":
    unittest.main()
