"""Synthetic R11 attribution, future isolation and loss-tail audit tests."""
import sys
from pathlib import Path
import unittest

import pandas as pd

sys.path.insert(0,str(Path(__file__).resolve().parent))
import r11_exit_diagnostics as r11


def prices():
    n=27
    return pd.DataFrame({"date":pd.bdate_range("2024-01-01",periods=n),
        "open":[100.]*n,"high":[101.]*n,"low":[99.]*n,
        "close":[100.]*n,"volume":[1000.]*n,"amount":[10000.]*n})


def event(*,reason="FROZEN_TARGET_LIMIT_TOUCH", h=2, delta=5.0):
    method=r11.METHODS[0]
    return {"code":"006400","date":"2024-01-03","idx":2,"line_id":"frozen",
        "nonoverlap_20d":True,"entry_dplus1_open":100.,
        "resistance":{method:{"status":"KNOWN_AFTER_D0_CLOSE","target":110.}},
        "models":{
            method+"/"+r11.TARGET_RULE:{"status":"PROVISIONAL_TEST","reason":reason,
                "net_total20bps_pct":delta,"exit_day":h},
            r11.ATR_ONLY:{"status":"PROVISIONAL_TEST","reason":"TIME_D20_FALLBACK",
                "net_total20bps_pct":0.,"exit_day":20},
        }}


class R11Tests(unittest.TestCase):
    def test_frozen_source_hash_contract(self):
        self.assertEqual(len(r11.R10_SHA256),64)

    def test_future_high_does_not_include_exit_day(self):
        df=prices()
        # Exit on D+2 at index 4; the large index-4 high is intraday ambiguous.
        df.loc[4,"high"]=500.
        df.loc[5,"high"]=121.
        v=r11.diagnose_event(event(),r11.METHODS[0],df)
        self.assertEqual(v["pair_status"],"PROVISIONAL_PAIRED_3STOCK_NOT_OOS")
        self.assertEqual(v["outcome_bucket"],"FROZEN_TARGET_EXIT")
        self.assertEqual(v["post_exit_expost"]["later_high"],121.)
        self.assertEqual(v["post_exit_expost"]["remaining_days_until_D20"],18)
        self.assertTrue(v["post_exit_expost"]["later_high_gain_at_least_10pct"])
        self.assertTrue(v["post_exit_expost"]["excluded_exit_day_high_due_to_unknown_intraday_order"])

    def test_day20_exit_has_no_future_high(self):
        v=r11.diagnose_event(event(h=20),r11.METHODS[0],prices())
        self.assertEqual(v["post_exit_expost"]["remaining_days_until_D20"],0)
        self.assertIsNone(v["post_exit_expost"]["later_high"])

    def test_unknown_kept_in_denominator(self):
        x=event()
        x["models"][r11.METHODS[0]+"/"+r11.TARGET_RULE]={"status":"UNKNOWN_MISSING_PATH"}
        v=r11.diagnose_event(x,r11.METHODS[0],prices())
        result=r11._group([v])
        self.assertEqual(result["input_n"],1)
        self.assertEqual(result["paired_n"],0)
        self.assertEqual(result["unknown_n"],1)

    def test_non_target_exit_must_match_atr_only(self):
        x=event(reason="INITIAL_ATR_STOP",h=2,delta=0)
        v=r11.diagnose_event(x,r11.METHODS[0],prices())
        self.assertEqual(v["outcome_bucket"],"ATR_STOP_BEFORE_TARGET")
        self.assertIsNone(v["post_exit_expost"])
        x["models"][r11.METHODS[0]+"/"+r11.TARGET_RULE]["net_total20bps_pct"]=-1.
        with self.assertRaisesRegex(AssertionError,"NON_TARGET_EXIT"):
            r11.diagnose_event(x,r11.METHODS[0],prices())

    def test_group_paired_attribution_sums_and_tails(self):
        x1=r11.diagnose_event(event(delta=5.),r11.METHODS[0],prices())
        x2=r11.diagnose_event(event(delta=-6.),r11.METHODS[0],prices())
        x3=r11.diagnose_event(event(reason="TIME_D20_FALLBACK",delta=0),r11.METHODS[0],prices())
        g=r11._group([x1,x2,x3])
        self.assertEqual(g["paired_n"],3)
        self.assertEqual(g["paired_positive_n"],1)
        self.assertEqual(g["paired_negative_n"],1)
        self.assertEqual(g["paired_equal_n"],1)
        self.assertEqual(g["paired_difference_pp"]["mean"],round(-1/3,6))
        self.assertEqual(g["R10_loss_tail"]["-5.0"]["r10_n"],1)

    def test_methods_cannot_be_mixed_in_one_comparison(self):
        a=r11.diagnose_event(event(),r11.METHODS[0],prices())
        b=dict(a,method=r11.METHODS[1])
        with self.assertRaisesRegex(ValueError,"MIXED_METHODS"):
            r11._group([a,b])

    def test_incomplete_path_beyond_d20_cannot_be_described(self):
        with self.assertRaisesRegex(AssertionError,"PATH_MISSING"):
            r11.diagnose_event(event(),r11.METHODS[0],prices().iloc[:20])


if __name__=="__main__":
    unittest.main()
