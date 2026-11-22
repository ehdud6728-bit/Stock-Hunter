"""Synthetic safety/causality checks; never contact KIS or place orders."""
import importlib.util
import unittest
from pathlib import Path

import pandas as pd

PATH = Path(__file__).with_name("r17_failure_exit.py")
SPEC = importlib.util.spec_from_file_location("r17_tested", PATH)
r17 = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(r17)


def prices(n=320):
    base = [100.0 + i*.1 for i in range(n)]
    return pd.DataFrame({"open":base,"high":[v+1 for v in base],
                         "low":[v-1 for v in base],"close":base,
                         "volume":[1000]*n,"amount":[100000]*n,
                         "date":pd.date_range("2023-01-01",periods=n).strftime("%Y-%m-%d")})


class R17Tests(unittest.TestCase):
    def test_asof_features_ignore_future_change(self):
        p=prices();a=r17.make_features(p,225)
        p.loc[226:,["open","high","low","close","volume","amount"]]*=100
        self.assertEqual(a,r17.make_features(p,225))
        self.assertEqual(set(a),set(r17.FEATURES))

    def test_break_is_executed_only_after_known_close(self):
        p=prices();t=225
        p.loc[t,"low"]=120
        p.loc[t+1,["open","high","low","close"]]=[122,123,118,119]
        p.loc[t+2,["open","high","low","close"]]=[115,117,112,113]
        out=r17.exit_close_next_open(p,t,60,"D0_LOW_NEXT_OPEN")
        self.assertEqual(out["exit_day"],2)
        self.assertAlmostEqual(out["net_pct"],round(100*(115/122-1)-.2,6))

    def test_trailing_low_uses_prior_days_not_current_low(self):
        p=prices();t=225
        p.loc[t+1,["open","high","low","close"]]=[130,140,126,128]
        p.loc[t+2,["open","high","low","close"]]=[128,132,127,127]
        p.loc[t+3,["open","high","low","close"]]=[125,130,120,121]
        p.loc[t+4,["open","high","low","close"]]=[119,121,117,118]
        out=r17.exit_close_next_open(p,t,60,"PREV5_LOW_NEXT_OPEN")
        self.assertEqual(out["exit_day"],4)
        self.assertAlmostEqual(out["net_pct"],round(100*(119/130-1)-.2,6))

    def test_ambiguous_intraday_atr_stays_unknown(self):
        p=prices();t=225
        entry=float(p.loc[t+1,"open"])
        p.loc[t+1,["open","high","low","close"]]=[entry,entry+9,entry-9,entry]
        self.assertEqual(r17.first_passage_atr(p,t),"AMBIGUOUS_SAME_DAY_UP_DOWN")

    def test_split_cutoffs_use_training_only(self):
        def row(date,feature):
            return {"date":date,"features":{k:feature for k in r17.FEATURES}}
        samples=[row("2022-01-01",1.),row("2023-01-01",3.),row("2025-01-01",999.)]
        self.assertTrue(all(v==2. for v in r17.median_training_cutoffs(samples).values()))


if __name__=="__main__":
    unittest.main()
