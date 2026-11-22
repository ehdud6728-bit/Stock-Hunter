"""R18 synthetic causality and training-only cutoff tests."""
import importlib.util
import unittest
from pathlib import Path

import pandas as pd

SPEC=importlib.util.spec_from_file_location("r18_tested",Path(__file__).with_name("r18_big_loss_warning.py"))
r18=importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(r18)


class R18Tests(unittest.TestCase):
    def test_asof_open_cannot_read_next_day_future(self):
        n=240
        f=pd.DataFrame({"open":[100.]*n,"high":[102.]*n,
                        "low":[98.]*n,"close":[100.]*n})
        f.loc[225,"open"]=103.
        a=r18.gap_at_open(f,224)
        f.loc[225,["high","low","close"]]=[999.,1.,501.]
        self.assertEqual(a,r18.gap_at_open(f,224))

    def test_code_split_fixed(self):
        self.assertEqual(r18.fold_of_code("093320"),r18.fold_of_code("093320"))
        self.assertTrue(0<=r18.fold_of_code("093320")<5)

    def test_training_tail_cut_ignores_higher_year(self):
        def r(ret):return {"outcomes":{"R14_INTRADAY":{"net_pct":ret}}}
        self.assertEqual(r18.train_tail_threshold([r(-10.),r(0.),r(10.)]),-8.)

    def test_features_premeditated_not_future_paths(self):
        self.assertEqual(len(r18.FEATURES_D0),6)
        self.assertEqual(len(r18.FEATURES_DPLUS1_OPEN),2)
        self.assertFalse(any("profit" in k or "future" in k for k in r18.FEATURES_D0))


if __name__=="__main__":unittest.main()
