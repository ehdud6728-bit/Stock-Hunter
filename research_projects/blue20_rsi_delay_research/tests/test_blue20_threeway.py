import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import unittest
import numpy as np
import pandas as pd
from blue20_threeway_audit import classify, calculate_bands, simulate

class TestClassify(unittest.TestCase):
    def test_clear(self): self.assertEqual(classify(95,110,105,100),'CLEAR_BODY')
    def test_micro_low_above(self): self.assertEqual(classify(92,105,101,100),'MICRO_BODY')
    def test_micro_small_candle(self): self.assertEqual(classify(99.8,100.4,100.2,100),'MICRO_BODY')
    def test_wick(self): self.assertEqual(classify(95,102,98,100),'WICK_ONLY')
    def test_gap(self): self.assertEqual(classify(101,104,103,100),'GAP_ABOVE')
    def test_no_bull(self): self.assertIsNone(classify(103,105,99,100))
    def test_no_touch(self): self.assertIsNone(classify(95,99,98,100))
    def test_shift(self):
        dates=pd.bdate_range('2023-01-02',periods=60)
        closes=[100.0 + n*.1 for n in range(60)]
        df=pd.DataFrame(dict(date=dates,code='000001',open=closes,high=np.array(closes)+1,
                             low=np.array(closes)-1,close=closes))
        a=calculate_bands(df)
        # The first usable band index = 19+26=45.
        self.assertTrue(pd.isna(a.loc[44,'UPPER']))
        self.assertAlmostEqual(a.loc[45,'UPPER'],np.mean(closes[:20])+2*np.std(closes[:20],ddof=0))
    def test_stop_first_when_both_in_same_bar(self):
        dates=pd.bdate_range('2023-01-02',periods=23)
        df=pd.DataFrame(dict(open=[100.0]*23,high=[100.0]*23,low=[100.0]*23,close=[100.0]*23),index=dates)
        df.loc[dates[1],['high','low']]=[112,94]
        r=simulate(df,0,dates,'D0_CLOSE')
        self.assertEqual(r['exit_reason'],'SL_AMBIGUOUS')
        self.assertAlmostEqual(r['net_pct'],-5.25)
    def test_d1_open_stop_first(self):
        dates=pd.bdate_range('2023-01-02',periods=23)
        df=pd.DataFrame(dict(open=[100.0]*23,high=[100.0]*23,low=[100.0]*23,close=[100.0]*23),index=dates)
        df.loc[dates[1],['open','high','low']]=[102,120,95]
        r=simulate(df,0,dates,'D1_OPEN')
        self.assertEqual(r['exit_reason'],'SL_AMBIGUOUS')
        self.assertAlmostEqual(r['net_pct'],-5.25)
if __name__=='__main__': unittest.main()
