import unittest
import pandas as pd
import numpy as np
from research import bands,CONFIGS
class ConfigTests(unittest.TestCase):
 def test_eight_configs(self):self.assertEqual(len(CONFIGS),8);self.assertEqual(len(set(CONFIGS)),8)
 def test_all_boundaries(self):
  for n,m,s in CONFIGS:
   b=bands(pd.Series([100.]*160),n,m,s);self.assertEqual(b['UPPER'].first_valid_index(),n-1+s);self.assertTrue(b['UPPER'].dropna().eq(100).all())
 def test_no_future(self):
  x=pd.Series(np.arange(180,dtype=float)+50)
  for n,m,s in CONFIGS:pd.testing.assert_series_equal(bands(x,n,m,s)['UPPER'].iloc[:120],bands(x.iloc[:120],n,m,s)['UPPER'])
 def test_multiplier_and_shift(self):
  x=pd.Series(np.arange(160,dtype=float)+50)
  for n,m,s in CONFIGS:self.assertAlmostEqual(bands(x,n,m,s)['UPPER'].iloc[n-1+s],x.iloc[:n].mean()+m*x.iloc[:n].std(ddof=0))
if __name__=='__main__':unittest.main()
