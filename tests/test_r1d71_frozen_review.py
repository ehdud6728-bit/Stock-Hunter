import sys,unittest,pathlib,os,pandas as pd,numpy as np
output=pathlib.Path(os.environ["R1D71_OUTPUT_DIR"]) if os.environ.get("R1D71_OUTPUT_DIR") else None
root=pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0,str(root/'research_formulas'))
from r1d71_prior_5247_legacy_review import near_fraction

class TestR1D71(unittest.TestCase):
 def test_near_fraction_is_relative_to_lower_not_ma(self):
  close=pd.Series([90.,100.]);band=pd.Series([90.,90.]);
  self.assertEqual(near_fraction(close,band,.02).tolist(),[True,False])
 def test_near_fraction_is_symmetric(self):
  close=pd.Series([98.1,101.9,97.9,102.1]);band=pd.Series([100.]*4)
  self.assertEqual(near_fraction(close,band,.02).tolist(),[True,True,False,False])
 def test_zero_lower_cannot_signal(self):
  self.assertEqual(near_fraction(pd.Series([10.]),pd.Series([0.]),.1).tolist(),[False])
 def test_price_adjustment_not_imputed(self):
  m=100.;self.assertAlmostEqual(m*.9,90.);self.assertAlmostEqual(m*.8,80.);self.assertAlmostEqual(m*.98,98.)
 def test_ddof_difference(self):
  close=np.asarray([10.,11.,13.,14.,19.]);std0=close.std(ddof=0)
  self.assertAlmostEqual(close.std(ddof=1),std0*np.sqrt(len(close)/(len(close)-1)))
 def test_result_contract_parity(self):
  p=(output or root/'results')/'01_FROZEN_SAME_DATE_BB_SOURCE_PARITY.csv'
  if not p.exists():self.skipTest('Run full audit first')
  x=pd.read_csv(p);self.assertEqual(len(x),2)
  self.assertEqual(x.n_equal_tolerance_0p01_krw.astype(int).tolist(),[5247,5246])
 def test_source_envelope_nonoverlap(self):
  p=(output or root/'results')/'03_DEFINITION_SAME_DATE_CROSS_TAB.csv'
  if not p.exists():self.skipTest('Run full audit first')
  x=pd.read_csv(p).set_index('mode_a');self.assertEqual(int(x.loc['ENV20_PCT10_near','both']),0)
 def test_seven_historical_case_parity(self):
  p=(output or root/'results')/'07_SEVEN_CASE_OHLC_SOURCE_PARITY.csv'
  if not p.exists():self.skipTest('Chart reconstruction first')
  x=pd.read_csv(p);self.assertEqual(len(x),7);self.assertTrue((x[['close_t_diff','entry_d1_diff','exit_d20_diff']].abs()<1.e-6).all().all())

if __name__=='__main__':unittest.main()
