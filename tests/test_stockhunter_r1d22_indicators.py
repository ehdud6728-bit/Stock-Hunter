import unittest
import numpy as np
from research_formulas.stockhunter_r1d22_indicators import indicators,wilder,rsi_at,FEATURES

def bars(close=None,volume=None):
 c=np.asarray(close if close is not None else np.linspace(100,115,244)+0.3*np.sin(np.arange(244)*.4))
 v=np.asarray(volume if volume is not None else np.full(244,1000))
 return [(i,cl,cl+2,cl-2,cl,v[i],cl*v[i]) for i,cl in enumerate(c)]

class R1D22ContractTests(unittest.TestCase):
 def test_wilder_seed_sma(self):self.assertAlmostEqual(wilder([1,2,3,4,5],3)[2],2.)
 def test_wilder_step(self):self.assertAlmostEqual(wilder([1,2,3,4,5],3)[3],2.66666667,places=6)
 def test_wilder_too_short(self):self.assertTrue(np.isnan(wilder([1,2],3)[-1]))
 def test_constant_price_rsi50(self):self.assertEqual(rsi_at(np.ones(60)*20),50.)
 def test_all_positive_rsi100(self):self.assertEqual(rsi_at(np.arange(100.)+20),100.)
 def test_indicator_asof_available(self):self.assertEqual(indicators(bars())['feature_status'],'ASOF_OK')
 def test_indicator_feature_keys(self):self.assertTrue({'rsi14','adx14','atr14_pct','cmf20','signed_vol20','mfi14','bb_width20_pct','volume5_over20'}.issubset(indicators(bars())))
 def test_constant_volume_ratio_one(self):self.assertAlmostEqual(indicators(bars())['volume5_over20'],1.)
 def test_no_past_data(self):self.assertEqual(indicators(bars()[:-1])['feature_status'],'INSUFFICIENT_244')
 def test_reject_noncontiguous(self):
  x=bars();x[-1]=(500,*x[-1][1:]);self.assertEqual(indicators(x)['feature_status'],'NONCONTIGUOUS')
 def test_reject_high_below_close(self):
  x=bars();q=list(x[-1]);q[2]=q[4]-1;x[-1]=tuple(q);self.assertEqual(indicators(x)['feature_status'],'INVALID_OHLCV')
 def test_reject_nonfinite(self):
  x=bars();q=list(x[-1]);q[4]=float('nan');x[-1]=tuple(q);self.assertEqual(indicators(x)['feature_status'],'INVALID_NONFINITE')
 def test_reject_big_unadjusted_gap(self):
  c=np.ones(244)*100;c[-1]=170
  self.assertEqual(indicators(bars(c))['feature_status'],'UNADJUSTED_PRICE_GAP_PROXY')
 def test_reject_missing_recent_volume(self):
  v=np.ones(244)*1000;v[-1]=0
  self.assertEqual(indicators(bars(volume=v))['feature_status'],'ZERO_RECENT_TURNOVER')
 def test_cmf_flat_wicks_zero(self):self.assertAlmostEqual(indicators(bars())['cmf20'],0.,places=7)
 def test_technical_feature_no_future_date(self):
  x=bars();a=indicators(x);x[-1]=(243,999,1000,995,999,1000,999000)
  b=indicators(x);self.assertNotEqual(a['rsi14'],b.get('rsi14'))
 def test_predeclared_symmetric_19(self):self.assertEqual(len(FEATURES),19)

if __name__=='__main__':unittest.main()
