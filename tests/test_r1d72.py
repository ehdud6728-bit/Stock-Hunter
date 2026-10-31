import unittest,sys,pathlib,math
import pandas as pd
import numpy as np
sys.path.insert(0,str(pathlib.Path(__file__).parent.parent))
import r1d72_source_price_replay as m

class R1D72Tests(unittest.TestCase):
 def test_band_near_at_level(self): self.assertTrue(m.is_near(98,100,.025))
 def test_band_near_not_ma(self): self.assertFalse(m.is_near(110,90,.02))
 def test_touch_requires_actual_range(self):
  self.assertTrue(m.touch(97,102,100)); self.assertFalse(m.touch(104,108,100))
 def test_touch_does_not_implicitly_reclaim(self):
  self.assertTrue(m.touch(97,102,100));self.assertFalse(m.reclaim(102,100,102,100))
 def test_true_reclaim_uses_prev_day_own_band(self):
  self.assertTrue(m.reclaim(99,100,101,100));self.assertFalse(m.reclaim(101,100,101,100))
 def test_reclaim_not_a_touch_only(self): self.assertFalse(m.reclaim(97,100,97,100))
 def test_invalid_bar_no_repair(self): self.assertFalse(m.check_bar(dict(open=100,high=99,low=95,close=100,volume=20,amount=1000)))
 def test_confirm_t1_only_after_t1_close(self):
  sig=pd.Series(dict(low=95,close=100)); d1=pd.Series(dict(low=96,close=102));self.assertTrue(m.confirm_next(sig,d1,99))
 def test_confirmation_rejects_new_lower_low(self):
  sig=pd.Series(dict(low=95,close=100));d1=pd.Series(dict(low=94,close=102));self.assertFalse(m.confirm_next(sig,d1,99))
 def test_confirm_rejects_falling_close(self):
  sig=pd.Series(dict(low=95,close=100));d1=pd.Series(dict(low=96,close=99));self.assertFalse(m.confirm_next(sig,d1,99))
 def test_invalid_future_quarantine(self):
  q=pd.DataFrame([dict(date=str(i),open=100,high=101,low=99,close=100,valid_bar=True) for i in range(35)])
  q.loc[6,'valid_bar']=False
  self.assertEqual(m.replay_exit(q,4,1)['reason'],'INVALID_FUTURE_BAR')
 def test_future_price_not_used_for_signal_and_t2_entrance(self):
  q=pd.DataFrame([dict(date=str(i),open=100,high=101,low=99,close=100,valid_bar=True) for i in range(40)])
  q.loc[5,'open']=95;q.loc[6,'open']=115
  z1=m.replay_exit(q,4,1);z2=m.replay_exit(q,4,2)
  self.assertEqual(z1['entry_open'],95);self.assertEqual(z2['entry_open'],115)
 def test_entry_path_20_bars_inclusive(self):
  q=pd.DataFrame([dict(date=str(i),open=100,high=101,low=99,close=100,valid_bar=True) for i in range(40)])
  z=m.replay_exit(q,4,1)
  self.assertEqual(z['entry_day'],'5');self.assertEqual(z['exit_day'],'24');self.assertAlmostEqual(z['ret_net_pct'],-.2)
 def test_price_gap_is_a_watch_not_auto_CA(self):
  q=pd.DataFrame([dict(date=str(i),open=100,high=101,low=99,close=100,valid_bar=True) for i in range(40)])
  q.loc[12,['open','high','low','close']]=[65,67,64,66]
  z=m.replay_exit(q,4,1)
  self.assertTrue(z['price_jump_watch']);self.assertTrue(z['valid'])
 def test_band_ddof0(self):
  p=pd.DataFrame({'close':list(range(1,42)),'volume':[1]*41})
  # need open etc not for indicators
  r=m.indicators(p)
  self.assertAlmostEqual(r.loc[19,'bb20'],10.5-2*np.std(range(1,21),ddof=0))

if __name__=='__main__':unittest.main()
