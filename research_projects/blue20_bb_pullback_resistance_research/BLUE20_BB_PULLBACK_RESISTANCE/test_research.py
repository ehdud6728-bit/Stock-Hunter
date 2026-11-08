import importlib.util,unittest
from pathlib import Path
from unittest.mock import patch
import pandas as pd
p=Path(__file__).with_name('study.py');s=importlib.util.spec_from_file_location('bb_research_snapshot',p);m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
class SnapshotTests(unittest.TestCase):
 def data(self):
  return pd.DataFrame(dict(date=pd.date_range('2020-01-01',periods=140).strftime('%Y-%m-%d'),open=100.,high=110.,low=90.,close=105.,volume=100.,prior_atr20=10.))
 def zone(self,lo,hi):return dict(low=lo,high=hi,count=3,median_drop_atr=1.2,rising_troughs=False,median_reapproach=10,tests=[dict(date='2020-01-03')])
 def test_below_close_not_active(self):
  with patch.object(m,'zones',return_value=([self.zone(90,100)],None)):
   c,z=m.snapshot(self.data(),130,120);self.assertFalse(c['strong_active']);self.assertTrue(c['strong_below']);self.assertEqual(z[0]['position'],'BELOW')
 def test_inside_is_active(self):
  with patch.object(m,'zones',return_value=([self.zone(100,110)],None)):
   c,z=m.snapshot(self.data(),130,120);self.assertTrue(c['strong_active']);self.assertEqual(c['resistance_bucket'],'0-3')
 def test_future_prefix_invariance(self):
  d=self.data();a=m.snapshot(d,125,120);d.loc[126:,'close']=9999;b=m.snapshot(d,125,120);self.assertEqual(a,b)
 def test_unknown_not_no_resistance(self):
  c,z=m.snapshot(self.data(),80,120);self.assertEqual(c['status'],'UNKNOWN_HISTORY');self.assertNotIn('strong_active',c)
if __name__=='__main__':unittest.main()
