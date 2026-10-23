"""R1-D8 read-only, frozen upstream sources, cluster-matching integrity."""
import unittest, inspect, hashlib, tempfile
from pathlib import Path
import numpy as np
import pandas as pd
from research_formulas import formula66_r1d8_structure_confounds as m

class FrozenR1D8(unittest.TestCase):
 def sample(self):
  dates=['2024-01-03','2024-01-03','2024-01-04','2024-01-04','2024-01-04','2024-01-05']
  return pd.DataFrame({'date':dates,'code':['000001','000002','000001','000003','000004','000005'],
   'wave_gain_pct':[18.,18.,40.,18.,26.,33.],'wave_retrace_pct':[22.,22.,55.,40.,55.,58.],
   'joint_index_20d_regime':['MIXED_OR_NONEXTREME_20D']*6,'amount3d_above_prior10':[1,0,1,0,1,0],
   'MA20_hold':[1,0,1,0,1,0],'BB20_L_hold':[0,1,0,1,0,1],
   'BB40_L_hold':[0,1,0,1,0,1],'ENV20_L_10_hold':[0,0,0,0,0,0],
   'ENV40_L_10_hold':[1]*6,'FIB50_reclaim':[1,0,0,1,0,1],
   'FIB618_reclaim':[1,0,0,1,0,1],'signal_close_above_prev_high':[1,0,1,0,1,0]})
 def test_01_pinned_r1d6_r1d7_and_not_new_oos(self):
  self.assertEqual(m.D6_RUN,39941707036)
  self.assertEqual(m.D7_RUN,39948595864)
  self.assertEqual(m.GAIN_LABELS,('12_TO_LT20','20_TO_LT35','GE35'))
  self.assertEqual(m.RETRACE_LABELS,('15_TO_LT30','30_TO_LT50','50_TO_618'))
 def test_02_wave_bins_frozen_boundary(self):
  d=m.shape_bins(self.sample())
  self.assertEqual(d.wave_gain_bin.tolist(),['12_TO_LT20','12_TO_LT20','GE35','12_TO_LT20','20_TO_LT35','20_TO_LT35'])
  self.assertEqual(d.wave_retrace_bin.tolist(),['15_TO_LT30','15_TO_LT30','50_TO_618','30_TO_LT50','50_TO_618','50_TO_618'])
 def test_03_first_in_cohort_per_code_not_per_date(self):
  d=m.shape_bins(self.sample())
  self.assertEqual(d.code_first_in_cohort.tolist(),[True,True,False,True,True,True])
 def test_04_same_date_only_excludes_dates_single_flag(self):
  d=self.sample()
  d['status_out']=m.DONE;d['fail5']=[True,False,True,False,True,False];d['strong15']=[False]*6
  d['mae_intraday_pct']=[-2.,-9.,-3.,-8.,-6.,-7.]
  x=m.date_contrasts(d,flags=('MA20_hold',))
  a=x[x.outcome=='failure'].iloc[0]
  self.assertEqual(a.n_eligible_dates,2)
  self.assertAlmostEqual(a.mean_flag1_minus_flag0,100.)
 def test_05_wave_matched_subset_disjoint(self):
  d=m.shape_bins(self.sample())
  d['status_out']=m.DONE;d['fail5']=[1,0,1,0,1,0];d['strong15']=0;d['mae_intraday_pct']=-5.
  # Only 2024-01-03 has within-day, within-wave matching (same gain & retrace).
  x=m.date_contrasts(d,flags=('MA20_hold',),matched=True)
  self.assertEqual(int(x.loc[0,'n_eligible_dates']),1)
 def test_06_bootstrap_determinism_and_boundary(self):
  x=[1.,2.,3.,4.,5.];a=m._date_bootstrap_ci(x);b=m._date_bootstrap_ci(x)
  self.assertEqual(a,b);self.assertLess(a[0],np.mean(x));self.assertGreater(a[1],np.mean(x))
  self.assertTrue(np.isnan(m._date_bootstrap_ci([1.])[0]))
 def test_07_no_trade_no_optimizers(self):
  s=inspect.getsource(m)
  for b in ('send_order(', 'LIVE_ORDER', 'optuna.', 'GridSearchCV', 'backtrader'):
   self.assertNotIn(b,s)
  self.assertTrue(m.RESEARCH_ONLY)
 def test_08_asof_filename_fail_closed(self):
  with tempfile.TemporaryDirectory() as root:
   with self.assertRaisesRegex(RuntimeError,'MISSING_SOURCE'):
    m.read_raw(root,'NONEXISTENT.csv')
 def test_09_original_d7_git_blob_is_frozen(self):
  p=Path('research_formulas/formula66_r1d7_path_risk_audit.py')
  self.assertTrue(p.exists())
  self.assertEqual(m.git_blob(p.read_bytes()),m.D7_SOURCE_BLOB)
 def test_10_no_feature_used_from_future_results_before_freeze(self):
  s=inspect.getsource(m.analyze)
  self.assertLess(s.index('asof_freeze_hash='),s.index('p20=p20.merge'))
  self.assertLess(s.index('p20=p20.merge'),s.index('joined=x.merge'))
 def test_11_date_bootstrap_keeps_date_as_unit(self):
  s=inspect.getsource(m.date_contrasts)
  self.assertIn("groupby('date')",s)
  self.assertIn('_date_bootstrap_ci',s)
 def test_12_source_ids_are_pinned(self):
  self.assertEqual(m.D7_ARTIFACT_ID,12499352019)
  self.assertEqual(m.D6_ASOF_ID,12496827313)
  self.assertEqual(m.D6_SEALED_ID,12497006782)
  for d in [m.D7_ARTIFACT_DIGEST,m.D6_ASOF_DIGEST,m.D6_SEALED_DIGEST,m.D6_ASOF_BYTE_SHA]:
   self.assertEqual(len(d),64)

if __name__=='__main__':unittest.main()
