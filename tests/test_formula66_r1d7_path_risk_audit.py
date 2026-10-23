"""Locked retrospective risk audit invariants; no LIVE calls or threshold search."""
import inspect,unittest
import numpy as np,pandas as pd
from research_formulas import formula66_r1d7_path_risk_audit as d7

class FrozenD7(unittest.TestCase):
 def setUp(self):
  self.days=pd.bdate_range('2024-01-02',periods=30).strftime('%Y-%m-%d').tolist()
  self.rows={self.days[i]:(100.,110.,95.,100.,1000.,1e7) for i in range(1,21)}
 def test_01_exact_locked_artifacts(self):
  self.assertEqual(d7.D6_RUN,39941707036)
  self.assertEqual(d7.HORIZONS,(5,10,15,20))
  self.assertEqual(len(d7.ORIGINAL_ASOF_SHA256),64)
 def test_02_no_price_data_when_entry_missing(self):
  a=self.rows.copy();a.pop(self.days[1]);x=d7.build_horizon(self.days[0],'000001',self.days,0,a,5,100.)
  self.assertEqual(x['status_out'],'INTERMEDIATE_SUSPENSION_OR_MISSING_PATH')
 def test_03_next_day_open_and_20bps(self):
  r=d7.build_horizon(self.days[0],'000001',self.days,0,self.rows,5,100.)
  self.assertEqual(r['entry_day'],self.days[1]);self.assertEqual(r['exit_day'],self.days[5]);self.assertAlmostEqual(r['net_20bps_pct'],-.2)
  self.assertAlmostEqual(r['mae_intraday_pct'],-5.);self.assertAlmostEqual(r['mfe_intraday_pct'],10.)
 def test_04_future_horizon_change_does_not_change_D5(self):
  a=d7.build_horizon(self.days[0],'000001',self.days,0,self.rows,5,100.)
  b=self.rows.copy();b[self.days[13]]=(100.,1000.,1.,900.,1000.,1e7)
  z=d7.build_horizon(self.days[0],'000001',self.days,0,b,5,100.)
  self.assertEqual(a['net_20bps_pct'],z['net_20bps_pct'])
 def test_05_gap_guard_40pct(self):
  a=self.rows.copy();a[self.days[3]]=(160.,161.,159.,160.,1000.,1e7)
  x=d7.build_horizon(self.days[0],'000001',self.days,0,a,5,100.)
  self.assertIn('GAP_BLOCKED',x['status_out'])
 def test_06_right_censor_not_allowed_to_be_success(self):
  x=d7.build_horizon(self.days[-3],'000001',self.days,len(self.days)-3,{},5,100.)
  self.assertIn('CENSORED',x['status_out'])
 def test_07_same_day_order_does_not_become_stop_prediction(self):
  a=self.rows.copy();a[self.days[1]]=(100.,120.,88.,100.,1000.,1e7)
  x=d7.build_horizon(self.days[0],'000001',self.days,0,a,5,100.)
  self.assertEqual(x['same_day_extreme_order_unknown'],1)
  self.assertEqual(x['both_mfe15_mae_minus5'],1)
 def test_08_duration_reuses_not_edits_events(self):
  src=inspect.getsource(d7)
  for forbidden in ('send_order(', 'LIVE_ORDER', 'optuna.', 'GridSearchCV'):
   self.assertNotIn(forbidden,src)
  self.assertIn('RESEARCH_ONLY',src)
 def test_09_compare_d20_must_fail_on_mismatch(self):
  a=pd.DataFrame([{'date':'2024-01-02','code':'000001','horizon':20,'status_out':d7.H20_OK,'net_20bps_pct':1.,'gross_pct':1.2,'entry_open':100.,'exit_close':101.2,'mae_intraday_pct':-6.,'mfe_intraday_pct':9.}])
  f=pd.DataFrame([{'date':'2024-01-02','code':'000001','status_out':d7.H20_OK,'net_20bps_pct':1.,'gross_pct':1.2,'entry_open':100.,'exit_close':101.2,'max_intraday_adverse_pct':-6.,'max_intraday_favorable_pct':9.}])
  self.assertTrue(d7.compare_d20(a,f)['passed'])
  f.loc[0,'net_20bps_pct']=2.
  with self.assertRaisesRegex(RuntimeError,'D20_PRICE_NUMBER_DRIFT'):d7.compare_d20(a,f)
 def test_10_nonfinite_bar_blocks(self):
  a=self.rows.copy();a[self.days[2]]=(np.nan,110.,95.,100.,1000.,1e7)
  x=d7.build_horizon(self.days[0],'000001',self.days,0,a,5,100.)
  self.assertEqual(x['status_out'],'MISSING_OR_SUSPENDED_ENTRY_OR_EXIT')
 def test_12_up_down_barrier_order_unknown_only_same_day(self):
  data=self.rows.copy()
  data[self.days[1]]=(100.,116.,100.,110.,1000.,1e7)
  data[self.days[2]]=(109.,110.,90.,95.,1000.,1e7)
  out=d7.build_horizon(self.days[0],'000001',self.days,0,data,5,100.)
  self.assertEqual(out['price_barrier_order'],'UP_FIRST')
  self.assertEqual(out['first_touch_up15_day_offset'],1)
  self.assertEqual(out['first_touch_down5_day_offset'],2)

 def test_11_source_bars_no_lookahead_labels(self):
  x=d7.build_horizon(self.days[0],'000001',self.days,0,self.rows,5,100.)
  self.assertNotIn('strategy_ready',x)
  self.assertNotIn('orders_placed',x)

if __name__=='__main__':unittest.main()
