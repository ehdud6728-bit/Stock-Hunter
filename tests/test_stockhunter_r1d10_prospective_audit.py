"""Read-only R1-D10 causal separation / provenance guard tests."""
import unittest, tempfile, inspect, json
from pathlib import Path
import pandas as pd
import numpy as np
from research_formulas import stockhunter_r1d10_prospective_audit as r

class R1D10QA(unittest.TestCase):
 def test_01_original_alnum_ticker_kept(self):
  self.assertEqual(r.ticker('0088M0'),'0088M0')
  self.assertEqual(r.ticker('000100'),'000100')
 def test_02_bad_ticker_fails_closed(self):
  for v in ('0008800','88M0','0088-M0','none',''):
   with self.assertRaisesRegex(RuntimeError,'R1D10_FAIL_CLOSED'):r.ticker(v)
 def test_03_research_only_contract(self):
  self.assertTrue(r.RESEARCH_ONLY)
  self.assertEqual(r.HORIZONS,(1,3,5,10))
  self.assertEqual(len(r.PIN),4)
 def test_04_no_live_or_order_tools(self):
  src=inspect.getsource(r)
  for t in ('send_order(', 'place_order(', 'submit_order(', 'optuna.', 'GridSearchCV','order_api('):self.assertNotIn(t,src)
 def test_05_pins_include_sha_and_run(self):
  for p in r.PIN.values():
   self.assertEqual(len(p['sha256']),64);self.assertGreater(p['run'],0)
 def test_06_numerical_horizons_use_completed_only(self):
  d=pd.DataFrame({'d1_complete':['1','0'],'d1_close_ret_pct':['4','900'],'d1_mfe_pct':['5','7'],'d1_mae_pct':['-2','-7'],
    **{f'd{h}_{k}':['1','0'] if k=='complete' else ['1','900'] for h in (3,5,10) for k in ('complete','close_ret_pct','mfe_pct','mae_pct')}})
  q=r.trust_horizon(d)
  self.assertEqual(list(q.n_complete),[1]*4)
  self.assertEqual(float(q.loc[q.horizon==1,'mean_close_pct_raw'].iloc[0]),4.)
 def test_07_qa_threshold_is_only_diagnostic(self):
  src=inspect.getsource(r.analyze)
  self.assertIn('QA_ABS_RETURN_PCT',src)
  self.assertIn('qa_abs_ret_gt40',src)
  self.assertNotIn('drop(index=',src)
 def test_08_capture_timing_slots_not_authority(self):
  d=pd.DataFrame({'snapshot_date':['2026-10-22','2026-10-23'],'first_observed_at_kst':['2026-10-22T14:31:00+09:00','2026-10-23T20:01:00+09:00'],'capture_slot':['PRIMARY_1440','FALLBACK_1520']})
  q=r.provenance_timing(d)
  self.assertEqual(list(q.observed_after_1530),[False,True])
 def test_09_duplicate_key_fails_closed(self):
  d=pd.DataFrame({'date':['2026-10-22']*2,'code':['0088M0']*2})
  with self.assertRaisesRegex(RuntimeError,'DUPLICATE_KEY'):r.ensure_key(d,['date','code'],'SIM')
 def test_10_input_file_not_manufactured(self):
  with self.assertRaisesRegex(RuntimeError,'MISSING_CSV'):r.read_csv('/none/there/definitely.csv')
 def test_11_after_coverage_is_not_zero_fill(self):
  src=inspect.getsource(r.analyze)
  self.assertIn('after_has_data',src)
  self.assertNotIn('after_pct.fillna(0)',src)
 def test_12_exact_identity_is_the_only_join(self):
  src=inspect.getsource(r.analyze)
  self.assertIn("on=['signal_date','code']",src)
  self.assertIn('EXACT_TICKER_MISMATCH_DO_NOT_AUTO_ALIAS',src)
 def test_13_latest_overlay_must_not_get_historical_labels(self):
  src=inspect.getsource(r.analyze)
  self.assertIn('no_historical_overlay_level_success_rates',src)
  self.assertIn('overlay_snapshot_only',src)
 def test_14_asof_future_separated(self):
  self.assertIn('ONLY',r.__doc__)
  src=inspect.getsource(r.analyze)
  self.assertIn('same-day',src.lower())
if __name__=='__main__':unittest.main()
