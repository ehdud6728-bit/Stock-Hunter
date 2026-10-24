"""R1-D11 only: frozen source integrity and descriptive QA (no trade impact)."""
import inspect, json, tempfile, unittest
from pathlib import Path
import numpy as np
import pandas as pd
from research_formulas import stockhunter_r1d11_integrity_and_giveback as r

class R1D11ReadOnly(unittest.TestCase):
 def test_01_pins_are_frozen_historical_runs(self):
  self.assertEqual(r.PIN['r1d10']['run'],40074113253)
  self.assertEqual(r.PIN['trust']['run'],39948786391)
 def test_02_legal_alphanumeric_ticker_retained(self):
  self.assertEqual(r.ticker('0088M0'),'0088M0')
  self.assertNotEqual(r.ticker('0088M0'),r.ticker('000880'))
 def test_03_malformed_code_must_fail(self):
  for x in ['880','0088m0','0088-M0','nan','00088.0']:
   with self.assertRaisesRegex(RuntimeError,'EXACT'):r.ticker(x)
 def test_04_missing_predeclared_protocol_fails(self):
  with self.assertRaisesRegex(RuntimeError,'MISSING_PREDECLARED_PROTOCOL'):r.protocol_guard('absent_R1D11_protocol.json')
 def test_05_predeclared_protocol_mismatch_fails(self):
  with tempfile.TemporaryDirectory() as d:
   p=Path(d)/'R1D11_PREDECLARED_PROTOCOL.json'
   p.write_text(json.dumps({'version':r.VERSION,'qa_abs_pct':35}))
   with self.assertRaisesRegex(RuntimeError,'PREDECLARED_PROTOCOL_MISMATCH'):r.protocol_guard(p)
 def test_06_duplicate_security_date_fails(self):
  a=pd.DataFrame([{'date':'2026-09-17','code':'000150'},{'date':'2026-09-17','code':'000150'}])
  with self.assertRaisesRegex(RuntimeError,'DUPLICATE'):r.unique(a,['date','code'],'FIXTURE')
 def test_07_complete_and_qa_flags_are_separated(self):
  src=inspect.getsource(r.compute)
  self.assertIn("('SOURCE_ALL_UNMODIFIED',p)",src)
  self.assertIn("('QA_FLAG_CLEAR_SENSITIVITY'",src)
 def test_08_qa_thresholds_are_frozen_and_diagnostic_only(self):
  self.assertEqual((r.CLEAN_QA_ABS40,r.CROSS_MISMATCH10),(40.,10.))
  self.assertIn('NOT automatically incorrect returns',inspect.getsource(r))
 def test_09_no_live_orders_or_optimizers(self):
  s=inspect.getsource(r)
  for bad in ('send_order(', 'place_order(', 'live_gate=', 'GridSearchCV', 'optuna.', 'backtrader'):
   self.assertNotIn(bad,s)
 def test_10_same_day_bootstrap_deterministic(self):
  a=pd.DataFrame({'snapshot_date':['d1','d1','d2','d3','d3'],'code':['000001','000002','000001','000003','000002'],'ret':[1.,2.,-4.,3.,0.], 'first_of_code':[True,False,True,True,False]})
  self.assertEqual(r.date_code_bootstrap(a,'ret',n=60,seed=43),r.date_code_bootstrap(a,'ret',n=60,seed=43))
 def test_11_code_cluster_bootstrap_deterministic(self):
  a=pd.DataFrame({'snapshot_date':['d1','d2','d3','d4'],'code':['000001','000001','000002','000002'],'ret':[1.,2.,3.,4.],'first_of_code':[True,False,True,False]})
  self.assertEqual(r.date_code_bootstrap(a,'ret',n=50,seed=9,cluster='code'),r.date_code_bootstrap(a,'ret',n=50,seed=9,cluster='code'))
 def test_12_insufficient_2_clusters_nan_guard(self):
  a=pd.DataFrame({'snapshot_date':['d1'],'code':['000001'],'ret':[1.]})
  self.assertEqual(r.date_code_bootstrap(a,'ret'),(None,None))
 def test_13_missing_after_does_not_become_zero_return(self):
  s=inspect.getsource(r.compute)
  self.assertNotIn('fillna(0)',s)
  self.assertIn("as_bool(after.covered)",s)
 def test_14_no_relabeling_snapshot_overlay_colors(self):
  s=inspect.getsource(r)
  self.assertNotIn('research_level',s)
 def test_15_no_preexisting_research_output_overwrite(self):
  s=inspect.getsource(r.compute)
  self.assertIn('R1D11_',s)
  self.assertNotIn("out/'R1D10_",s)
 def test_16_source_contents_must_exist(self):
  with tempfile.TemporaryDirectory() as tmp:
   with self.assertRaisesRegex(RuntimeError,'REQUIRED_SOURCE_FILE_MISSING'):
    r.read_source(tmp,'r1d10','REALFULL_PAIRED_D5_D10_GIVEBACK.csv')
 def test_17_run_script_only_returns_audit_manifest(self):
  self.assertIn("'orders_placed':0",inspect.getsource(r.compute))
  self.assertTrue(r.RESEARCH_ONLY)

if __name__=='__main__':unittest.main()
