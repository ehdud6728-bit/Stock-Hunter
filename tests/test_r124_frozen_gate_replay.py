import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock
import pandas as pd
from closebet_research import r124_frozen_gate_replay as m

ROOT=Path(__file__).resolve().parents[1]
POLICY=ROOT/'closebet_research/r124_frozen_hypotheses.json'

class FrozenTests(unittest.TestCase):
 def frame(self):
  return pd.DataFrame([{
   'scope':'V4976_SELECTED','code':'000001','signal_date':'2026-10-17',
   'prev_low_relation':'HOLD_PREV_LOW','volume_context':'ACCUM20_RECENT_DRY5',
   'ma_band':'ABOVE_20_60','ma20_slope20_pct':2.5,'overhead_proxy_type':'ABOVE_HEAVY_PROXY',
   'future_return_should_not_be_used':-100.0},
   {'scope':'V4976_SELECTED','code':'000002','signal_date':'2026-10-17',
   'prev_low_relation':'CLOSE_BELOW_PREV_LOW','volume_context':'ACCUM20_RECENT_DRY5',
   'ma_band':'ABOVE_20_60','ma20_slope20_pct':2.5,'overhead_proxy_type':'BELOW_HEAVY_PROXY',
   'future_return_should_not_be_used':1000.0},
   {'scope':'V4976_SELECTED','code':'000003','signal_date':'2026-10-17',
   'prev_low_relation':'HOLD_PREV_LOW','volume_context':'BOTH_SUBDUED',
   'ma_band':'BELOW_20_60','ma20_slope20_pct':-5.,'overhead_proxy_type':'UNKNOWN_SPARSE_CLOSE_PROXY',
   'future_return_should_not_be_used':99.0}])
 def test_policy_is_predeclared(self):
  p=m.validate_policy_file(POLICY)
  self.assertEqual(p['controls'],['BASE_ALL','NEG_OVERHEAD_HEAVY'])
 def test_h1_requires_hold(self):
  p=m.gate_matrix(self.frame());self.assertEqual(p.H1_PREV_LOW_HELD.tolist(),[True,False,True])
 def test_h2_requires_rising_ma_above(self):
  p=m.gate_matrix(self.frame());self.assertEqual(p.H2_HELD_MA20_UP_ABOVE_20_60.tolist(),[True,False,False])
 def test_h3_requires_dryup(self):
  p=m.gate_matrix(self.frame());self.assertEqual(p.H3_HELD_ACCUM20_DRY5.tolist(),[True,False,False])
 def test_negative_control_is_not_unknown(self):
  p=m.gate_matrix(self.frame());self.assertEqual(p.NEG_OVERHEAD_HEAVY.tolist(),[True,False,False])
 def test_future_labels_never_used_in_gate_matrix(self):
  a=self.frame();b=a.copy();b.future_return_should_not_be_used=[222.,444.,666.]
  self.assertTrue(m.gate_matrix(a)[list(m.GATES)].equals(m.gate_matrix(b)[list(m.GATES)]))
 def test_missing_ma_is_fail_closed(self):
  a=self.frame();a.loc[0,'ma20_slope20_pct']=float('nan');p=m.gate_matrix(a)
  self.assertFalse(bool(p.loc[0,'H2_HELD_MA20_UP_ABOVE_20_60']))
 def test_labels_do_not_turn_stop_then_later_into_win(self):
  x=pd.DataFrame({'path_failure_class':['TP5_FIRST','STOP5_FIRST_THEN_LATER_TP5','STOP5_FIRST_NO_LATER_TP5','AMBIGUOUS_TP_STOP_SAME_DAY','NO_TOUCH_WEAK_LIFT']})
  self.assertEqual(m.failure_bucket(x).tolist(),['TP_FIRST','STOP_THEN_LATER_TP','STOP_NO_TP','AMBIGUOUS','NO_TOUCH'])
 def test_no_future_success_is_created(self):
  x=m.forecast_contract()
  self.assertFalse(x['forward_available_at_generation'])
  self.assertEqual(x['mode'],'PROSPECTIVE_EVIDENCE_INTAKE_SPECIFICATION_NOT_ENABLED')
 def test_policy_guard_rejects_drift(self):
  p=json.loads(POLICY.read_text());p['candidate_gates'].pop()
  with tempfile.TemporaryDirectory() as d:
   f=Path(d)/'rules.json';f.write_text(json.dumps(p))
   with self.assertRaisesRegex(ValueError,'GATE_SIGNATURE_DRIFT'):m.validate_policy_file(f)
 def test_source_contract_no_false_oos(self):
  self.assertFalse(m.forecast_contract()['applies_to_trade'])
  self.assertIn('snapshot_captured_utc',m.forecast_contract()['required_source_fields'])
 def test_workflow_research_only(self):
  f=ROOT/'.github/workflows/closing_bet_r124_frozen_support_volume_gates_research.yml'
  s=f.read_text()
  self.assertIn('workflow_dispatch:',s)
  self.assertNotIn('  schedule:',s)
  self.assertNotIn('workflow_run:',s)
  self.assertNotIn('TELEGRAM',s)
  self.assertNotIn('secrets.',s)
 def test_optional_visible_root_copy(self):
  actual=ROOT/'.github/workflows/closing_bet_r124_frozen_support_volume_gates_research.yml'
  visible=ROOT/'closing_bet_r124_frozen_support_volume_gates_research.yml'
  if visible.is_file():self.assertEqual(hashlib.sha256(actual.read_bytes()).digest(),hashlib.sha256(visible.read_bytes()).digest())
 def test_duplicate_source_strict(self):
  self.assertEqual(len(m.KEY),3)
 def test_manifest_hash_size(self):
  self.assertEqual(len(m.PINNED_MANIFEST_SHA),64)

if __name__=='__main__':unittest.main()
