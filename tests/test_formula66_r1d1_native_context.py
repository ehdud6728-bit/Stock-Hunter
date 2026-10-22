"""Offline as-of contract tests for independent KRX proxy; historical 66 parity explicitly blocked."""
import gzip
import importlib.util
import json
import pathlib
import tempfile
import unittest
from dataclasses import asdict
from unittest import mock

ROOT=pathlib.Path(__file__).resolve().parents[1]
SPEC=importlib.util.spec_from_file_location('formula66_r1d1_native_context',ROOT/'research_formulas/formula66_r1d1_native_context.py')
import sys
MOD=importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name]=MOD
SPEC.loader.exec_module(MOD)


def samples():
 bars=[]
 for i in range(350):
  if i<270:close=100.
  elif i<=333:close=92.
  elif i<=343:close=92.+(116.-92.)*(i-333)/10.
  else:close=114.-2.*(i-344)
  op=close
  hi=close+2.;lo=close-2.
  if i==333:lo=90.
  if i==343:hi=120.
  amount=100.
  if 328<=i<333:amount=130.
  if 333<=i<=343:amount=1000.
  if i>=344:amount=180.
  # Last days need no zero and no trading interruption.
  date=f'2025-TRADING-{i:03d}'
  bars.append((i,date,op,hi,lo,close,100.,amount))
 return bars


class R1DNativeTests(unittest.TestCase):
 def test_01_research_only(self):
  self.assertTrue(MOD.RESEARCH_ONLY)
  self.assertIn('NOT_MAIN7_FORMULA',MOD.Feature('UNKNOWN').model_kind)
 def test_02_exact_source_not_rewritten(self):
  self.assertNotIn('rank',MOD.VERSION.lower())
  self.assertEqual(MOD.PROXY['ma_period'],224)
 def test_03_224_enough_bars(self):
  x=samples()[:280];r=MOD.asof_features(x,279,x[-1][1]);self.assertIn('INSUFFICIENT',r.status)
 def test_04_continuous_session_required(self):
  x=samples();x[-180]=(171,)+x[-180][1:];r=MOD.asof_features(x,349,x[-1][1]);self.assertIn('NOT_CONTIGUOUS',r.status)
 def test_05_no_future_data_tolerated(self):
  x=samples();
  with self.assertRaisesRegex(RuntimeError,'LOOKAHEAD'):
   MOD.asof_features(x,348,x[-2][1])
 def test_06_nontradeable_today(self):
  x=samples();y=list(x[-1]);y[6]=0.;x[-1]=tuple(y)
  r=MOD.asof_features(x,349,x[-1][1]);self.assertIn('NOT_TRADEABLE',r.status)
 def test_07_corporate_action_gap_failclosed(self):
  x=samples();y=list(x[-2]);y[2]=y[2]*2.;x[-2]=tuple(y)
  r=MOD.asof_features(x,349,x[-1][1]);self.assertIn('GAP_GT_40',r.status)
 def test_08_independent_asof_shape(self):
  x=samples();r=MOD.asof_features(x,349,x[-1][1])
  self.assertIn('KRX_PRICE_CONTEXT_READY',r.status)
  self.assertEqual(r.wave_found,1)
  self.assertTrue(r.prewave_base)
  self.assertEqual(r.strict_proxy,1)
  self.assertEqual(r.candidate_stage,'BASE_ACCUM_WAVE1_PULLBACK_DRY')
  self.assertGreater(r.wave_gain_pct,12)
  self.assertTrue(15<r.wave_retrace_pct<61.8)
  self.assertGreater(r.prewave_accum_amount_ratio,1.03)
  self.assertLess(r.pullback_dry_amount_ratio,.85)
 def test_09_strict_proxy_missing_dry_reject(self):
  x=samples()
  for i in range(347,350):
   y=list(x[i]);y[7]=1400.;x[i]=tuple(y)
  r=MOD.asof_features(x,349,x[-1][1]);self.assertEqual(r.strict_proxy,0)
 def test_10_strict_proxy_not_same_as_66_formula(self):
  x=MOD.Feature('READY');d=MOD.feature_dict(x)
  self.assertIn('NOT_MAIN7_FORMULA',d['model_kind'])
 def test_11_bad_date_raises(self):
  x=samples();
  with self.assertRaisesRegex(RuntimeError,'ASOF'):
   MOD.asof_features(x,349,'2025-01-01')
 def test_12_negative_ratio_protected(self):
  self.assertIsNone(MOD.ratio(10,0))
  self.assertIsNone(MOD.ratio(10,None))
 def test_13_proxy_thresholds_frozen(self):
  self.assertEqual((MOD.PROXY['healthy_retrace_min'],MOD.PROXY['healthy_retrace_max']),(.15,.618))
  self.assertEqual(MOD.PROXY['accum_recent']+MOD.PROXY['accum_prior'],25)
 def test_14_static_main7_parity_not_claimed(self):
  import inspect
  t=inspect.getsource(MOD.run_census)
  self.assertIn('NOT_VALIDATED_52_DERIVED_INPUTS',t)
  self.assertIn('NOT_PERFORMED_NO_PRODUCER_PARITY',t)
  self.assertIn('NOT_EVALUATED_IN_LEGACY_CAPTURE',t)
 def test_15_no_fast_fallback_price(self):
  import inspect
  t=inspect.getsource(MOD)
  self.assertNotIn('FinanceDataReader',t)
  self.assertNotIn('Close*Volume',t)
 def test_16_no_cli_bypass_verification(self):
  import inspect
  src=inspect.getsource(MOD.main)
  self.assertNotIn('skip_full_verify',src)
 def test_17_hold_code_ticker_alphanumeric(self):
  x=MOD._number(None);self.assertEqual(x,'')
 def test_18_actual_amount_not_inferred_from_price(self):
  x=samples();r=MOD.asof_features(x,349,x[-1][1]);self.assertNotEqual(r.prewave_accum_amount_ratio,r.recent5_vs_prior20_actual_amount)
 def test_19_no_old_capture_as_authoritative_universe(self):
  import inspect
  t=inspect.getsource(MOD.run_census)
  self.assertIn('for rr in df.itertuples(index=False,name=None)',t)
  self.assertNotIn('if code not in original_true_keys:continue',t)
 def test_20_gzip_hidden_outcomes(self):
  import inspect
  t=inspect.getsource(MOD.run_census)
  self.assertIn("[-260:]",t)
  self.assertIn("'known_at_signal':1",t)
  self.assertIn('chart_future_hidden',t)

if __name__=='__main__':unittest.main()
