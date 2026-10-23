"""Research-only frozen R1-D6 correctness and anti-lookahead regression.
Run with: python -m unittest discover -s tests -p test_formula66_r1d6_oos_holdout.py -v
"""
import datetime,inspect,tempfile,unittest
import numpy as np,pandas as pd
from pathlib import Path
from research_formulas import formula66_r1d6_oos_holdout as d6
from research_formulas import formula66_r1d1_native_context as d1
from research_formulas import formula66_r1d5_support_reaction as d5

class R1D6FrozenOosTests(unittest.TestCase):
 def setUp(self):
  self.dates=pd.bdate_range('2024-01-01',periods=350).strftime('%Y-%m-%d').tolist()
  x=np.full(350,100.)
  x[329]=95;x[335]=134;x[337]=120;x[338]=114;x[339]=110
  self.df=pd.DataFrame({'open':x.copy(),'high':x+2,'low':x-2,'close':x,
                      'volume':np.full(350,1000.),'amount':np.full(350,100000.)},index=self.dates)
  self.df.iloc[329,self.df.columns.get_loc('low')]=94
  self.ep={'code':'123456','date':self.dates[339],
           'wave_trough_date':self.dates[329],'wave_peak_date':self.dates[335]}

 def test_01_original_modules_are_checked_by_frozen_git_blob(self):
  # Fails on changed original feature logic: important even if API shape looks unchanged.
  d1sha=d6.git_blob('research_formulas/formula66_r1d1_native_context.py')
  d5sha=d6.git_blob('research_formulas/formula66_r1d5_support_reaction.py')
  self.assertEqual(d1sha,d6.D1_SOURCE_BLOB)
  self.assertEqual(d5sha,d6.D5_SOURCE_BLOB)

 def test_02_holdout_has_no_signal_overlap_with_development(self):
  self.assertLess(d6.SIGNAL_END,'2025-10-01')
  self.assertLess(d6.SIGNAL_END,'2026-06-15')
  self.assertLess(d6.WARMUP_START,d6.SIGNAL_START)

 def test_03_exact_support_features_stable_after_future_append(self):
  before=d5.one_episode(d5._roll_indicators(self.df.copy()),self.ep)
  f=self.df.copy()
  f.loc['2029-01-02']=[999999,1000000,1,5,900,1]
  after=d5.one_episode(d5._roll_indicators(f),self.ep)
  for k in d5.BANDS:
   self.assertEqual(before[k+'_touch_type'],after[k+'_touch_type'])
   self.assertEqual(before[k+'_signal_above'],after[k+'_signal_above'])
  for k in ['signal_close_above_prev_high','signal_amount_vs_prior5median',
            'amount_3d_vs_prior10d','pullback_intraday_max_retrace_pct']:
   self.assertEqual(before[k],after[k])

 def test_04_FIB_reclaim_is_not_any_touch(self):
  k=d5.one_episode(d5._roll_indicators(self.df.copy()),self.ep)
  self.assertIn(k['FIB50_reclaim'],[0,1])
  self.assertNotEqual(int(k['FIB50_reclaim']),int(k['FIB50_fail']) if k['FIB50_reclaim'] else 1)

 def test_05_d5_pre_signal_amount_gate_is_unchanged(self):
  f={'signal_amount_vs_prior5median':1.0,'amount_3d_vs_prior10d':0.99999,
     'signal_lower_wick_fraction':0.3,'bounce_from_postpeak_min_to_signal_close_pct':2.0}
  g=d6.asof_flags(f)
  self.assertEqual((g['signal_amount_above_prev5'],g['amount3d_above_prior10'],
                    g['signal_low_wick_gt30'],g['postpeak_min_rebound_gt2']), (1,0,1,1))

 def test_06_forward20_entry_next_open_unchanged_and_net20bp(self):
  dates=pd.bdate_range('2024-01-02',periods=30).strftime('%Y-%m-%d').tolist()
  ix=1
  ep={'date':dates[ix],'code':'123456','signal_ix':ix,'signal_close':100.,'forward_bars':[]}
  for i in range(ix+1,ix+21):ep['forward_bars'].append((dates[i],100,104,94,110,1000,1e6))
  a=d6.forward_outcome(ep,dates,{})
  self.assertEqual(a['status_out'],'PRICE_PATH_COMPLETE_NOT_EXECUTION_CERTIFIED')
  self.assertAlmostEqual(a['net_20bps_pct'],9.8)
  self.assertAlmostEqual(a['max_intraday_adverse_pct'],-6.0)

 def test_07_missing_intermediate_price_must_block(self):
  dates=pd.bdate_range('2024-01-02',periods=30).strftime('%Y-%m-%d').tolist()
  ep={'date':dates[0],'code':'123456','signal_ix':0,'signal_close':100.,'forward_bars':[]}
  for i in list(range(1,8))+list(range(9,22)):
   ep['forward_bars'].append((dates[i],100,101,99,100,1,1000))
  b=d6.forward_outcome(ep,dates,{})
  self.assertEqual(b['status_out'],'INTERMEDIATE_SUSPENSION_OR_MISSING_PATH')

 def test_08_price_jump_over_40_blocks_raw_actions(self):
  dates=pd.bdate_range('2024-01-02',periods=30).strftime('%Y-%m-%d').tolist()
  ep={'date':dates[0],'code':'123456','signal_ix':0,'signal_close':100.,'forward_bars':[]}
  for i in range(1,21):
   v=200 if i==7 else 100
   ep['forward_bars'].append((dates[i],v,v+1,v-1,v,1000,100000))
  b=d6.forward_outcome(ep,dates,{})
  self.assertEqual(b['status_out'],'UNADJUSTED_FUTURE_CORPORATE_ACTION_OR_EXTREME_GAP_BLOCKED')

 def test_09_hypotheses_frozen_and_disjoint(self):
  self.assertEqual(len(d6.HYPOTHESES),16)
  self.assertEqual(d6.HYPOTHESES['H01_MA20_HOLD'],'MA20_hold')
  self.assertEqual(d6.HYPOTHESES['H02_FIB50_RECLAIM'],'FIB50_reclaim')
  self.assertEqual(d6.HYPOTHESES['H03_PREV_HIGH_BREAK'],'signal_close_above_prev_high')
  self.assertIn('WAVE_LOW_fail',d6.HYPOTHESES.values())

 def test_10_trade_authority_forbidden(self):
  src=inspect.getsource(d6)
  self.assertNotIn('send_order(',src)
  self.assertNotIn('LIVE_ORDER',src)
  self.assertIn('RESEARCH_ONLY',src)

 def test_11_wilson_empty_group_and_bounds(self):
  self.assertTrue(np.isnan(d6.wilson(0,0)[0]))
  lo,hi=d6.wilson(5,10)
  self.assertLess(lo,50);self.assertGreater(hi,50)

 def test_13_all_source_digest_fail_closed(self):
  import hashlib
  with tempfile.TemporaryDirectory() as temp:
   p=Path(temp)/'source.gz'
   p.write_bytes(b'authentic')
   src={'2024-01-02':(p,hashlib.sha256(b'authentic').hexdigest())}
   # Small fixture cannot pass the complete 1,907-row audit.
   with self.assertRaises(RuntimeError):d6.verify_all_daily_raw_sha(src)
   src={'2024-01-02':(p,hashlib.sha256(b'tampered').hexdigest())}
   with self.assertRaisesRegex(RuntimeError,'SHA_DRIFT'):d6.verify_all_daily_raw_sha(src)

 def test_12_dates_and_outcome_not_in_asof_feature_schema(self):
  f=d5.one_episode(d5._roll_indicators(self.df.copy()),self.ep)
  for k in ('net_20bps_pct','gross_pct','entry_open','exit_close'):
   self.assertNotIn(k,f)

if __name__=='__main__':unittest.main()
