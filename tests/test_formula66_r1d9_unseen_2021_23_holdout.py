"""Predeclared unobserved 2021-23 historical holdout unit/guard tests."""
import inspect,io,tempfile,unittest
from pathlib import Path
import numpy as np,pandas as pd
from research_formulas import formula66_r1d9_unseen_2021_23_holdout as d9

class LockedR1D9(unittest.TestCase):
 def test_01_window_is_new_and_all_outcomes_end_before_R1D6(self):
  self.assertEqual((d9.SIGNAL_START,d9.SIGNAL_END),('2021-01-04','2023-11-29'))
  self.assertEqual(d9.EXPECTED_D20_END,'2023-12-28')
  self.assertLess(d9.EXPECTED_D20_END,'2024-01-02')
 def test_02_predeclared_primary_is_BB20_not_result_selected(self):
  self.assertEqual(d9.PRIMARY_FLAG,'BB20_L_hold')
  self.assertIn(d9.PRIMARY_FLAG,d9.CHECK_FLAGS)
  self.assertEqual(len(d9.CHECK_FLAGS),6)
 def test_03_horizons_are_frozen_and_D20_correct(self):
  self.assertEqual(d9.HORIZONS,(5,10,15,20))
  days=pd.bdate_range('2023-01-02',periods=30).strftime('%Y-%m-%d').tolist()
  ep={'date':days[0],'code':'000001','signal_ix':0,'signal_close':100.,'forward_bars':[]}
  for i in range(1,21):ep['forward_bars'].append((days[i],100.,106.,95.,105.,1000.,1e7))
  rows=d9.forward_price_packs(ep,days)
  self.assertEqual(len(rows),4)
  self.assertAlmostEqual(rows[-1]['net_20bps_pct'],4.8)
  self.assertAlmostEqual(rows[-1]['mae_intraday_pct'],-5.)
 def test_04_D20_forward_price_must_be_parity_checked(self):
  self.assertIn('d6.forward_outcome',inspect.getsource(d9.forward_price_packs))
  self.assertIn('ORIGINAL_D20_NUMERIC_DRIFT',inspect.getsource(d9.forward_price_packs))
 def test_05_unseen_period_required_outcomes_block_in_2023(self):
  src=inspect.getsource(d9.verify_calendar)
  for token in ('EXPECTED_D20_END','EXPECTED_PREVIOUS','PERIOD_OVERLAP_WITH_R1D6'):
   self.assertIn(token,src)
 def test_06_20session_repeat_cooldown_frozen(self):
  self.assertIn('t-previous_signal[code]<20',inspect.getsource(d9.discover))
 def test_07_features_frozen_before_returns(self):
  src=inspect.getsource(d9.discover)
  self.assertLess(src.index('feature_path=out/'),src.index('results=[]'))
  self.assertIn('OUTCOME_LEAKED_IN_SIGNAL_TIME_FEATURES',src)
 def test_08_market_day_contrast_unmatched(self):
  a=pd.DataFrame([{'date':'2022-01-03','wave_gain_bin':'12_TO_LT20','wave_retrace_bin':'30_TO_LT50',
         'BB20_L_hold':1,'failure5':0,'strong15':1,'mae_intraday_pct':-4},
        {'date':'2022-01-03','wave_gain_bin':'12_TO_LT20','wave_retrace_bin':'30_TO_LT50',
         'BB20_L_hold':0,'failure5':1,'strong15':0,'mae_intraday_pct':-14},
        {'date':'2022-01-04','wave_gain_bin':'20_TO_LT35','wave_retrace_bin':'30_TO_LT50',
         'BB20_L_hold':1,'failure5':0,'strong15':0,'mae_intraday_pct':-8}])
  z=d9.date_matched_contrast(a,'BB20_L_hold',matched=False,bootstrap_n=25)
  self.assertEqual(z['n_eligible_dates'],1)
  self.assertAlmostEqual(z['delta_fail_pp'],-100)
  self.assertAlmostEqual(z['delta_MAE_pp'],10)
 def test_09_matched_wave_only_relevant_same_day(self):
  a=pd.DataFrame([{'date':'2022-01-03','wave_gain_bin':g,'wave_retrace_bin':'30_TO_LT50',
                    'BB20_L_hold':v,'failure5':v,'strong15':0,'mae_intraday_pct':-4}
                     for g,v in [('12_TO_LT20',1),('20_TO_LT35',0)]])
  z=d9.date_matched_contrast(a,'BB20_L_hold',matched=True)
  self.assertEqual(z['n_eligible_dates'],0)
  self.assertTrue(np.isnan(z['delta_fail_pp']))
 def test_10_stable_bootstrap(self):
  from research_formulas import formula66_r1d8_structure_confounds as d8
  self.assertEqual(d8._date_bootstrap_ci([1.,2.,3.],iterations=40,seed=25),
                   d8._date_bootstrap_ci([1.,2.,3.],iterations=40,seed=25))
 def test_11_no_trade_calls_or_threshold_search(self):
  src=inspect.getsource(d9)
  for forbidden in ('send_order(', 'LIVE_ORDER', 'GridSearchCV', 'optuna.', 'backtrader', 'place_order('):
   self.assertNotIn(forbidden,src)
  self.assertIn('RESEARCH_ONLY=True',src)
 def test_12_old_R1D8_reference_unavailable_must_block(self):
  with tempfile.TemporaryDirectory() as tmp:
   with self.assertRaisesRegex(RuntimeError,'MISSING_FROZEN_R1D8_COMPARISON'):
    d9.old_d8_reference(tmp)
 def test_13_duplicate_reference_tamper_must_block(self):
  with tempfile.TemporaryDirectory() as tmp:
   out=Path(tmp)/'result';out.mkdir()
   p=out/'R1D8_DATE_CLUSTERED_CONTRASTS_WITH_CI.csv'
   pd.DataFrame([{'condition':'BB20_L_hold','comparison':'DATE_WAVE_RETRACE_MATCHED',
                  'outcome':'failure','mean_flag1_minus_flag0':+7.06}]).to_csv(p,index=False)
   with self.assertRaisesRegex(RuntimeError,'FROZEN_R1D8_REPORTED_RESULT_CHANGED'):
    d9.old_d8_reference(tmp)
 def test_14_independent_period_must_not_assume_tradeable(self):
  src=inspect.getsource(d9.discover)
  self.assertIn("'tradeability_certified_PIT':False",src)
  self.assertIn("'native_original_66_formulas':False",src)
 def test_15_completed_D20_cohort_retains_first_code(self):
  from unittest.mock import patch
  with tempfile.TemporaryDirectory() as tmp:
   rows=[('2021-01-04','000001',0),('2021-02-01','000001',1),('2021-02-01','000002',0)]
   feat=[];result=[]
   for date,code,k in rows:
    f={'date':date,'code':code,'market_regime':'MIXED_NONEXTREME20',
       'wave_gain_bin':'12_TO_LT20','wave_retrace_bin':'15_TO_LT30'}
    f.update({flag:int((k+pos)%2==0) for pos,flag in enumerate(d9.CHECK_FLAGS)})
    feat.append(f)
    result.append({'date':date,'code':code,'horizon':20,
       'status_out':d9.DONE,'net_20bps_pct':(-6.0 if k else 16.0),
       'mae_intraday_pct':(-9.0 if k else -3.0),
       'mfe_intraday_pct':(3.0 if k else 18.0)})
   asof=pd.DataFrame(feat);paths=pd.DataFrame(result)
   with patch.object(d9,'old_d8_reference',return_value=dict(d9.REFERENCE_R1D8_MATCHED_FAIL_DELTA)):
    metrics=d9.analyze(asof,paths,tmp,tmp)
   self.assertEqual(metrics['n_D20_complete'],3)
   self.assertTrue((Path(tmp)/'R1D9_REPEATED_CODE_SENSITIVITY.csv').is_file())
   audit=pd.read_csv(Path(tmp)/'R1D9_SEALED_AUDIT_JOINED_NO_LIVE.csv.gz')
   self.assertEqual(audit['first_code'].astype(int).sum(),2)
   self.assertEqual(int(audit[(audit.code.astype(str)=='1')]['first_code'].sum()),1)
if __name__=='__main__':unittest.main()
