import ast
import importlib
import pathlib
import tempfile
import unittest
from unittest.mock import patch
import numpy as np
import pandas as pd

mod=importlib.import_module('research_formulas.formula66_r1d3_success_failure_context')

class R1D3ReadOnlyTests(unittest.TestCase):
 def test_01_source_run_pinned(self):
  self.assertEqual(mod.FROZEN_SOURCE_RUN,39873801594)
  self.assertEqual(mod.FROZEN_R1D2_ARTIFACT_ID,12468095269)
  self.assertEqual(len(mod.FROZEN_R1D2_ARTIFACT_SHA256),64)
 def test_02_label_boundaries(self):
  self.assertEqual(mod.label_outcome(15),'STRONG_15_TO_30')
  self.assertEqual(mod.label_outcome(30),'EXPLOSIVE_GE_30')
  self.assertEqual(mod.label_outcome(5),'SUCCESS_5_TO_15')
  self.assertEqual(mod.label_outcome(-5),'FAILURE_LE_MINUS5')
  self.assertEqual(mod.label_outcome(-4.99),'MIDDLE_MINUS5_TO_PLUS5')
  self.assertEqual(mod.label_outcome(np.nan),'NOT_OBSERVED')
 def test_03_asof_calculation_cannot_use_future(self):
  dates=pd.bdate_range('2026-01-05',periods=220).strftime('%Y-%m-%d').tolist()
  d=pd.DataFrame([{'date':date,'code':'000001','close':100+i,'amount':1e8,'volume':10000,'open':100+i,'high':101+i,'low':99+i} for i,date in enumerate(dates)])
  s,_=mod.stock_and_market_asof(d)
  day=dates[-30];origin=s[s.date.eq(day)].iloc[0]
  d.loc[d.date>day,'close']=999999
  d.loc[d.date>day,'amount']=99999999e10
  s2,_=mod.stock_and_market_asof(d)
  rerun=s2[s2.date.eq(day)].iloc[0]
  for k in ['stock_ret20_pct','stock_ret60_pct','krx_avg20_actual_amount_100mKRW','krx_actual_amount_5vs20_ratio','krx_recent20_return_sd_pct']:
   self.assertAlmostEqual(float(origin[k]),float(rerun[k]))
 def test_04_no_same_date_duplicate_feature_winner(self):
  a=pd.DataFrame([{'date':'2026-07-01','wave_gain_pct':30,'net_20bps_pct':20}, {'date':'2026-07-01','wave_gain_pct':15,'net_20bps_pct':-15}, {'date':'2026-07-02','wave_gain_pct':22,'net_20bps_pct':21},{'date':'2026-07-02','wave_gain_pct':17,'net_20bps_pct':-7}])
  z=mod.match_dates(a,['wave_gain_pct'])
  self.assertEqual(z.iloc[0].paired_signal_dates,2)
  self.assertEqual(z.iloc[0].pct_dates_strong_higher,100)
 def test_05_group_counts_no_alteration(self):
  a=pd.DataFrame({'date':['2026-07-01']*4,'code':['000001','000002','000003','000004'], 'status_out':[mod.OUTCOME_STATUS]*3+['RIGHT_CENSORED_SOURCE_END_NO_FUTURE_PRICE'],'net_20bps_pct':[31,17,-8,np.nan]})
  counts=mod.summary_counts(a)
  self.assertEqual(counts['explosive_n'],1);self.assertEqual(counts['strong_n'],2)
  self.assertEqual(counts['failure_n'],1);self.assertEqual(counts['n_not_complete'],1)
 def test_06_script_has_no_trading_order_api(self):
  text=pathlib.Path(mod.__file__).read_text()
  for needle in ['place_order(', 'submit_order(', 'telegram.send', 'send_message(', 'STOCKHUNTER_LIVE_ENABLE=1']:
   self.assertNotIn(needle,text)
 def test_07_external_proxies_explicitly_not_joined(self):
  self.assertIn('foreign_institutional_net_buy_by_stock',mod.EXTERNAL_UNKNOWN)
  self.assertIn('PIT_sector_industry_theme',mod.EXTERNAL_UNKNOWN)
 def test_08_20session_episodes_not_pretend_independent(self):
  self.assertEqual(mod.OUTCOME_STATUS,'PRICE_PATH_COMPLETE_NOT_EXECUTION_CERTIFIED')
 def test_09_does_not_change_66_formulas(self):
  text=pathlib.Path(mod.__file__).read_text()
  self.assertNotIn('main7_bugfix_2.py',text)
  self.assertTrue('NOT_66_FORMULAS' in text)
 def test_10_missing_source_closed(self):
  with tempfile.TemporaryDirectory() as t:
   with self.assertRaises(RuntimeError):mod.read_inputs(pathlib.Path(t))

if __name__=='__main__':unittest.main()
