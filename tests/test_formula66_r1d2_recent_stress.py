"""Fully offline unit regressions for 2026 recent downside stress, no KRX network."""
from __future__ import annotations
import json
import unittest
from datetime import date,timedelta
from pathlib import Path
import sys
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from research_formulas import formula66_r1d2_recent_stress as m

class RecentStressTests(unittest.TestCase):
 def test_01_exact_frozen_2026_four_month_window(self):
  self.assertEqual(m.WINDOWS['recent_4m'],('2026-06-15','2026-10-12'))
 def test_02_exact_frozen_2026_three_month_window(self):
  self.assertEqual(m.WINDOWS['recent_3m'],('2026-07-01','2026-10-12'))
 def test_03_latest_krx_not_fabricated(self):
  self.assertEqual(m.SOURCE_END,'2026-10-12')
 def test_04_main_66_formula_not_rerun(self):
  self.assertIn('NOT original 66 formulas',m.__doc__)
 def test_05_same_old_r1d1_frozen_source_sha(self):
  self.assertRegex(m.R1D1_SOURCE_GIT_BLOB,r'^[a-f0-9]{40}$')
 def test_06_immutable_r1d1_sha_actual_repo(self):
  root=Path(__file__).resolve().parents[1]
  if (root/'research_formulas/formula66_r1d1_native_context.py').exists():
   self.assertEqual(m.blob_sha(root/'research_formulas/formula66_r1d1_native_context.py'),m.R1D1_SOURCE_GIT_BLOB)
 def test_07_4m_daily_calendar_scope(self):
  cal=['2026-06-14','2026-06-15','2026-07-01','2026-10-12','2026-10-13']
  self.assertEqual(m.choose_dates(cal,'recent_4m','daily'),cal[1:-1])
 def test_08_missing_KRX_end_fails_closed(self):
  with self.assertRaisesRegex(RuntimeError,'SCOPE_OR_CALENDAR_DRIFT'):
   m.choose_dates(['2026-06-15','2026-10-10'],'recent_4m','daily')
 def test_09_weekly_keeps_last_KRX_session(self):
  cal=['2026-06-15','2026-06-16','2026-06-19','2026-06-22','2026-06-26','2026-10-12']
  self.assertEqual(m.choose_dates(cal,'recent_4m','weekly'),['2026-06-19','2026-06-26','2026-10-12'])
 def test_10_horizons_and_cost_stress(self):
  self.assertEqual(m.HORIZONS,(5,10,15,20));self.assertEqual(m.COST_BPS,(20,50,100))
 def test_11_risk_halt_zero_volume_not_tradeable(self):
  self.assertFalse(m._valid_bar((100,110,95,100,0,0)))
  self.assertFalse(m._valid_bar(None))
 def test_12_positive_KRX_OHLCA_is_price_proxy_only(self):
  self.assertTrue(m._valid_bar((100,110,95,101,10000,1000000)))
 def _data(self):
  cal=['2026-09-%02d'%i for i in range(1,23)]
  market={x:{'KOSPI_close':500,'KOSDAQ_close':700} for x in cal}
  price={x:{'000001':(100,110,90,100,10000,1000000)} for x in cal}
  signal={'date':cal[0],'code':'000001','joint_index_20d_regime':'BOTH_INDEX_BEAR20_LE_MINUS5','below_ma224':1}
  return cal,market,price,signal
 def test_13_forward_gross_next_open_D5_and_20bp(self):
  cal,market,price,signal=self._data()
  price[cal[5]]['000001']=(110,115,95,110,10000,1000000)
  out=m.outcome_rows(signal,cal,price,market)
  self.assertEqual(out[0]['horizon'],5)
  self.assertEqual(out[0]['status'],'PRICE_PATH_COMPLETE_NOT_EXECUTION_CERTIFIED')
  self.assertAlmostEqual(out[0]['net_20bps_pct'],9.8)
  self.assertEqual(out[0]['entry_day'],cal[1]);self.assertEqual(out[0]['exit_day'],cal[5])
 def test_14_future_censored_not_false_negative(self):
  cal,market,price,signal=self._data();signal['date']=cal[-2]
  got=m.outcome_rows(signal,cal,price,market)
  self.assertTrue(all(r['status']=='RIGHT_CENSORED_SOURCE_END_NO_FUTURE_PRICE' for r in got))
 def test_15_future_suspension_blocks_profit(self):
  cal,market,price,signal=self._data();price[cal[2]]['000001']=(100,100,100,100,0,0)
  out=m.outcome_rows(signal,cal,price,market)
  self.assertEqual(out[0]['status'],'INTERMEDIATE_SUSPENSION_OR_MISSING_PATH')
 def test_16_40pct_future_split_like_gap_blocks_profit(self):
  cal,market,price,signal=self._data();price[cal[3]]['000001']=(150,150,150,150,1000,100000)
  out=m.outcome_rows(signal,cal,price,market)
  self.assertEqual(out[0]['status'],'UNADJUSTED_FUTURE_CORPORATE_ACTION_OR_EXTREME_GAP_BLOCKED')
 def test_17_no_actual_buy_sell_order_changes(self):
  text=(Path(__file__).resolve().parents[1]/'research_formulas/formula66_r1d2_recent_stress.py').read_text()
  for term in ('place_order(', 'submit_order(', 'send_telegram(', 'LIVE_ENABLE = 1'):
   self.assertNotIn(term,text)
 def test_18_study_must_respect_daily_full_universe(self):
  text=(Path(__file__).resolve().parents[1]/'research_formulas/formula66_r1d2_recent_stress.py').read_text()
  self.assertIn('for row in d.itertuples(index=False)',text)
  self.assertIn("if day>=selected[0]:recent_prices[day][code]=vals",text)
 def test_19_research_contract_false_authority(self):
  text=(Path(__file__).resolve().parents[1]/'research_formulas/formula66_r1d2_recent_stress.py').read_text()
  self.assertIn("'strategy_ready':False",text)
  self.assertIn("'ORDERS_PLACED':0",text)
  self.assertIn("'not_independent_OOS':True",text)
 def test_20_repeated_signal_episode_gap_is_20(self):
  self.assertEqual(m.EPISODE_COOLDOWN,20)

if __name__=='__main__':unittest.main()
