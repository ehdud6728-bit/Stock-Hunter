import hashlib
import tempfile
import unittest
from pathlib import Path
import numpy as np
import pandas as pd
from research_formulas import formula66_r1d4_matched_blind as m

class R1D4Tests(unittest.TestCase):
 def test_01_research_only_and_no_orders(self):
  self.assertTrue(m.RESEARCH_ONLY)
  self.assertNotIn('place_order',Path(m.__file__).read_text())
 def test_02_frozen_counts_and_groups(self):
  self.assertEqual(m.EXPECTED_R1D3['n_complete'],939)
  self.assertEqual(m.EXPECTED_R1D3['strong_n'],91)
  self.assertEqual(m.EXPECTED_R1D3['failure_n'],286)
 def test_03_blind_id_deterministic(self):
  self.assertEqual(m.code_key('PAIR_001','005930','2026-07-01'),m.code_key('PAIR_001','005930','2026-07-01'))
  self.assertNotEqual(m.code_key('PAIR_001','005930','2026-07-01'),m.code_key('PAIR_001','005930','2026-07-02'))
 def test_04_blind_id_no_plain_stock_code(self):
  self.assertNotIn('005930',m.code_key('PAIR_001','005930','2026-07-01'))
 def test_05_match_does_not_use_outcome_return(self):
  self.assertEqual(len(m.MATCH_BASELINE),4)
  self.assertNotIn('net_20bps_pct',m.MATCH_BASELINE)
  self.assertNotIn('wave_gain_pct',m.MATCH_BASELINE)
  self.assertNotIn('pullback_dry_amount_ratio',m.MATCH_BASELINE)
 def test_06_asof_check_refuses_future_returns(self):
  with tempfile.TemporaryDirectory() as tmp:
   p=Path(tmp)/'src.csv'
   pd.DataFrame([{'date':'2026-07-03','code':'005930','stock_ret20_pct':1,
      'stock_ret60_pct':1,'krx_avg20_actual_amount_100mKRW':1,
      'krx_recent20_return_sd_pct':1,'net_20bps_pct':99}]).to_csv(p,index=False)
   with self.assertRaisesRegex(RuntimeError,'ASOF_LEAK'):m.asof_check(p)
 def test_07_pre_signal_window_never_reads_future_rows(self):
  dates=pd.bdate_range('2024-01-01',periods=510).strftime('%Y-%m-%d').tolist()
  sign=dates[500]
  hist={'005930':[{'date':d,'open':100+i,'high':110+i,'low':90+i,
                  'close':100+i,'volume':100,'amount':100000000} for i,d in enumerate(dates)]}
  w=m.extract_asof_window(hist,dates,'005930',sign)
  self.assertEqual(len(w),260)
  self.assertEqual(w.date.iloc[-1],sign)
  self.assertTrue(w.MA224.notna().iloc[-1])
  self.assertNotIn(dates[-1],w.date.tolist())
 def test_08_ma224_matches_actual_224_valid_closes(self):
  dates=pd.bdate_range('2024-01-01',periods=510).strftime('%Y-%m-%d').tolist()
  h={'005930':[{'date':d,'open':100+i,'high':110+i,'low':90+i,
                  'close':100+i,'volume':100,'amount':100000000} for i,d in enumerate(dates)]}
  w=m.extract_asof_window(h,dates,'005930',dates[480])
  self.assertAlmostEqual(w.MA224.iloc[-1],np.mean(np.arange(100+480-223,100+481)))
 def test_09_original_krx_amount_not_volume(self):
  self.assertIn('float(row[\'amount\'])',Path(m.__file__).read_text())
 def test_10_public_fields_do_not_contain_return_labels(self):
  # render output file names and public review card list must omit outcome/class
  code=Path(m.__file__).read_text()
  self.assertIn("'chart_name':f'{pairid}_{side}_{token}.png'",code)
  self.assertIn("'review_status':'NOT_REVIEWED'",code)
 def test_11_fails_if_krx_sessions_missing(self):
  with tempfile.TemporaryDirectory() as d:
   with self.assertRaisesRegex(RuntimeError,'SESSION_SET_NOT_1907'):
    m.day_path_map(d)
 def test_12_no_future_outcome_loaded_during_renderer(self):
  import inspect
  src=inspect.getsource(m.render_blind)+inspect.getsource(m.historical_bars)+inspect.getsource(m.extract_asof_window)+inspect.getsource(m.render_one)
  self.assertNotIn('net_20bps',src)
  self.assertNotIn('FORWARD_PRICE',src)
  self.assertNotIn('outcome_label',src)
 def test_13_workflow_scope_backwards_compatible(self):
  p=Path(__file__).resolve().parents[1]/'.github/workflows/stockhunter_r1d3_strong_winners_external_context.yml'
  if not p.exists():self.skipTest('workflow only required in GitHub repo root')
  s=p.read_text()
  for value in ('- source_check','- full_compare','- r1d4_matched_blind'):
   self.assertIn(value,s)
  self.assertIn('stockhunter-r1d4-BLIND-review-chart-pairs-NO-FUTURE',s)
  self.assertIn('stockhunter-r1d4-SEALED-paired-outcomes-and-source',s)
  self.assertIn('stockhunter-r1d3-winner-failure-asof-and-outcome-audit',s)
 def test_14_reviewer_blind_no_signal_identity(self):
  import inspect
  src=inspect.getsource(m.render_one)
  self.assertNotIn('row.code',src)
  self.assertNotIn('signal_date',src)
 def test_15_pair_metric_only_for_retrospective_description(self):
  self.assertIn('MATCHED_DATE',m.VERSION)

if __name__=='__main__':unittest.main()
