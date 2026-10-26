import gzip, hashlib, importlib, io, json, os, sys, tempfile, unittest, zipfile
from pathlib import Path
import pandas as pd
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from research_formulas import stockhunter_r1d26k_matched_failure as mod

class R1D26KContracts(unittest.TestCase):
 @classmethod
 def setUpClass(cls):
  cls.p=json.loads((ROOT/'R1D26K_PREDECLARED_PROTOCOL.json').read_text());cls.zip25=os.environ.get('R1D26K_TEST_R1D25_ZIP');cls.ziph=os.environ.get('R1D26K_TEST_R1D26H_ZIP')
  cls.live=bool(cls.zip25 and Path(cls.zip25).exists() and cls.ziph and Path(cls.ziph).exists())
  if cls.live:
   cls.x=mod.build_data(mod.check_source(cls.zip25,cls.p['r1d25_sha256']),cls.p)
   cls.pairs=mod.pairs_same_month(cls.x,10)
 def test_protocol_isolation(self):
  self.assertTrue(self.p['research_only']);self.assertFalse(self.p['live_changed']);self.assertEqual(self.p['orders_placed'],0);self.assertTrue(self.p['matching_feature_blind'])
 def test_source_hash_tamper_rejected(self):
  with tempfile.NamedTemporaryFile() as f:
   f.write(b'fake');f.flush()
   with self.assertRaises(ValueError):mod.check_source(f.name,self.p['r1d25_sha256'])
 def test_month_date_gap_distinct_ticker_and_unique_controls(self):
  x=pd.DataFrame([{'strategy':mod.C,'year_month':'2025-04','date':'2025-04-11','code':'000001','label':'HIT15_CLOSE_PROFIT','hit15':1,'net_20bps_pct':10},
                  {'strategy':mod.C,'year_month':'2025-04','date':'2025-04-12','code':'000002','label':'NO_HIT5_CLOSE_LOSS5','hit15':0,'net_20bps_pct':-10}])
  for f in mod.FEATS[mod.C]:x[f]=1.
  res=mod.pairs_same_month(x,10)
  self.assertEqual(len(res),1);self.assertNotEqual(res.iloc[0].anchor_code,res.iloc[0].control_code)
  self.assertEqual(res.iloc[0].calendar_gap_days,1)
 def test_month_change_ignored(self):
  x=pd.DataFrame([{'strategy':mod.C,'year_month':'2025-04','date':'2025-04-30','code':'000001','label':'HIT15_CLOSE_PROFIT','hit15':1,'net_20bps_pct':10},
                  {'strategy':mod.C,'year_month':'2025-05','date':'2025-05-01','code':'000002','label':'NO_HIT5_CLOSE_LOSS5','hit15':0,'net_20bps_pct':-10}])
  for f in mod.FEATS[mod.C]:x[f]=1.
  self.assertEqual(len(mod.pairs_same_month(x,10)),0)
 def test_never_self_pair(self):
  x=pd.DataFrame([{'strategy':mod.C,'year_month':'2025-04','date':'2025-04-11','code':'000001','label':'HIT15_CLOSE_PROFIT','hit15':1,'net_20bps_pct':10},
                  {'strategy':mod.C,'year_month':'2025-04','date':'2025-04-12','code':'000001','label':'NO_HIT5_CLOSE_LOSS5','hit15':0,'net_20bps_pct':-10}])
  for f in mod.FEATS[mod.C]:x[f]=1.
  self.assertEqual(len(mod.pairs_same_month(x,10)),0)
 @unittest.skipUnless(os.environ.get('R1D26K_TEST_R1D25_ZIP'),'frozen archive absent')
 def test_actual_R1D25_price_source_counts(self):
  self.assertTrue(self.live);self.assertEqual(len(self.x),10198);self.assertEqual(sum(self.x.strategy==mod.C),7416);self.assertEqual(sum(self.x.strategy==mod.D),2782)
 @unittest.skipUnless(os.environ.get('R1D26K_TEST_R1D25_ZIP'),'frozen archive absent')
 def test_actual_labels_exclusive_and_stable(self):
  self.assertEqual(sum(self.x.label.eq('HIT15_CLOSE_LOSS5')),592)
  self.assertEqual(sum(self.x.label.eq('HIT15_CLOSE_PROFIT')),3154)
  self.assertEqual(sum(self.x.label.eq('NO_HIT5_CLOSE_LOSS5')),2247)
 @unittest.skipUnless(os.environ.get('R1D26K_TEST_R1D25_ZIP'),'frozen archive absent')
 def test_actual_exact_month_gap_and_no_control_reuse(self):
  p=self.pairs
  self.assertTrue((p.calendar_gap_days<=10).all())
  self.assertTrue((p.anchor_code!=p.control_code).all())
  self.assertTrue((p.anchor_date.str[:7]==p.control_date.str[:7]).all())
  self.assertFalse(p.duplicated(['contrast','strategy','control_date','control_code']).any())
  self.assertFalse(p.duplicated(['contrast','strategy','anchor_date','anchor_code']).any())
 @unittest.skipUnless(os.environ.get('R1D26K_TEST_R1D25_ZIP'),'frozen archive absent')
 def test_actual_pair_cardinalities(self):
  g=self.pairs.groupby(['contrast','strategy']).size()
  self.assertEqual(int(g['SUCCESS_VS_HARD_FAILURE',mod.C]),1327);self.assertEqual(int(g['SUCCESS_VS_HARD_FAILURE',mod.D]),471)
  self.assertEqual(int(g['PROFIT_KEEP_VS_GIVEBACK',mod.C]),470);self.assertEqual(int(g['PROFIT_KEEP_VS_GIVEBACK',mod.D]),111)
 @unittest.skipUnless(os.environ.get('R1D26K_TEST_R1D25_ZIP'),'frozen archive absent')
 def test_paired_statistic_deterministic(self):
  a=mod.agg_features(self.pairs,120);b=mod.agg_features(self.pairs,120)
  pd.testing.assert_frame_equal(a,b);self.assertEqual(len(a),42)
 @unittest.skipUnless(os.environ.get('R1D26K_TEST_R1D26H_ZIP'),'frozen archive absent')
 def test_true_krw_link_300_no_leak(self):
  df,tab=mod.flow_link(self.x,self.ziph)
  self.assertEqual(len(df),300);self.assertEqual(df._merge.eq('both').sum(),294)
  self.assertEqual(tab.n.sum(),294)
 def test_KRW_research_not_official_PIT(self):
  self.assertTrue(self.p['not_new_OOS']);self.assertTrue(self.p['do_not_change_strategy'])

if __name__=='__main__':unittest.main()
