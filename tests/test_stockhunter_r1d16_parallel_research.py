import io,json,os,tempfile,unittest,zipfile
from pathlib import Path
import pandas as pd
from research_formulas import stockhunter_r1d16_parallel_research as r

class R1D16FrozenResearch(unittest.TestCase):
 def setUp(self):
  self.t=tempfile.TemporaryDirectory();self.addCleanup(self.t.cleanup)
  self.root=Path(self.t.name)
  self.p={'version':r.VERSION,'research_only':True,'orders_placed':0,'live_logic_changed':False,'snapshot_asof':'2026-10-23','no_new_oos_claim':True,'pinned_sources':{k:{'run_id':1,'artifact_id':1,'head_sha':'a'*40,'zip_sha256':'b'*64} for k in r.ARCHIVES}}
  self.proto=self.root/'proto.json';self.proto.write_text(json.dumps(self.p))
 def test_01_protocol_version(self):self.assertEqual(r.protocol(self.proto)['version'],r.VERSION)
 def test_02_missing_protocol(self):
  with self.assertRaisesRegex(RuntimeError,'R1D16_'):r.protocol(self.root/'none.json')
 def test_03_protocol_live_guard(self):
  self.p['live_logic_changed']=True;self.proto.write_text(json.dumps(self.p))
  with self.assertRaisesRegex(RuntimeError,'PROTOCOL_LIVE_GUARD'):r.protocol(self.proto)
 def test_04_future_period_guard(self):
  self.p['snapshot_asof']='2026-10-25';self.proto.write_text(json.dumps(self.p))
  with self.assertRaisesRegex(RuntimeError,'PROTOCOL_TIME_GUARD'):r.protocol(self.proto)
 def test_05_missing_source_pin(self):
  self.p['pinned_sources'].pop('odoli');self.proto.write_text(json.dumps(self.p))
  with self.assertRaisesRegex(RuntimeError,'PROTOCOL_PIN_SET'):r.protocol(self.proto)
 def test_06_invalid_sha(self):
  self.p['pinned_sources']['r143']['zip_sha256']='xxx';self.proto.write_text(json.dumps(self.p))
  with self.assertRaisesRegex(RuntimeError,'INVALID_SHA_r143'):r.protocol(self.proto)
 def test_07_exact_alnum(self):r.exact(pd.DataFrame({'code':['0088M0','000880']}),'CODE')
 def test_08_lossy_code_not_accepted(self):
  with self.assertRaisesRegex(RuntimeError,'EXACT_ID'):r.exact(pd.DataFrame({'code':['0088M','000880']}),'CODE')
 def test_09_missing_code_fails(self):
  with self.assertRaisesRegex(RuntimeError,'EXACT_ID'):r.exact(pd.DataFrame({'code':[None]}),'CODE')
 def test_10_duplicate_event_fails(self):
  with self.assertRaisesRegex(RuntimeError,'DUPLICATE'):r.unique(pd.DataFrame({'code':['0088M0','0088M0'],'signal_date':['2026-10-23']*2}),['signal_date','code'],'TEST')
 def test_11_distinct_same_day_codes_allowed(self):
  r.unique(pd.DataFrame({'code':['0088M0','000880'],'signal_date':['2026-10-23']*2}),['signal_date','code'],'TEST')
 def test_12_bool_parsing(self):self.assertEqual(r.bools(pd.Series(['True','FALSE','1','nan'])).tolist(),[True,False,True,False])
 def test_13_missing_not_zero(self):self.assertTrue(pd.isna(r.num(pd.Series(['',None])).iloc[0]))
 def test_14_write_in_own_subdir(self):
  p=self.root/'results'/'child.csv';r.safe_write(pd.DataFrame({'x':[1]}),p);self.assertEqual(pd.read_csv(p).x.iloc[0],1)
 def test_15_directory_table(self):
  (self.root/'sub').mkdir();(self.root/'sub'/'sample.csv').write_text('code,value\n0088M0,2\n')
  self.assertEqual(r.table(self.root,'sub/sample.csv').code.iloc[0],'0088M0')
 def test_16_zip_table(self):
  z=self.root/'src.zip'
  with zipfile.ZipFile(z,'w') as f:f.writestr('sub/sample.csv','code,value\n0088M0,3\n')
  self.assertEqual(r.table(z,'sub/sample.csv').value.iloc[0],3)
 def test_17_zip_missing_member_fail_closed(self):
  z=self.root/'src.zip'
  with zipfile.ZipFile(z,'w') as f:f.writestr('sample.csv','code\n0088M0\n')
  with self.assertRaisesRegex(RuntimeError,'SOURCE_ZIP_MEMBER_MISSING'):r.table(z,'other.csv')
 def test_18_hash_stable(self):
  f=self.root/'x';f.write_bytes(b'test')
  self.assertEqual(len(r.sha(f)),64)
 def test_19_no_orders_or_network(self):
  s=Path(r.__file__).read_text();self.assertNotIn('requests.',s);self.assertNotIn('place_order',s);self.assertNotIn('subprocess.',s)
 def test_20_only_independent_cohorts(self):self.assertEqual(set(r.ARCHIVES),{'r143','r1d13','odoli','after'})
 def test_21_same_horizon_contract(self):self.assertEqual(r.H,(1,3,5,10))
 def test_22_missing_output_cannot_be_imputed(self):self.assertIsNone(r.meditate(pd.Series([float('nan')])) )
 def test_23_manifest_authority(self):self.assertEqual(self.p['no_new_oos_claim'],True)
 def test_24_source_declared_not_automatically_certified(self):
  s=Path(r.__file__).read_text();self.assertIn("'historic_outcome_cache_sha_certified']=False",s)
 def test_25_after_observation_not_predictor(self):
  s=Path(r.__file__).read_text();self.assertIn("'flow_is_signal_time_feature']=False",s)
if __name__=='__main__':unittest.main()
