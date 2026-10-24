"""R1-D15 immutable research proof: safe exact security identity and fail-closed cache joins."""
import unittest, tempfile, json, io, zipfile, pathlib, inspect, hashlib
import pandas as pd
from research_formulas import stockhunter_r1d15_cache_identity_quarantine as r

class TestR1D15(unittest.TestCase):
 def test_01_exact_preserves_alphanumeric(self): self.assertEqual(r.exact_code('0088m0'),'0088M0')
 def test_02_legacy_reproduction(self): self.assertEqual(r.legacy_code('0088M0'),'000880')
 def test_03_exact_never_equals_legacy_alias(self): self.assertNotEqual(r.exact_code('0088M0'),r.exact_code('000880'))
 def test_04_strips_suffix_prohibited(self):
  with self.assertRaises(ValueError):r.exact_code('0088M0.KQ')
 def test_05_rejects_silent_truncation(self):
  with self.assertRaises(ValueError):r.exact_code('0123456')
 def test_06_rejects_empty(self):
  with self.assertRaises(ValueError):r.exact_code('')
 def test_07_filename_exact_code(self):self.assertEqual(r.filename_code('0088M0_900.pkl.gz'),'0088M0')
 def test_08_filename_unknown_failed(self):self.assertEqual(r.filename_code('prefix_0088M0_900.pkl.gz'),'')
 def test_09_file_suffix_case(self):self.assertEqual(r.filename_code('000880_900.PKL.GZ'),'000880')
 def test_10_exact_missing_no_fallback(self):
  with self.assertRaisesRegex(ValueError,'R1D15_NO_UNIQUE_EXACT_PRICE_CACHE'):
   r.safe_lookup('0088M0',{'000880':['some cache']})
 def test_11_collision_quarantine(self):
  with self.assertRaisesRegex(ValueError,'R1D15_LEGACY_ALIAS_JOIN_FORBIDDEN'):
   r.safe_lookup('0088M0',{'0088M0':['meju'], '000880':['other']},join_mode='legacy')
 def test_11b_two_exact_codes_can_coexist_without_substitution(self):
  index={'0088M0':['meju'], '000880':['other']}
  self.assertEqual(r.safe_lookup('0088M0',index),'meju')
  self.assertEqual(r.safe_lookup('000880',index),'other')
 def test_12_code_without_collision(self):self.assertEqual(r.safe_lookup('000100',{'000100':['ok']}),'ok')
 def test_13_duplicate_exact_cache_quarantined(self):
  with self.assertRaisesRegex(ValueError,'R1D15_NO_UNIQUE_EXACT_PRICE_CACHE'):
   r.safe_lookup('000150',{'000150':['a','b']})
 def test_14_protocol_freeze(self):
  p=pathlib.Path(__file__).resolve().parents[1]/'R1D15_PREDECLARED_PROTOCOL.json'
  v=r.protocol_guard(p,None);self.assertEqual(v['schema'],r.SCHEMA)
 def test_15_wrong_protocol_fails(self):
  with tempfile.TemporaryDirectory() as d:
   f=pathlib.Path(d)/'protocol.json';f.write_text(json.dumps({'schema':'fake'}))
   with self.assertRaisesRegex(ValueError,'PREDECLARED_PROTOCOL_MISMATCH'):r.protocol_guard(f)
 def test_16_pin_is_frozen(self):
  self.assertEqual(r.PIN['run'],40099215663);self.assertEqual(len(r.PIN['sha256']),64)
 def test_17_read_only_contract(self):
  src=inspect.getsource(r)
  self.assertNotIn('actions/cache/save',src)
  self.assertNotIn('place_order(',src)
  self.assertNotIn('orders.submit',src)
  self.assertNotIn('from triangle1pb_research import',src)
 def test_18_pinned_zip_and_dir_equivalent(self):
  with tempfile.TemporaryDirectory() as d:
   root=pathlib.Path(d);csv='code,close\n0088M0,14500\n';doc=root/'reports/r1d14/result/sample.csv';doc.parent.mkdir(parents=True);doc.write_text(csv)
   z=root/'frozen.zip'
   with zipfile.ZipFile(z,'w') as f:f.write(doc,'reports/r1d14/result/sample.csv')
   a=r.frozen_csv(root,'sample.csv');b=r.frozen_csv(z,'sample.csv')
   self.assertTrue(a.equals(b))
 def test_19_missing_frozen_artifact_blocks(self):
  with tempfile.TemporaryDirectory() as d:
   with self.assertRaisesRegex(ValueError,'FROZEN_CSV_MISSING_OR_DUP'):r.frozen_csv(d,'missing.csv')
 def test_20_exact_bars_stay_separate(self):
  f=pd.DataFrame([{'relative_path':'0088M0_900.pkl.gz','r1d14_sha256':'abc'}, {'relative_path':'000880_900.pkl.gz','r1d14_sha256':'def'}])
  b=pd.DataFrame([{'cache_relative_path':'0088M0_900.pkl.gz','code':'000880','date':'2026-08-18','close':14000,'file_sha256':'abc'}, {'cache_relative_path':'000880_900.pkl.gz','code':'000880','date':'2026-08-18','close':125000,'file_sha256':'def'}])
  q=r.exact_bars(b,f)
  self.assertEqual(q.exact_file_code.nunique(),2)
  self.assertEqual(int(q.original_code_erased_by_old_loader.sum()),1)
 def test_21_same_code_date_from_two_files_blocks(self):
  f=pd.DataFrame([{'relative_path':'0088M0_900.pkl.gz','r1d14_sha256':'abc'}, {'relative_path':'0088M0_901.pkl.gz','r1d14_sha256':'def'}])
  b=pd.DataFrame([{'cache_relative_path':v,'code':'000880','date':'2026-08-18','close':14000,'file_sha256':c} for v,c in [('0088M0_900.pkl.gz','abc'),('0088M0_901.pkl.gz','def')]])
  with self.assertRaisesRegex(ValueError,'R1D15_DUPLICATED_EXACT_CODE_DATE'):r.exact_bars(b,f)
 def test_22_collision_expected(self):
  f=pd.DataFrame([{'exact_file_code':x,'legacy_numeric_alias':'000880'} for x in ['0088M0','000880']])
  p=pd.DataFrame([{'exact_file_code':'0088M0','date':'2026-08-18'}, {'exact_file_code':'000880','date':'2026-08-18'}])
  out=r.build_collision(f,p)
  self.assertEqual(len(out),2)
  self.assertTrue(out.join_by_legacy_alias.eq('BLOCKED_AMBIGUOUS_IDENTITY').all())
 def test_23_meju_signal_cutoff(self):
  p=pd.DataFrame([{'exact_file_code':'0088M0','date':'2026-08-18'}, {'exact_file_code':'000880','date':'2026-09-28'}])
  f=pd.DataFrame([{'exact_file_code':'0088M0'}, {'exact_file_code':'000880'}])
  a=r.signal_guard(p,f)
  m=a[a.requested_exact_code=='0088M0'].iloc[0]
  self.assertEqual(m.r1d14_post_signal_price_rows,0)
  self.assertEqual(m.horizon_return_status,'BLOCK_MISSING_EXACT_POST_SIGNAL_SOURCE')
 def test_24_frozen_file_cannot_be_assumed_historic(self):
  with tempfile.TemporaryDirectory() as d:
   files=pd.DataFrame([{'cache_relative_path':'0088M0_900.pkl.gz','sha256':'abc','byte_size':1,'parsed_loader_code':'000880','first_date':'2026-03-26','last_date':'2026-08-18'}])
   f=r.file_evidence(files,pathlib.Path(d))
   self.assertFalse(bool(f.iloc[0].historic_as_used_certified));self.assertEqual(f.iloc[0].cache_evidence_state,'NO_CURRENT_RESTORED_FILE')
 def test_25_no_fake_prices_missing_source(self):
  with tempfile.TemporaryDirectory() as d:
   files=pd.DataFrame([{'cache_relative_path':'000880_900.pkl.gz','sha256':'a'*64,'byte_size':1,'parsed_loader_code':'000880','first_date':'2026-03-26','last_date':'2026-08-18'}])
   f=r.file_evidence(files,pathlib.Path(d))
   self.assertEqual(f.iloc[0].currently_restored_sha256,'')

if __name__=='__main__':unittest.main()
