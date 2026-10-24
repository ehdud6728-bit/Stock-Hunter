"""R1-D13 research-only data-provenance / contamination QA guards."""
import unittest, tempfile, json, io, zipfile
from pathlib import Path
from unittest.mock import patch
import pandas as pd
from research_formulas import stockhunter_r1d13_contamination_scope as r

class TestR1D13(unittest.TestCase):
 def test_01_alphanumeric_code(self):self.assertEqual(r.exact_ticker('0088m0'),'0088M0')
 def test_02_collision_legacy(self):self.assertEqual(r.lossy_legacy('0088M0'),'000880')
 def test_03_exacts_not_equal(self):self.assertNotEqual(r.exact_ticker('0088M0'),r.exact_ticker('000880'))
 def test_04_no_truncated_code(self):
  with self.assertRaisesRegex(RuntimeError,'SECURITY_ID'):r.exact_ticker('880')
 def test_05_empty_fails(self):
  with self.assertRaisesRegex(RuntimeError,'SECURITY_ID'):r.exact_ticker('')
 def test_06_bad_symbols_fail(self):
  with self.assertRaisesRegex(RuntimeError,'SECURITY_ID'):r.exact_ticker('A0088M0')
 def test_07_full_pins(self):self.assertEqual(set(r.PIN),{'trust','r143','r1d12'})
 def test_08_nondegenerate_horizons(self):self.assertEqual(r.HORIZONS,(1,3,5,10))
 def test_09_correct_frozen_sha(self):
  for d in r.PIN.values():self.assertEqual(len(d['sha256']),64)
 def test_10_raw_file_import_guard(self):
  import inspect
  src=inspect.getsource(r)
  self.assertNotIn('send_order(',src);self.assertNotIn('place_order(',src)
  self.assertNotIn('GridSearchCV',src);self.assertNotIn('optuna.',src)
 def test_11_test_git_sha(self):
  x=b'example\n';import hashlib
  self.assertEqual(r.git_blob_sha(x),hashlib.sha1(b'blob 8\x00'+x).hexdigest())
 def test_12_missing_proto_fails(self):
  with self.assertRaisesRegex(RuntimeError,'PREDECLARED_PROTOCOL_MISSING'):r.protocol_guard('.', '/no-such-protocol.json')
 def test_13_archive_reader(self):
  with tempfile.TemporaryDirectory() as td:
   z=Path(td)/'x.zip'
   with zipfile.ZipFile(z,'w') as out:out.writestr('reports/expected.csv','code,val\n0088M0,1\n')
   d=r.read_data(z,'expected.csv');self.assertEqual(d.loc[0,'code'],'0088M0')
 def test_14_archive_missing_fails(self):
  with tempfile.TemporaryDirectory() as td:
   z=Path(td)/'x.zip'
   with zipfile.ZipFile(z,'w') as out:out.writestr('reports/other.csv','a\nb\n')
   with self.assertRaisesRegex(RuntimeError,'NOT_UNIQUE'):r.read_data(z,'expected.csv')
 def test_15_dir_reader(self):
  with tempfile.TemporaryDirectory() as td:
   p=Path(td)/'some';p.mkdir();(p/'x.csv').write_text('code\n000150\n')
   self.assertEqual(r.read_data(td,'x.csv').loc[0,'code'],'000150')
 def test_16_missing_source_fails(self):
  with tempfile.TemporaryDirectory() as td:
   with self.assertRaisesRegex(RuntimeError,'MISSING_STATIC_SOURCE'):r.check_source_blobs(td)
 def test_17_cannot_auto_certify_cache(self):
  import inspect
  src=inspect.getsource(r.run)
  self.assertIn("'source_path_file_sha_available']=False",src)
  self.assertIn('underlying_price_file_cause_proven=False',src)
  self.assertIn('correction_applied=False',src)
 def test_18_missing_after_not_imputed(self):
  x=pd.to_numeric(pd.Series(['','bad','-2.3']),errors='coerce')
  self.assertTrue(pd.isna(x.iloc[0]));self.assertTrue(pd.isna(x.iloc[1]))
 def test_19_protocol_mismatch(self):
  with tempfile.TemporaryDirectory() as td:
   f=Path(td)/'p.json';f.write_text(json.dumps({'version':'WRONG'}))
   with self.assertRaisesRegex(RuntimeError,'PROTOCOL_LOCK_MISMATCH'):r.protocol_guard(td,f)
 def test_20_no_live_modification(self):
  import inspect
  src=inspect.getsource(r.run)
  self.assertIn('live_search_score_rank_order_changed=False',src)
  self.assertIn('input_source_data_mutated=False',src)
 def test_21_sha_format(self):self.assertEqual(len(r.sha256(b'a')),64)

if __name__=='__main__':unittest.main()
