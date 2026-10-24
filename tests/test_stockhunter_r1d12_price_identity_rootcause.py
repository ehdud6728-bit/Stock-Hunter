"""R1-D12 self-contained fail-closed/provenance tests (no market access)."""
import ast,hashlib,json,tempfile,unittest,inspect,zipfile
from pathlib import Path
from unittest.mock import patch
import pandas as pd
from research_formulas import stockhunter_r1d12_price_identity_rootcause as d

class TestR1D12(unittest.TestCase):
 def test_01_lossy_stock_id_reproducible(self):
  self.assertEqual(d.legacy_digit_only('0088M0'),'000880')
  self.assertNotEqual('0088M0','000880')
 def test_02_exact_alphanumeric_preserved(self):
  self.assertEqual(d.ticker('0088m0'),'0088M0')
  self.assertEqual(d.ticker('000880'),'000880')
 def test_03_empty_invalid_identity_rejected(self):
  for x in ['', '123', 'A0088M0','../001','0088-M0']:
   with self.assertRaisesRegex(RuntimeError,'INVALID_EXACT_TICKER'):
    d.ticker(x)
 def test_04_no_trading_controls(self):
  self.assertTrue(d.RESEARCH_ONLY)
  src=inspect.getsource(d)
  for token in ('send_order(', 'place_order(', 'LIVE_ORDER', 'GridSearchCV', 'optuna.'):
   self.assertNotIn(token,src)
 def test_05_sha256_and_git_blob_not_confused(self):
  raw=b'hello\n'
  self.assertEqual(d.blob(raw),hashlib.sha1(b'blob 6\0hello\n').hexdigest())
  self.assertNotEqual(d.blob(raw),d.sha(raw))
 def test_06_artifact_digests_frozen(self):
  self.assertEqual(d.PIN['trust']['id'],12504190178)
  self.assertEqual(d.PIN['r143']['id'],12504990509)
  self.assertEqual(d.PIN['r1d10']['id'],12551784748)
  self.assertTrue(all(len(v['sha256'])==64 for v in d.PIN.values()))
 def test_07_blobs_pinned_source_not_edited(self):
  self.assertEqual(len(d.SRC),3)
  self.assertTrue(all(len(v[1])==40 for v in d.SRC.values()))
 def test_08_static_attestation_never_imports_actual_source(self):
  a='import re\ndef check(v):\n s=re.sub(r"\\D","",str(v))\n return s[-6:].zfill(6)\n'
  b='def keep(v):\n s="".join(c for c in v if c.isalnum())\n return s.zfill(6)\n'
  with tempfile.TemporaryDirectory() as td:
   root=Path(td);(root/'trust.py').write_text(a);(root/'r143.py').write_text(b)
   pins={'trust':('trust.py',d.blob(a.encode()),'check','LOSSY_DIGITS_ONLY'),
         'r143':('r143.py',d.blob(b.encode()),'keep','ALPHANUMERIC_RETAINED')}
   with patch.object(d,'SRC',pins):
    r=d.source_audit(root)
    self.assertEqual(len(r),2)
    self.assertTrue((~r.file_imported_or_executed).all())
 def test_09_source_mutation_blocks(self):
  with tempfile.TemporaryDirectory() as td:
   root=Path(td);(root/'x.py').write_text('def code(v):return v')
   with patch.object(d,'SRC',{'x':('x.py','0'*40,'code','LOSSY_DIGITS_ONLY')}):
    with self.assertRaisesRegex(RuntimeError,'SOURCE_GIT_BLOB_DRIFT'):d.source_audit(root)
 def test_10_duplicate_csv_input_blocks(self):
  with tempfile.TemporaryDirectory() as td:
   p=Path(td);(p/'one.csv').write_text('code\n0088M0\n');(p/'sub').mkdir();(p/'sub'/'one.csv').write_text('code\n0088M0\n')
   with self.assertRaisesRegex(RuntimeError,'ARTIFACT_CSV_NOT_UNIQUE'):d.read_artifact(p,'one.csv')
 def test_11_dir_and_zip_read_equivalent(self):
  with tempfile.TemporaryDirectory() as td:
   root=Path(td);(root/'reports').mkdir()
   (root/'reports'/'e.csv').write_text('code,score\n0088M0,1\n')
   archive=root/'a.zip'
   with zipfile.ZipFile(archive,'w') as z:z.write(root/'reports'/'e.csv','reports/e.csv')
   self.assertTrue(d.read_artifact(root/'reports','e.csv').equals(d.read_artifact(archive,'e.csv')))
 def test_12_errors_never_convert_missing_to_zero(self):
  self.assertIn('ARTIFACT_CSV_NOT_UNIQUE',inspect.getsource(d.read_artifact))
  self.assertIn('MISSING',inspect.getsource(d.analysis))
 def test_13_no_unverified_price_cache_certification(self):
  src=inspect.getsource(d.analysis)
  self.assertIn('identical_horizon_calendar_proven',src)
  self.assertIn('exact_cache_file_sha_available',src)
  self.assertIn('False',src)
 def test_14_fixed_horizon_contract(self):
  self.assertEqual(d.HORIZONS,(1,3,5,10))
 def test_15_analysis_only_writes_own_output(self):
  src=inspect.getsource(d)
  self.assertNotIn('to_sql(',src)
  self.assertNotIn('git push',src)
  self.assertIn('out/\'R1D12_MANIFEST.json\'',src)

if __name__=='__main__':unittest.main()
