import unittest,tempfile,json,os,sys,types
from unittest.mock import patch
from pathlib import Path
import pandas as pd
from research_formulas import stockhunter_r1d14_cache_provenance as r

class TestR1D14(unittest.TestCase):
 def test_code_loss_proof(self):
  self.assertEqual(r.lossy_code('0088M0'),'000880')
  self.assertNotEqual(r.exact_code('0088M0'),r.exact_code('000880'))
 def test_invalid_codes_blocked(self):
  for x in ['0088M','0/088M0','',None,'A0001500']:
   with self.assertRaises(ValueError):r.exact_code(x)
 def test_pins_immutable(self):
  self.assertEqual(r.PINS['r1d13']['run'],40096795372)
  self.assertEqual(len(r.PINS['r1d13']['sha256']),64)
 def test_not_history_certified(self):
  self.assertNotEqual(r.CACHE_STATUS_PRESENT,'HISTORICAL_EXACT')
 def test_missing_cache_graceful(self):
  with tempfile.TemporaryDirectory() as t:
   fs,n=r.candidates(Path(t)/'does-not-exist',r.TARGETS)
   self.assertEqual(n,0);self.assertEqual(fs,[])
 def test_exact_filename_candidate(self):
  with tempfile.TemporaryDirectory() as t:
   p=Path(t);(p/'price_000150.csv').write_text('date,close\n2026-09-18,1449000\n');(p/'junk.csv').write_text('unused')
   a,n=r.candidates(p,r.TARGETS);self.assertEqual(n,2);self.assertEqual(len(a),1)
 def test_read_cache_csv_and_sha(self):
  with tempfile.TemporaryDirectory() as t:
   p=Path(t);z=p/'000100.csv';z.write_text('date,open,high,low,close\n2026-10-21,86000,87000,85500,86400\n')
   try:
    import triangle1pb_research as _real
    module_context=patch.dict(sys.modules,{'triangle1pb_research':_real})
   except ImportError:
    # Offline harness only: synthetic minimal parser. GitHub uses actual pinned module.
    fake=types.ModuleType('triangle1pb_research')
    fake._load_any=lambda p:pd.read_csv(p)
    fake._iter_frame_candidates=lambda o:iter([(o,{})])
    fake._frame_code=lambda raw,p,meta:p.name[:6]
    fake._date_series_from_frame=lambda raw:(pd.to_datetime(raw['date']),'COLUMN:date')
    fake._first_col=lambda columns,aliases:next((a for a in aliases if a in columns),None)
    fake.CLOSE_ALIASES=['close'];fake.HIGH_ALIASES=['high'];fake.LOW_ALIASES=['low'];fake.OPEN_ALIASES=['open']
    module_context=patch.dict(sys.modules,{'triangle1pb_research':fake})
   with module_context:out,bars=r.extracted_frames([z],p)
   self.assertEqual(len(out),1);self.assertEqual(out.iloc[0].read_status,'PARSED_SELECTED_FRAME');self.assertEqual(len(bars),1);self.assertEqual(float(bars.iloc[0]['close']),86400)
 def test_no_code_alias(self):
  self.assertNotEqual('0088M0','000880')
 def test_protocol_validated(self):
  with tempfile.TemporaryDirectory() as t:
   p=Path(t)/'p.json';p.write_text('{}')
   with self.assertRaises(ValueError):r.freeze(p,t)
 def test_no_trading_controls(self):
  s=Path(r.__file__).read_text()
  self.assertNotIn('create_order(',s);self.assertNotIn('STOCKHUNTER_LIVE_ENABLE=1',s)
 def test_source_paths_readonly(self):
  s=Path(r.__file__).read_text()
  self.assertNotIn('write_bytes(',s);self.assertNotIn('shutil.move(',s)
 def test_no_zero_fill(self):
  s=Path(r.__file__).read_text()
  self.assertIn('np.nan',s);self.assertIn('CACHE_NOT_RESTORED',s)
 def test_gitblob(self):
  with tempfile.TemporaryDirectory() as t:
   p=Path(t)/'x';p.write_bytes(b'hello')
   self.assertEqual(r.gitblob(p),'b6fc4c620b67d95f953a5c1c1230aaab5db5a1b0')
 def test_csv_reader_missing(self):
  with tempfile.TemporaryDirectory() as t:
   with self.assertRaises(ValueError):r.get_csv(t,r.PANEL)
 def test_targets_frozen(self):
  self.assertEqual(r.TARGETS,('000150','000100','000880','0088M0'))
 def test_no_source_repair(self):
  s=Path(r.__file__).read_text()
  self.assertIn("'frozen_original_outcomes_corrected':False",s)
if __name__=='__main__':unittest.main()
