import os,unittest,tempfile
from pathlib import Path
from unittest.mock import patch
import pandas as pd
from r1d44_krx_sector_mcap_quarter import norm,save,select_dates,diagnose_source,code_audit,normalize_code
class TestMarketCapIndustry(unittest.TestCase):
 def fixture(self,market='KOSPI',date='2025-09-23'):
  return pd.DataFrame({'종목명':['삼성전자','SK하이닉스'],'업종명':['전기전자','전기전자'],'시가총액':['123,000,000,000','45,000,000,000'],'종가':[60000,230000]},index=['005930','000660'])
 def test_exact_mcap_and_sector(self):
  d=norm(self.fixture(),'KOSPI','2025-09-23');self.assertEqual(d.mcap_krw.tolist(),[123000000000,45000000000]);self.assertEqual(d.industry.tolist(),['전기전자']*2)
 def test_reject_zero_missing(self):
  a=self.fixture();a.iloc[0,a.columns.get_loc('시가총액')]=0
  with self.assertRaises(ValueError):norm(a,'KOSPI','2025-09-23')
 def test_reject_non_sixdigit(self):
  a=self.fixture();a.index=['A005930','000660']
  with self.assertRaises(ValueError):norm(a,'KOSPI','2025-09-23')
 def test_reject_missing_sector(self):
  a=self.fixture();a.iloc[0,a.columns.get_loc('업종명')]=None
  with self.assertRaises(ValueError):norm(a,'KOSPI','2025-09-23')
 def test_duplicate_ticker_across_markets(self):
  d=norm(self.fixture(),'KOSPI','2025-09-23')
  with tempfile.TemporaryDirectory() as t:
   with self.assertRaises(ValueError):save('2025-09-23',[d,d],t)
class TestCanonicalKrxCodes(unittest.TestCase):
 def fixture(self,codes):
  return pd.DataFrame({'종목명':['a']*len(codes),'업종명':['전자']*len(codes),'시가총액':['5000000000']*len(codes),'종가':[10000]*len(codes)},index=codes)
 def test_preserve_official_alphanumeric_six(self):
  a=norm(self.fixture(['005930','0010K0']),'KOSPI','2025-09-23')
  self.assertEqual(a.code.tolist(),['005930','0010K0'])
  self.assertEqual(a.attrs['alphanumeric_6_rows'],1)
 def test_allow_numeric_short_padded_and_audit(self):
  a=norm(self.fixture(['5930','0010K0']),'KOSPI','2025-09-23')
  self.assertEqual(a.code.tolist(),['005930','0010K0'])
  self.assertEqual(a.attrs['numeric_padded_rows'],1)
 def test_bad_prefix_must_not_be_stripped(self):
  for raw in ['A005930','KRX-005930','0000X00','---']:
   with self.subTest(raw=raw),self.assertRaises(ValueError):normalize_code(raw)
 def test_duplicate_after_zero_pad_fails(self):
  with self.assertRaisesRegex(ValueError,'DUPLICATE_CODE'):
   norm(self.fixture(['5930','005930']),'KOSPI','2025-09-23')
 def test_mixed_index_audit_uses_counts_not_codes(self):
  a=code_audit(['005930','0010K0','A005930','1234','unknown code'])
  self.assertEqual(a['numeric_6_rows'],1)
  self.assertEqual(a['alphanumeric_6_rows'],1)
  self.assertEqual(a['unsupported_rows'],2)
  self.assertNotIn('005930',str(a))
 def test_diagnostic_captures_code_shapes_without_source_values(self):
  def raw(day,market):return self.fixture(['005930','0010K0'])
  with tempfile.TemporaryDirectory() as d,patch.dict(os.environ,{'KRX_ID':'foo','KRX_PW':'bar'}):
   status=diagnose_source(d,fetch=raw)
   self.assertTrue(status['source_verified'])
   self.assertEqual(status['probes'][0]['code_shape_audit']['alphanumeric_6_rows'],1)
   self.assertNotIn('0010K0',(Path(d)/'DIAGNOSTIC.json').read_text())

class TestSafeDiagnostic(unittest.TestCase):
 def test_provider_failure_is_redacted_and_not_claimed_success(self):
  def bad(date,market):raise ValueError('SECRET-MUST-NOT-APPEAR')
  with tempfile.TemporaryDirectory() as t,patch.dict(os.environ,{'KRX_ID':'notprinted','KRX_PW':'notprinted'}):
   state=diagnose_source(t,fetch=bad)
   self.assertFalse(state['source_verified'])
   data=(Path(t)/'DIAGNOSTIC.json').read_text()
   self.assertNotIn('SECRET-MUST-NOT-APPEAR',data)
   self.assertNotIn('notprinted',data)
   self.assertEqual(len(state['probes']),4)
   self.assertTrue(all(x['stage']=='PROVIDER_FETCH' for x in state['probes']))
 def test_normalizer_failure_code_and_dates_are_identified(self):
  def bad(date,market):return pd.DataFrame()
  with tempfile.TemporaryDirectory() as t,patch.dict(os.environ,{'KRX_ID':'u','KRX_PW':'p'}):
   state=diagnose_source(t,fetch=bad)
   self.assertEqual([x['error_code'] for x in state['probes']],['EMPTY_OR_SCHEMA_INVALID_KOSPI','EMPTY_OR_SCHEMA_INVALID_KOSDAQ']*2)
 def test_valid_two_date_probe_does_not_save_raw_data(self):
  def good(date,market):
   return pd.DataFrame({'종목명':['sample'],'업종명':['전자'],'시가총액':['5000000000'],'종가':[10000]},index=['005930'])
  with tempfile.TemporaryDirectory() as t,patch.dict(os.environ,{'KRX_ID':'u','KRX_PW':'p'}):
   state=diagnose_source(t,fetch=good)
   self.assertEqual(state['status'],'PASS_2DATES_2MARKETS')
   self.assertEqual([p.name for p in Path(t).iterdir()],['DIAGNOSTIC.json'])
   self.assertNotIn('sample',(Path(t)/'DIAGNOSTIC.json').read_text())
 def test_missing_secrets_stops_without_network(self):
  with tempfile.TemporaryDirectory() as t,patch.dict(os.environ,{'KRX_ID':'','KRX_PW':''}):
   state=diagnose_source(t,fetch=lambda *x:self.fail('must not fetch'))
   self.assertEqual(state['status'],'BLOCKED_NO_KRX_LOGIN_PAIR')

if __name__=='__main__':unittest.main()
