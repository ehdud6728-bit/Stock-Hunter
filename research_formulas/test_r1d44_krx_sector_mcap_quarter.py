import os,unittest,tempfile
from pathlib import Path
from unittest.mock import patch
import pandas as pd
from r1d44_krx_sector_mcap_quarter import norm,save,select_dates,diagnose_source
class TestMarketCapIndustry(unittest.TestCase):
 def fixture(self,market='KOSPI',date='2025-09-23'):
  return pd.DataFrame({'종목명':['삼성전자','SK하이닉스'],'업종명':['전기전자','전기전자'],'시가총액':['123,000,000,000','45,000,000,000'],'종가':[60000,230000]},index=['005930','000660'])
 def test_exact_mcap_and_sector(self):
  d=norm(self.fixture(),'KOSPI','2025-09-23');self.assertEqual(d.mcap_krw.tolist(),[123000000000,45000000000]);self.assertEqual(d.industry.tolist(),['전기전자']*2)
 def test_reject_zero_missing(self):
  a=self.fixture();a.iloc[0,a.columns.get_loc('시가총액')]=0
  with self.assertRaises(ValueError):norm(a,'KOSPI','2025-09-23')
 def test_reject_non_sixdigit(self):
  a=self.fixture();a.index=['ABC','000660']
  with self.assertRaises(ValueError):norm(a,'KOSPI','2025-09-23')
 def test_reject_missing_sector(self):
  a=self.fixture();a.iloc[0,a.columns.get_loc('업종명')]=None
  with self.assertRaises(ValueError):norm(a,'KOSPI','2025-09-23')
 def test_duplicate_ticker_across_markets(self):
  d=norm(self.fixture(),'KOSPI','2025-09-23')
  with tempfile.TemporaryDirectory() as t:
   with self.assertRaises(ValueError):save('2025-09-23',[d,d],t)
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
