import os,unittest,tempfile
from pathlib import Path
from unittest.mock import patch
import pandas as pd
from r1d44_krx_sector_mcap_quarter import norm,save,select_dates
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
if __name__=='__main__':unittest.main()
