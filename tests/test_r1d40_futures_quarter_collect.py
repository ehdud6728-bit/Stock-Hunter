import csv
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import pandas as pd
from research_formulas.r1d40_futures_quarter_collect import validate_contracts,quarter_dates,run

CAL='frozen_krx_sessions_2021_2025.csv'

class TestR1D40(unittest.TestCase):
    def test_real_pinned_sessions(self):
        self.assertEqual(sum(len(quarter_dates(Path(CAL),yr,q)) for yr in range(2021,2026) for q in range(1,5)),1225)
        self.assertIn('20250923',quarter_dates(Path(CAL),2025,3))
        self.assertIn('20250924',quarter_dates(Path(CAL),2025,3))
    def fake_df(self, product):
        return pd.DataFrame([
            {'종목명':'코스피200 F 202512 (주간)' if product=='KRDRVFUK2I' else '코스닥150 F 202512 (주간)',
            '시가':100,'고가':102,'저가':99,'종가':101,'거래량':1000},
            {'종목명':'코스피200 F 202603 (주간)' if product=='KRDRVFUK2I' else '코스닥150 F 202603 (주간)',
            '시가':100,'고가':102,'저가':99,'종가':101,'거래량':10},
            {'종목명':'코스피200 F 202612 SP' if product=='KRDRVFUK2I' else '코스닥150 F 202612 SP',
            '시가':100,'고가':102,'저가':99,'종가':101,'거래량':10000}
        ],index=['101WC000','A0163000','S0000000'])
    def test_contract_filter(self):
        rows,meta=validate_contracts(self.fake_df('KRDRVFUK2I'),'20250923','KOSPI200')
        self.assertEqual(len(rows),2)
        self.assertEqual(rows[0]['contract_code'],'101WC000')
        self.assertEqual(meta['excluded_nontarget_rows'],1)
    def test_credentials_fail_closed(self):
        with tempfile.TemporaryDirectory() as t:
            res=run(Path(CAL),2025,3,Path(t),env={},provider=lambda *_:self.fake_df('KRDRVFUK2I'),pause=0)
            self.assertEqual(res['status'],'CREDENTIALS_NOT_INJECTED')
            self.assertFalse((Path(t)/'VALIDATED_CONTRACTS.csv.gz').exists())
    def test_full_quarter_mock_and_resume(self):
        called=[]
        def provider(day,product,alternative=False):
            called.append((day,product));return self.fake_df(product)
        with tempfile.TemporaryDirectory() as t:
            env={'KRX_ID':'fake-id-private','KRX_PW':'fake-password-private'}
            res=run(Path(CAL),2025,3,Path(t),env=env,provider=provider,pause=0)
            self.assertEqual(res['status'],'COMPLETE_SOURCE_ONLY_NO_STRATEGY_INFERENCE')
            self.assertEqual(res['completed_requests'],res['expected_requests'])
            self.assertTrue((Path(t)/'VALIDATED_CONTRACTS.csv.gz').exists())
            self.assertNotIn('fake-password-private',(Path(t)/'COLLECTION_AUDIT.json').read_text())
            before=len(called)
            res2=run(Path(CAL),2025,3,Path(t),env=env,provider=provider,pause=0)
            self.assertEqual(len(called),before)
            self.assertEqual(res2['status'],'COMPLETE_SOURCE_ONLY_NO_STRATEGY_INFERENCE')
    def test_bad_day_blocks_publish(self):
        def provider(day,prod,alternative=False):
            if day=='20250923':return pd.DataFrame()
            return self.fake_df(prod)
        with tempfile.TemporaryDirectory() as t:
            res=run(Path(CAL),2025,3,Path(t),env={'KRX_ID':'a','KRX_PW':'b'},provider=provider,pause=0)
            self.assertEqual(res['status'],'INCOMPLETE_FAIL_CLOSED')
            self.assertFalse((Path(t)/'VALIDATED_CONTRACTS.csv.gz').exists())
            self.assertEqual(len(res['failed_requests']),2)

if __name__=='__main__':unittest.main()
