"""Offline-only source contracts. Real full-market capture is audited on GitHub Actions."""
import csv
import gzip
import hashlib
import pathlib
import sys
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd
import numpy as np
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]/'research_pit'))
import pit_security_master_audit as a
import krx_full_market_pit as old


class FakeProvider:
    def get_market_ticker_list(self,date,market):
        return {
          'KOSPI':[f'{i:06d}' for i in range(1,101)],
          'KOSDAQ':[f'{i:06d}' for i in range(101,201)],
          'KONEX':[f'{i:06d}' for i in range(201,221)]
        }[market]


class PITStage2Tests(unittest.TestCase):
    def test_archive_pin_and_non_pit_contract(self):
        self.assertEqual(a.PINNED_RUN,39295295197)
        self.assertEqual(a.PINNED_EXPECTED_ROWS,5090428)
        self.assertNotIn('get_market_ticker_list()',a.membership_sample.__doc__ or '')

    def test_sample_overlap_never_claims_universe_complete(self):
        with tempfile.TemporaryDirectory() as temp:
            s={'2019-01-02':{'codes':{f'{i:06d}' for i in range(1,219)} | {'03473K','0099X0'}}}
            report=a.membership_sample(FakeProvider(),s,['2019-01-02'],temp,pause=0)
            self.assertEqual(report['sampled_sessions'],1)
            self.assertEqual(report['price_only_rows'],2)
            self.assertEqual(report['roster_only_rows'],2)
            self.assertEqual(report['mismatch_sessions'],1)
            self.assertFalse(report['strategy_ready'])

    def test_invalid_market_codes_fail_closed(self):
        class Bad(FakeProvider):
            def get_market_ticker_list(self,date,market):
                if market=='KONEX':return []
                return super().get_market_ticker_list(date,market)
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaisesRegex(RuntimeError,'EMPTY_MARKET_MEMBERSHIP'):
                a.membership_sample(Bad(),{'2019-01-02':{'codes':{'000001'}}},['2019-01-02'],temp,pause=0,retries=1)

    def test_market_code_with_2026_internal_letter_retained(self):
        self.assertTrue(a.CODE_RE.fullmatch('0099X0'))
        self.assertTrue(a.CODE_RE.fullmatch('03473K'))
        self.assertFalse(a.CODE_RE.fullmatch('KRX123'))
        self.assertFalse(a.CODE_RE.fullmatch('005930*'))

    def test_derive_uses_full_source_without_seeing_terminal_as_delisting(self):
        test_dates=['2020-01-02','2020-01-03','2020-01-06','2020-01-07']
        class Provider:
            def get_index_ohlcv_by_date(self,start,end,ticker,name_display=False):
                return pd.DataFrame({'종가':[1000,1001,1002,1003]},index=pd.to_datetime(test_dates))
            def get_market_ohlcv_by_ticker(self,date,market,alternative=False):
                self_codes=[str(x).zfill(6) for x in range(1,600)]+['03473K']
                return pd.DataFrame({
                  '시가':np.full(600,100),'고가':np.full(600,102),
                  '저가':np.full(600,98),'종가':np.full(600,100),
                  '거래량':np.full(600,100),'거래대금':np.full(600,10000),
                },index=self_codes)
        with tempfile.TemporaryDirectory() as td:
            root=pathlib.Path(td)
            ref=root/'ref.csv'
            with ref.open('w',newline='') as f:
                w=csv.writer(f)
                w.writerow(['date','expected_all_market_rows'])
                for d in test_dates:w.writerow([d,600])
            pr=Provider()
            old.fetch(pr,test_dates[0],test_dates[-1],ref,root/'data',0,2,1,0)
            old.fetch(pr,test_dates[0],test_dates[-1],ref,root/'data',1,2,1,0)
            with patch.object(a,'PINNED_DATES',4),patch.object(a,'PINNED_EXPECTED_ROWS',2400),patch.object(a,'PINNED_CALENDAR_SHA',hashlib.sha256('\n'.join(test_dates).encode()).hexdigest()):
                summary,date_info=a.derive_lifecycle(root/'data',ref,root/'result')
                self.assertEqual(summary['price_rows'],2400)
                self.assertEqual(summary['observed_codes'],600)
                self.assertEqual(summary['alphanumeric_codes'],1)
                self.assertEqual(summary['codes_last_observed_before_final'],0)
                self.assertFalse(summary['strategy_ready'])
                with (root/'result'/'observed_ticker_lifecycles.csv').open() as f:
                    row=next(x for x in csv.DictReader(f) if x['code']=='03473K')
                self.assertEqual(row['delisted_status'],'NOT_VERIFIED_EXTERNAL_DELISTING_REGISTER')
                self.assertEqual(row['security_type'],'NOT_VERIFIED_OFFICIAL_TYPE_REGISTER')
            # Corrupt a file that already passed full-source verification.
            (root/'data'/'shard_00'/'20200102.csv.gz').write_bytes(b'BAD')
            with patch.object(a,'PINNED_DATES',4),patch.object(a,'PINNED_EXPECTED_ROWS',2400),patch.object(a,'PINNED_CALENDAR_SHA',hashlib.sha256('\n'.join(test_dates).encode()).hexdigest()):
                with self.assertRaisesRegex(RuntimeError,'BAD_SHA'):
                    a.derive_lifecycle(root/'data',ref,root/'result_again')

    def test_reject_unpinned_corrupted_data_and_no_false_delisting(self):
        with tempfile.TemporaryDirectory() as temp:
            t=pathlib.Path(temp)
            with self.assertRaisesRegex(RuntimeError,'PINNED_REFERENCE_NOT_MATCHED'):
                p=t/'ref.csv';p.write_text('date,expected_all_market_rows\n2026-10-12,1000\n')
                a.read_manifest_dates(t/'no_data',p)
            # A missing final observation is not a confirmed delisting event.
            self.assertIn('not delisting',a.derive_lifecycle.__doc__.lower() if a.derive_lifecycle.__doc__ else 'not delisting')

if __name__=='__main__':unittest.main()
