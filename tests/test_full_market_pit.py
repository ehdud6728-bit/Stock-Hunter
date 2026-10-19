"""Offline contract tests: no KRX network required."""
import csv
import json
import pathlib
import sys
import tempfile
import unittest
from unittest.mock import patch

import numpy as np
import pandas as pd

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]/'research_pit'))
import krx_full_market_pit as k

DATES = ['2020-01-02','2020-01-03','2020-01-06','2020-01-07']


def fake_raw(n=600):
    codes = [str(i).zfill(6) for i in range(1,n+1)]
    a = pd.DataFrame({'시가':np.full(n,100), '고가':np.full(n,105),
                      '저가':np.full(n,99), '종가':np.full(n,101),
                      '거래량':np.full(n,10000), '거래대금':np.full(n,1010000)},index=codes)
    # A real zero-volume suspension-like bar: keep intact, don't treat as tradeable.
    a.loc['000599',['시가','고가','저가','거래량','거래대금']]=0
    return a


class FakeKRX:
    def __init__(self):self.calls=[]
    def get_index_ohlcv_by_date(self, start, end, ticker, name_display=False):
        self.calls.append(('index',ticker,start,end))
        return pd.DataFrame({'종가':[1000,1001,1002,1003]}, index=pd.to_datetime(DATES))
    def get_market_ohlcv_by_ticker(self,date,market,alternative=False):
        assert market=='ALL' and alternative is False
        self.calls.append(('ALL',date))
        return fake_raw()


def make_reference(path):
    with open(path,'w',newline='',encoding='utf-8') as f:
        w=csv.writer(f);w.writerow(['date','expected_all_market_rows','prior_selected_rows','prior_selected_sha256'])
        for d in DATES:w.writerow([d,600,190,'deadbeef'])


class PITTests(unittest.TestCase):
    def test_no_frozen_universe_filter(self):
        df,flags=k.normalize_day(fake_raw(),DATES[0]);self.assertEqual(len(df),600)
        self.assertEqual(df.code.nunique(),600)
        self.assertEqual(flags['zero_volume_rows'],1)
        self.assertEqual(df.loc[df.code=='000599','volume'].item(),0)
        self.assertEqual(df.loc[df.code=='000599','close'].item(),101)
        # Many tickers not in a hypothetical current-universe list of first 190.
        self.assertIn('000600',set(df.code))
    def test_real_krx_alphanumeric_stock_codes_not_dropped(self):
        # Actual historical response includes 03473K (SK preferred), plus other
        # Korean listed alphabetic-suffix securities. These are not corrupted codes.
        src=fake_raw()
        ix=list(src.index)
        ix[0]='03473K';ix[1]='00088K';ix[2]='00593K'
        src.index=ix
        df,flags=k.normalize_day(src,DATES[0])
        self.assertEqual(len(df),600)
        self.assertEqual(df.code.nunique(),600)
        for ticker in ('03473K','00088K','00593K'):
            self.assertEqual((df.code==ticker).sum(),1)
        self.assertEqual(flags['rows'],600)
        blob=k.encode_day(df)
        decoded=k.validate_stored_day(blob,DATES[0],600)
        self.assertEqual(set(decoded.code),set(df.code))
        self.assertEqual(k.canonical_ticker('123'), '000123')
        self.assertEqual(k.canonical_ticker('03473K'), '03473K')

    def test_alphanumeric_bad_shapes_still_fail_closed(self):
        for invalid in ('03473k','0347*K','KRX123','03473KK','0347K','034K30'):
            with self.subTest(code=invalid):
                with self.assertRaisesRegex(RuntimeError,'BAD_TICKER'):
                    k.canonical_ticker(invalid)
        src=fake_raw();idx=list(src.index);idx[0]='03473K';idx[1]='03473K';src.index=idx
        with self.assertRaisesRegex(RuntimeError,'DUPLICATE_TICKER'):
            k.normalize_day(src,DATES[0])

    def test_malformed_or_duplicate_records_stop(self):
        x=fake_raw(); x.index=['000001']*len(x)
        with self.assertRaisesRegex(RuntimeError,'DUPLICATE_TICKER'):k.normalize_day(x,DATES[0])
        x=fake_raw();x.loc['000001','거래대금']=-5
        with self.assertRaisesRegex(RuntimeError,'NEGATIVE_PRICE_OR_AMOUNT'):k.normalize_day(x,DATES[0])
    def test_full_collection_validation_and_tampering(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=pathlib.Path(tmp);r=root/'reference.csv';make_reference(r)
            provider=FakeKRX()
            k.fetch(provider,DATES[0],DATES[-1],r,root/'data',0,2,1,0)
            k.fetch(provider,DATES[0],DATES[-1],r,root/'data',1,2,1,0)
            self.assertEqual(sum(x[0]=='ALL' for x in provider.calls),4)
            result=k.verify(root/'data',r,root/'result.json')
            self.assertEqual(result['full_market_price_rows'],2400)
            self.assertFalse(result['whole_market_pit_strategy_ready'])
            bad= root/'data/shard_00/20200102.csv.gz';bad.write_bytes(b'CORRUPTED')
            with self.assertRaisesRegex(RuntimeError,'BAD_SHA'):k.verify(root/'data',r,root/'bad.json')
    def test_reference_count_mismatch_stops(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=pathlib.Path(tmp);r=root/'reference.csv';make_reference(r)
            with open(r,'r',encoding='utf-8') as f:content=f.read()
            r.write_text(content.replace('2020-01-02,600','2020-01-02,601'),encoding='utf-8')
            with self.assertRaisesRegex(RuntimeError,'H7_ANCHOR_COUNT_MISMATCH'):
                k.fetch(FakeKRX(),DATES[0],DATES[-1],r,root/'data',0,2,1,0)
    def test_missing_shard_blocks(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=pathlib.Path(tmp);r=root/'reference.csv';make_reference(r)
            k.fetch(FakeKRX(),DATES[0],DATES[-1],r,root/'data',0,2,1,0)
            with self.assertRaisesRegex(RuntimeError,'SHARDS_MISSING'):
                k.verify(root/'data',r,root/'result.json')


if __name__=='__main__':unittest.main()
