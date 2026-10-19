"""Offline synthetic-only tests: NEVER interpret as KRX or future OOS evidence."""
from datetime import datetime, timezone
from pathlib import Path
import json
import tempfile
import unittest
from unittest import mock
import pandas as pd
from closebet_research import r125_prospective_capture as m

GOOD_UTC=datetime(2026,10,19,8,15,tzinfo=timezone.utc)  # 17:15 KST

class FakeKRX:
    def __init__(self,mismatch=False,missing_amount=False,duplicate=False):
        self.calls=[];self.mismatch=mismatch;self.missing_amount=missing_amount;self.duplicate=duplicate
    def get_index_ohlcv_by_date(self,a,b,index):
        self.calls.append(('official',a,b,index))
        return pd.DataFrame({'종가':[2600]},index=[pd.Timestamp('2026-10-19')])
    def get_market_ticker_list(self,d,market='ALL'):
        self.calls.append(('member',d,market))
        return {'KOSPI':['000001','000002'],'KOSDAQ':['000003'],'KONEX':['000004']}[market]
    def get_market_ohlcv_by_ticker(self,d,market='ALL',alternative=False):
        self.calls.append(('quotes',d,market,alternative))
        columns={'시가':[100,99,150,90], '고가':[110,103,160,95],
                 '저가':[95,98,148,89], '종가':[106,100,152,92],
                 '거래량':[100,200,300,400], '거래대금':[10000,20000,30000,40000]}
        if self.missing_amount:columns.pop('거래대금')
        x=pd.DataFrame(columns,index=['000001','000002','000003','000004'])
        if self.mismatch:x=x.drop('000004')
        if self.duplicate:x.index=['000001','000001','000003','000004']
        return x

class R125Tests(unittest.TestCase):
    def test_after_2026_freeze_only(self):
        with self.assertRaisesRegex(ValueError,'DATE_NOT_AFTER_FROZEN_DISCOVERY'):
            m.validate_capture_time('2026-10-16',datetime(2026,10,16,8,30,tzinfo=timezone.utc))
    def test_wrong_day_fails_before_network(self):
        f=FakeKRX()
        with tempfile.TemporaryDirectory() as td:
            with self.assertRaisesRegex(ValueError,'NOT_CAPTURED_ON_TARGET_KST_DATE'):
                m.create_snapshot(f,'2026-10-20',Path(td)/'out',now=GOOD_UTC)
        self.assertEqual(f.calls,[])
    def test_pre_close_fails_before_network(self):
        f=FakeKRX()
        with tempfile.TemporaryDirectory() as td:
            with self.assertRaisesRegex(ValueError,'KRX_EOD_NOT_YET_FROZEN'):
                m.create_snapshot(f,'2026-10-19',Path(td)/'out',now=datetime(2026,10,19,6,40,tzinfo=timezone.utc))
        self.assertEqual(f.calls,[])
    def test_no_naive_clock(self):
        with self.assertRaisesRegex(ValueError,'NAIVE_CAPTURE_CLOCK_FORBIDDEN'):
            m.validate_capture_time('2026-10-19',datetime(2026,10,19,17))
    def test_invalid_ticker_fails(self):
        with self.assertRaisesRegex(ValueError,'INVALID_SIX_DIGIT_TICKER'):
            m.code_str('5930')
    def test_real_amount_mandatory(self):
        f=FakeKRX(missing_amount=True)
        with tempfile.TemporaryDirectory() as td:
            with self.assertRaisesRegex(ValueError,'KRX_ACTUAL_AMOUNT_OR_OHLCV_MISSING'):
                m.create_snapshot(f,'2026-10-19',Path(td)/'out',now=GOOD_UTC)
    def test_duplicate_ticker_rejected(self):
        f=FakeKRX(duplicate=True)
        with tempfile.TemporaryDirectory() as td:
            with self.assertRaisesRegex(ValueError,'KRX_DUPLICATE_CODE'):
                m.create_snapshot(f,'2026-10-19',Path(td)/'out',now=GOOD_UTC)
    def test_suspended_close_only_retained_in_census(self):
        raw=FakeKRX().get_market_ohlcv_by_ticker('20261019')
        raw.loc['000001',['시가','고가','저가','종가','거래량','거래대금']]=[0,0,0,106,0,0]
        n=m.normalize_market_frame(raw,'2026-10-19')
        self.assertTrue(bool(n.loc[n.code.eq('000001'),'invalid_or_suspended_bar'].iloc[0]))
        self.assertEqual(len(n),4)
    def test_suspended_close_only_must_have_zero_amount_and_volume(self):
        raw=FakeKRX().get_market_ohlcv_by_ticker('20261019')
        raw.loc['000001',['시가','고가','저가','종가','거래량','거래대금']]=[0,0,0,106,1,5]
        with self.assertRaisesRegex(ValueError,'KRX_PARTIAL_ZERO_BAR_INVALID'):
            m.normalize_market_frame(raw,'2026-10-19')
    def test_active_zero_volume_with_nonzero_amount_invalid(self):
        raw=FakeKRX().get_market_ohlcv_by_ticker('20261019')
        raw.loc['000001',['거래량','거래대금']]=[0,100]
        with self.assertRaisesRegex(ValueError,'KRX_VOLUME_AMOUNT_CONFLICT'):
            m.normalize_market_frame(raw,'2026-10-19')
    def test_bad_ohlc_rejected(self):
        f=FakeKRX()
        data=f.get_market_ohlcv_by_ticker('20261019');data.loc['000001','고가']=99
        with self.assertRaisesRegex(ValueError,'KRX_BAR_RANGE_INVALID'):
            m.normalize_market_frame(data,'2026-10-19')
    def test_inf_quote_rejected(self):
        f=FakeKRX()
        x=f.get_market_ohlcv_by_ticker('20261019')
        x['거래대금']=x['거래대금'].astype(float)
        x.loc['000001','거래대금']=float('inf')
        with self.assertRaisesRegex(ValueError,'KRX_NAN_INF_OR_BAD_NUMERIC'):
            m.normalize_market_frame(x,'2026-10-19')
    def test_valid_600_code_scope_parity(self):
        class FullMock(FakeKRX):
            def get_market_ticker_list(self,d,market='ALL'):
                slices={'KOSPI':range(0,260),'KOSDAQ':range(260,520),'KONEX':range(520,600)}
                return [f'{i:06d}' for i in slices[market]]
            def get_market_ohlcv_by_ticker(self,d,market='ALL',alternative=False):
                return pd.DataFrame({
                    '시가':[100]*600,'고가':[110]*600,'저가':[95]*600,
                    '종가':[105]*600,'거래량':[1000]*600,
                    '거래대금':[106000]*600,
                },index=[f'{i:06d}' for i in range(600)])
        with tempfile.TemporaryDirectory() as td:
            p=Path(td)/'snap'
            v=m.create_snapshot(FullMock(),'2026-10-19',p,now=GOOD_UTC)
            self.assertTrue(v['coverage']['eod_snapshot_scope_pass'])
            self.assertEqual(v['status'],'CAPTURED_MEMBERSHIP_PARITY_CHECKED_HISTORY_PENDING')
            self.assertTrue(m.verify_snapshot(p)['coverage']['exact_market_membership_match'])
            self.assertFalse(v['oos_complete'])
    def test_append_only(self):
        f=FakeKRX()
        with tempfile.TemporaryDirectory() as td:
            p=Path(td)/'snap'
            meta=m.create_snapshot(f,'2026-10-19',p,now=GOOD_UTC)
            self.assertEqual(meta['status'],'BLOCKED_MEMBERSHIP_INCOMPLETE_NOT_OOS') # synthetic has only 4 names
            with self.assertRaisesRegex(ValueError,'APPEND_ONLY_DESTINATION_ALREADY_EXISTS'):
                m.create_snapshot(f,'2026-10-19',p,now=GOOD_UTC)
    def test_parity_mismatch_fails_closed_not_oos(self):
        with tempfile.TemporaryDirectory() as td:
            meta=m.create_snapshot(FakeKRX(mismatch=True),'2026-10-19',Path(td)/'a',now=GOOD_UTC)
            self.assertFalse(meta['coverage']['exact_market_membership_match'])
            self.assertEqual(meta['coverage']['missing_member_quotes_count'],1)
            self.assertFalse(meta['coverage']['eod_snapshot_scope_pass'])
            self.assertFalse(meta['oos_complete'])
    def test_snapshot_bytes_tampered_detection(self):
        with tempfile.TemporaryDirectory() as td:
            p=Path(td)/'snap'
            m.create_snapshot(FakeKRX(),'2026-10-19',p,now=GOOD_UTC)
            self.assertEqual(m.verify_snapshot(p)['revision'],m.REVISION)
            f=p/'r1250_listed_membership.csv';f.write_bytes(f.read_bytes()+b'X')
            with self.assertRaisesRegex(ValueError,'SNAPSHOT_MEMBER_SHA_MISMATCH'):
                m.verify_snapshot(p)
    def test_weak_reconstruction_not_oos(self):
        r=m.readiness({},None)
        self.assertFalse(r['independent_forward_OOS'])
        self.assertFalse(r['full_market_pit_history_224_sessions_complete'])
        self.assertFalse(r['separate_wave1_pullback_detector_executed'])
    def test_empty_official_session(self):
        f=FakeKRX();f.get_index_ohlcv_by_date=lambda *a:pd.DataFrame()
        with tempfile.TemporaryDirectory() as td:
            with self.assertRaisesRegex(ValueError,'KRX_OFFICIAL_DAY_NOT_FOUND'):
                m.create_snapshot(f,'2026-10-19',Path(td)/'snap',now=GOOD_UTC)
    def test_holiday_fallback_explicitly_forbidden(self):
        with tempfile.TemporaryDirectory() as td:
            f=FakeKRX();m.create_snapshot(f,'2026-10-19',Path(td)/'snap',now=GOOD_UTC)
            self.assertIn(('quotes','20261019','ALL',False),f.calls)
    def test_baseline_blob_change_fail_closed(self):
        with tempfile.TemporaryDirectory() as td:
            d=Path(td);(d/'foo').write_bytes(b'x')
            with mock.patch.dict(m.PINS,{'foo':'0'*40},clear=True),mock.patch.dict(m.PROTECTED,{},clear=True):
                with self.assertRaisesRegex(ValueError,'BASELINE_GIT_BLOB_DRIFT'):
                    m.check_baselines(d)
    def test_git_blob_hash_known(self):
        self.assertEqual(m.gitblob_bytes(b'hello\n'),'ce013625030ba8dba906f756967f9e9ca394464a')
    def test_capture_does_not_use_future_metrics(self):
        with tempfile.TemporaryDirectory() as td:
            p=Path(td)/'s';m.create_snapshot(FakeKRX(),'2026-10-19',p,now=GOOD_UTC)
            import gzip
            frame=pd.read_csv(pd.io.common.BytesIO(gzip.decompress((p/'r1250_full_market_ohlcva.csv.gz').read_bytes())))
            self.assertEqual(frame.columns.tolist(),m.COLUMNS)
            self.assertFalse(any('future' in col or 'tp5' in col for col in frame.columns))
    def test_optional_root_workflow_copy(self):
        root=Path(__file__).resolve().parents[1]
        work=root/'.github/workflows/closing_bet_r1250_prospective_capture.yml'
        user_visible=root/'closing_bet_r1250_prospective_capture.yml'
        self.assertTrue(work.is_file())
        if user_visible.exists():self.assertEqual(work.read_bytes(),user_visible.read_bytes())
    def test_manual_only_workflow_and_no_live(self):
        root=Path(__file__).resolve().parents[1]
        y=(root/'.github/workflows/closing_bet_r1250_prospective_capture.yml').read_text()
        self.assertIn('workflow_dispatch:',y)
        self.assertNotIn('  schedule:',y)
        self.assertNotIn('secrets.',y)
        self.assertNotIn('TELEGRAM',y)
        self.assertNotIn('git push',y)
    def test_lock_full_market_eligibility(self):
        self.assertGreaterEqual(m.FIRST_ELIGIBLE_DATE,'2026-10-19')
        self.assertEqual(m.FREEZE_DATE,'2026-10-16')

if __name__=='__main__':unittest.main()
