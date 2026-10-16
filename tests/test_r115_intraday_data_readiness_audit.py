import csv
import io
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from closebet_research import r115_intraday_data_readiness_audit as a

class IntradayTests(unittest.TestCase):
    def row(self, start='15:00', end='15:01',kind='REGULAR_INTERVAL',venue='KRX',session='KRX_REGULAR', v='100',turnover='1000000'):
        return dict(trade_date='2026-10-15',code='005930',bar_start_kst='2026-10-15 '+start,bar_end_kst='2026-10-15 '+end,
            volume_delta=v,turnover_krw=turnover,venue=venue,session=session,record_kind=kind,
            provider='RESEARCH_TEST_FIXTURE',provider_evidence_id='SYNTHETIC_TEST_ONLY',quality_flag='BROKER_REPORTED')
    def read(self,rows):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'minutes.csv';p.write_bytes(a.csv_bytes(rows,a.HEADER));return a.ingest_minutes(p)[0]
    def test_no_source_yields_unknown_not_zero(self):
        self.assertEqual(a.ingest_minutes(Path('/this/path/does/not/exist'))[0],[])
    def test_reject_cumulative_alias_columns(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'x.csv';p.write_text('date,code,accumulated_volume\n2026-10-15,005930,123\n')
            with self.assertRaisesRegex(ValueError,'MINUTE_SCHEMA_INVALID'):a.ingest_minutes(p)
    def test_reject_1530_as_regular(self):
        with self.assertRaisesRegex(ValueError,'REGULAR_SESSION_MISMATCH'):self.read([self.row('15:20','15:30')])
    def test_auction_explicit_and_asof_exclusion(self):
        bars=self.read([self.row('15:20','15:30','AUCTION_ACTUAL_1530','KRX','KRX_CLOSING_AUCTION')])
        s=a.summarize_minutes(bars);t=next(x for x in s if x['window']=='AUCTION_1530')
        self.assertEqual(t['coverage_pct'],100);self.assertEqual(t['available_after_kst'],'15:30')
        self.assertEqual(t['eligible_at_1503'],'False')
    def test_reject_auction_wrong_session(self):
        with self.assertRaisesRegex(ValueError,'AUCTION_TIME_INVALID'):self.read([self.row('15:20','15:30','AUCTION_ACTUAL_1530','KRX','KRX_REGULAR')])
    def test_reject_overlap(self):
        with self.assertRaisesRegex(ValueError,'OVERLAPPING_MINUTE_INTERVAL'):self.read([self.row('15:00','15:02'),self.row('15:01','15:03')])
    def test_reject_duplicate(self):
        with self.assertRaisesRegex(ValueError,'DUPLICATE_MINUTE_INTERVAL'):self.read([self.row(),self.row()])
    def test_unverified_cannot_be_research_eligible(self):
        r=self.row('15:00','15:03');r['quality_flag']='RESEARCH_UNVERIFIED'
        s=a.summarize_minutes(self.read([r]));p=next(x for x in s if x['window']=='PRE_1500_1503')
        self.assertEqual(p['coverage_pct'],100);self.assertEqual(p['eligible_for_price_volume_research'],'False')
    def test_missing_turnover_is_not_proxy(self):
        s=a.summarize_minutes(self.read([self.row('15:00','15:03',turnover='')]))
        p=next(x for x in s if x['window']=='PRE_1500_1503')
        self.assertEqual(p['turnover_krw'],'');self.assertEqual(p['eligible_for_price_volume_research'],'False')
    def test_03_20_not_available_at_1503(self):
        s=a.summarize_minutes(self.read([self.row('15:03','15:20')]))
        p=next(x for x in s if x['window']=='PRE_1503_1520')
        self.assertEqual(p['eligible_at_1503'],'False')
    def test_synthetic_minute_is_never_accepted_as_real_source(self):
        with self.assertRaisesRegex(ValueError,'PROVENANCE_REQUIRED'):
            r=self.row();r['provider_evidence_id']='';self.read([r])
    def test_r114_manifest_corruption(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d);(p/'r114_integrity_manifest.json').write_text('{}')
            with self.assertRaisesRegex(ValueError,'R114_MANIFEST_PIN_MISMATCH'):a.verify_r114(p)
    def test_code_probe(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)
            for fn in ['ham_restart_research.py','presurge_scanner.py','quick_open_checker.py','odoli_after_session_context_r1.py']:(p/fn).write_text('placeholder')
            r=a.audit_code(p);self.assertEqual(len(r),7);self.assertTrue(all(x['found']=='False' for x in r))
    def test_after_nxt_not_mixed_regular(self):
        bs=self.read([self.row('16:00','16:01','AFTER_SESSION_INTERVAL','NXT','NXT_AFTER')])
        s=a.summarize_minutes(bs)
        self.assertTrue(all(x['volume_delta']=='' for x in s if x['venue']=='KRX'))
        self.assertEqual(next(x for x in s if x['window']=='NXT_AFTER_1540_2000')['volume_delta'],100)
    def test_krx_after_1600_prelaunch_invalid(self):
        r=self.row('16:00','16:01','AFTER_SESSION_INTERVAL','KRX','KRX_AFTER')
        r['trade_date']='2026-08-20';r['bar_start_kst']='2026-08-20 16:00';r['bar_end_kst']='2026-08-20 16:01'
        with self.assertRaisesRegex(ValueError,'KRX_AFTER_1600_PRELAUNCH_INVALID'):self.read([r])
    def test_nxt_1540_auction_is_not_krx_1530(self):
        r=self.row('15:30','15:40','NXT_AUCTION_ACTUAL_1540','NXT','NXT_CLOSING_AUCTION')
        s=a.summarize_minutes(self.read([r]));t=next(x for x in s if x['window']=='NXT_AUCTION_1540')
        self.assertEqual(t['coverage_pct'],100)
        self.assertEqual(next(x for x in s if x['window']=='AUCTION_1530')['volume_delta'],'')
        self.assertEqual(t['eligible_at_1503'],'False')
    def test_nonzero_overnight_rejected(self):
        with self.assertRaisesRegex(ValueError,'CROSS_DATE'):
            r=self.row();r['bar_end_kst']='2026-10-16 15:01';self.read([r])
if __name__=='__main__':unittest.main()
