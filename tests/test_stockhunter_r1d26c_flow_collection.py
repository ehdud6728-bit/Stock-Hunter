import io
import json
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch
import pandas as pd
from research_formulas import stockhunter_r1d26c_flow_collection as r

class R1D26CContracts(unittest.TestCase):
    def setUp(self):
        self.days=pd.bdate_range('2024-03-01',periods=25).date
        self.exp=[d.isoformat() for d in self.days[:20]]
        self.row=dict(date=self.days[20].isoformat(),code='000910',strategy=r.C,source_event_sha256='deadbeef')
        self.wins=[(dict(self.row,code=f"{910+i:06d}",source_event_sha256=f"synthetic-{i}"),self.exp) for i in range(20)]
    def frame(self):
        return pd.DataFrame({'기관합계':[10]*20,'외국인합계':[-3]*20},index=pd.to_datetime(self.exp))
    def test_exact_20_dates_signed_values(self):
        val=r.validate_provider(self.frame(),self.row,self.exp,'2026-10-26T00:00:00+00:00')
        self.assertEqual(len(val),20)
        self.assertEqual(val[0]['institution_net_krw'],10)
        self.assertEqual(val[0]['foreign_net_krw'],-3)
        self.assertEqual(val[0]['code'],'000910')
    def test_reject_missing_date(self):
        with self.assertRaisesRegex(r.Invalid,'MISSING_OR_EXTRA'):
            r.validate_provider(self.frame().iloc[:-1],self.row,self.exp,'t')
    def test_reject_future(self):
        f=self.frame();f.index=pd.to_datetime(self.exp[:-1]+[self.row['date']])
        with self.assertRaises(r.Invalid):r.validate_provider(f,self.row,self.exp,'t')
    def test_reject_duplicate(self):
        f=self.frame();f.index=[self.exp[0]]*20
        with self.assertRaisesRegex(r.Invalid,'DUPLICATE'):
            r.validate_provider(f,self.row,self.exp,'t')
    def test_reject_noninteger_and_nan(self):
        f=self.frame().astype(float);f.iloc[0,0]=1.5
        with self.assertRaises(r.Invalid):r.validate_provider(f,self.row,self.exp,'t')
        f=self.frame();f.iloc[0,0]=float('nan')
        with self.assertRaises(r.Invalid):r.validate_provider(f,self.row,self.exp,'t')
    def test_reject_all_zero(self):
        f=self.frame()*0
        with self.assertRaisesRegex(r.Invalid,'ALL_ZERO'):
            r.validate_provider(f,self.row,self.exp,'t')
    def test_reject_empty_and_bad_schema(self):
        with self.assertRaises(r.Invalid):r.validate_provider(pd.DataFrame(),self.row,self.exp,'t')
        f=self.frame().drop(columns=['기관합계'])
        with self.assertRaises(r.Invalid):r.validate_provider(f,self.row,self.exp,'t')
    def test_full20_collection_and_no_outcomes(self):
        with tempfile.TemporaryDirectory() as temp:
            with patch.object(r.time,'sleep',return_value=None):
                m=r.collect(self.wins,lambda a,b,c:self.frame(),temp,sleep_seconds=0)
            self.assertEqual(m['events_complete'],20)
            self.assertEqual(m['verified_complete_event_rows'],400)
            self.assertFalse(m['strategy_effect_estimated'])
            self.assertFalse(m['strategy_ready'])
            self.assertFalse(m['official_value_cross_source_verified'])
            a=pd.read_csv(Path(temp)/'R1D26C_VERIFIED_COMPLETE_EVENTS_INVESTOR_PRIOR20_KRW.csv',dtype={'code':str})
            self.assertEqual(len(a),400)
    def test_source_fail_immediate_stop_quarantine(self):
        with tempfile.TemporaryDirectory() as temp:
            m=r.collect(self.wins,lambda a,b,c:None,temp,sleep_seconds=0)
            self.assertEqual(m['events_attempted'],1)
            self.assertEqual(m['events_not_attempted'],19)
            self.assertEqual(m['verified_complete_event_rows'],0)
            f=pd.read_csv(Path(temp)/'R1D26C_VERIFIED_COMPLETE_EVENTS_INVESTOR_PRIOR20_KRW.csv')
            self.assertTrue(f.empty)
    def test_incomplete_ticker_does_not_abort_other_events(self):
        with tempfile.TemporaryDirectory() as temp:
            calls=[0]
            def one_incomplete(a,b,c):
                calls[0]+=1
                return self.frame().iloc[:-1] if calls[0]==1 else self.frame()
            m=r.collect(self.wins,one_incomplete,temp,sleep_seconds=0)
            self.assertEqual(m['events_attempted'],20)
            self.assertEqual(m['events_complete'],19)
            self.assertEqual(m['verified_complete_event_rows'],380)
    def test_pinned_archive_digest_rejects_wrong_file(self):
        with tempfile.TemporaryDirectory() as temp:
            b=Path(temp)/'bad.zip'
            b.write_bytes(b'not the pinned GitHub artifact')
            proto=json.loads(Path('R1D26C_PREDECLARED_PROTOCOL.json').read_text())
            with self.assertRaisesRegex(r.Invalid,'SOURCE_ARTIFACT_SHA'):
                r.load_inputs(b,proto)
    def test_source_provider_exception_no_message_leak(self):
        with tempfile.TemporaryDirectory() as temp:
            def bad(a,b,c):raise RuntimeError('my-secret-password-should-not-log')
            m=r.collect(self.wins,bad,temp,sleep_seconds=0)
            self.assertEqual(m['events_complete'],0)
            t=(Path(temp)/'R1D26C_SOURCE_EVENT_AUDIT.csv').read_text()
            self.assertNotIn('my-secret-password',t)
    def test_protocol_sha_and_source_data(self):
        proto=json.loads(Path('R1D26C_PREDECLARED_PROTOCOL.json').read_text())
        self.assertEqual(proto['frozen_pilot_events'],20)
        # The test source is an identical copy of the real GitHub R1D26 artifact.
        source=Path('tests/data/R1D26_PINNED_PILOT_SOURCE.zip')
        if source.exists():
            wins=r.load_inputs(source,proto)
            self.assertEqual(len(wins),20)
            self.assertEqual({x[0]['strategy'] for x in wins},{r.C,r.D})
            self.assertTrue(all(x[1][-1]<x[0]['date'] for x in wins))

if __name__=='__main__':unittest.main()
