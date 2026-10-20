"""StockHunter Stage6A provider-free contracts, no real KRX/network/order functions."""
import csv
import gzip
import io
import json
import pathlib
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT=pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'research_pit'))
import pit_stage6_market_cap as m
import pit_stage5_daily_roster as p


class Index(list):
    def tolist(self):return list(self)


class Frame:
    def __init__(self, rows):
        self.data=rows;self.index=Index(rows);self.columns=m.CAP_COLUMNS
        self.loc=self
    def __getitem__(self,code):return self.data[code]
    def __len__(self):return len(self.index)


class Client:
    def __init__(self,frame,fail=0):self.frame=frame;self.fail=fail;self.calls=[]
    def get_market_cap_by_ticker(self,date,market='ALL',alternative=False):
        self.calls.append((date,market,alternative))
        if self.fail:
            self.fail-=1;raise ConnectionError('temporary provider failure')
        return self.frame


def codes():return ['000%03d'%i for i in range(1,46)]
def markets():
    allcodes=codes()
    return {'KOSPI':allcodes[:15],'KOSDAQ':allcodes[15:30],'KONEX':allcodes[30:]}
def frame():
    return Frame({c:dict(zip(m.CAP_COLUMNS,[100,10000000,0,0,100000])) for c in codes()})


def setup_8_day(tmp, corrupt_price=False, conflict=False):
    home=pathlib.Path(tmp)
    dates=['2020-01-%02d'%i for i in range(2,10)]
    s5=home/'stage5'; s6=home/'stage6'; px=home/'source'
    for shard,day in enumerate(dates):
        a=s5/('shard_%02d'%shard); b=s6/('shard_%02d'%shard);src=px/('shard_%02d'%shard)
        market=p.normalize_roster(day,markets());rdata=gzip.compress(p.jbytes(market),mtime=0)
        p.write(a/'days'/((day.replace('-',''))+'.json.gz'),rdata)
        entry={'date':day,'file':'days/'+day.replace('-','')+'.json.gz','sha256':p.digest(rdata),
               'market_rows':45,'expected_price_rows':45,'count_mismatch':False}
        m5=dict(schema=p.SCHEMA,status='COMPLETE_DATED_MARKET_ROSTER_EVIDENCE_ONLY',
                canary=False,shard_index=shard,shard_count=8,source_price_run=p.P_SOURCE_RUN,
                source_stage2_run=p.P_STAGE2_RUN,files=[entry])
        p.wjson(a/'manifest.json',m5)
        vals=m.normalize_cap(day,frame(),set(codes()))
        if conflict and shard==4:vals[0]['amount_krw']=12345
        cen=m.save_cap_day(day,vals,b)
        m6=dict(schema=m.SCHEMA,status='COMPLETE_DATED_CAP_SOURCE_ONLY',shard=shard,
            shards=8,source_stage5_run=m.STAGE5_RUN,source_stage5_sha=m.STAGE5_SHA,
            source_stage1_run=p.P_SOURCE_RUN,stage5_manifest_sha256=m.digest(m.jsonbytes(m5)),
            files=[cen],strategy_ready=False)
        m.dump(b/'manifest.json',m6)
        pr=[]
        for c in codes():pr.append({'code':c,'date':day,'open':100,'high':100,'low':100,'close':100,
                                    'volume':0,'amount':0})
        pdata=gzip.compress(m.csvbytes(pr,['code','date','open','high','low','close','volume','amount']),mtime=0)
        p.write(src/'days'/(day.replace('-','')+'.csv.gz'),pdata)
        f={'date':day,'file':'days/'+day.replace('-','')+'.csv.gz',
           'sha256':p.digest(pdata),'stored_rows':45}
        if corrupt_price and shard==2:f['sha256']='deadbeef'
        p.wjson(src/('manifest_shard_%02d.json'%shard),dict(status='COMPLETE_PRICE_COLLECTION_ONLY',
           universe_filter='NONE',files=[f]))
    types={c:{'type_evidence_label':'UNRESOLVED_NO_2019_2026_EVENT'} for c in codes()}
    return s5,s6,px,dict(zip(dates,[45]*8)),types


class Tests(unittest.TestCase):
    def test_cap_code_sets_exact_and_keep_zero_volume_unresolved(self):
        rows=m.normalize_cap('2020-01-02',frame(),set(codes()))
        self.assertEqual(len(rows),45)
        self.assertEqual(rows[0]['market_cap_krw'],10000000)
        self.assertEqual(rows[0]['volume'],0)
        self.assertNotIn('halt',rows[0])

    def test_missing_cap_codes_fail_even_if_not_tradeable(self):
        f=frame();f.data.pop(codes()[0]);f.index=Index(f.data)
        with self.assertRaisesRegex(RuntimeError,'CAP_CODE_ROSTER_MISMATCH'):
            m.normalize_cap('2020-01-02',f,set(codes()))

    def test_caps_cannot_be_coerced_from_floats_missing_or_negative(self):
        for value in ['-1','NaN','1.2','']:
            with self.subTest(value=value):
                f=frame();f.data[codes()[0]]['시가총액']=value
                with self.assertRaisesRegex(RuntimeError,'NON_INTEGER'):
                    m.normalize_cap('2020-01-02',f,set(codes()))

    def test_cap_positive_requires_shares_and_positive_amount_requires_volume(self):
        a=frame();a.data[codes()[0]]['상장주식수']=0
        with self.assertRaisesRegex(RuntimeError,'POSITIVE_CAP_WITH_ZERO_SHARES'):
            m.normalize_cap('2020-01-02',a,set(codes()))
        b=frame();b.data[codes()[0]]['거래대금']=1
        with self.assertRaisesRegex(RuntimeError,'POSITIVE_AMOUNT_ZERO_VOLUME'):
            m.normalize_cap('2020-01-02',b,set(codes()))

    def test_fake_client_uses_explicit_asof_day_not_previous_date(self):
        client=Client(frame(),fail=2)
        with patch.object(m.time,'sleep',return_value=None):
            ans=m.provider_cap(client,'2019-01-02',pause=0.01)
        self.assertEqual(len(ans),45)
        self.assertEqual(client.calls,[('20190102','ALL',False)]*3)
        client=Client(frame(),fail=3)
        with patch.object(m.time,'sleep',return_value=None),self.assertRaisesRegex(RuntimeError,'PROVIDER_FAILED'):
            m.provider_cap(client,'2019-01-02',pause=0.01)

    def test_stored_cap_sha_and_headers_rejected_on_tamper(self):
        with tempfile.TemporaryDirectory() as d:
            day='2020-01-02';entry=m.save_cap_day(day,m.normalize_cap(day,frame(),set(codes())),d)
            self.assertEqual(len(m.read_verified_rows(d,entry,day)),45)
            target=pathlib.Path(d)/entry['file'];target.write_bytes(b'corrupted')
            with self.assertRaisesRegex(RuntimeError,'CAP_STORED_SHA_DRIFT'):
                m.read_verified_rows(d,entry,day)

    def test_stage5_market_roster_manifest_sha_protected(self):
        with tempfile.TemporaryDirectory() as d:
            a,_,_,_,_=setup_8_day(d)
            base,m5=m.validate_manifest(a,0,strict=False)
            ids=m.stage5_day(base,m5['files'][0]);self.assertEqual(len(ids),45)
            file=base/m5['files'][0]['file'];file.write_bytes(b'TAMPER')
            with self.assertRaisesRegex(RuntimeError,'TAMPERED_STAGE5_ROSTER'):
                m.stage5_day(base,m5['files'][0])

    def test_scan_writes_idempotent_sha_manifest_without_livetrading(self):
        with tempfile.TemporaryDirectory() as d:
            a,_,_,_,_=setup_8_day(d);cl=Client(frame())
            done=m.scan(a,pathlib.Path(d)/'out',cl,shard=0,pause=0,strict=False)
            self.assertEqual(done['days'],1)
            self.assertFalse(done['strategy_ready'])
            self.assertEqual(cl.calls,[('20200102','ALL',False)])
            self.assertEqual(len(m.read_verified_rows(pathlib.Path(d)/'out',done['files'][0],'2020-01-02')),45)

    def test_verify_all_shards_and_preserve_type_unknown(self):
        with tempfile.TemporaryDirectory() as d:
            s5,s6,px,dates,types=setup_8_day(d)
            with patch.object(p,'source_stage2',return_value=dates),patch.object(p,'source_stage4',return_value=types):
                report=m.verify(s6,s5,px,'unused','unused',pathlib.Path(d)/'out',strict=False)
            self.assertEqual(report['sessions'],8)
            self.assertEqual(report['cap_rows'],360)
            self.assertEqual(report['stage4_unresolved_type_codes'],45)
            self.assertFalse(report['strategy_ready'])
            self.assertFalse(report['historical_security_type_verified'])
            self.assertFalse(report['official_daily_suspensions_verified'])
            self.assertEqual(report['price_vs_cap_value_mismatch_dates'],0)

    def test_verify_corrupted_price_sha_fails_closed(self):
        with tempfile.TemporaryDirectory() as d:
            s5,s6,px,dates,types=setup_8_day(d,corrupt_price=True)
            with patch.object(p,'source_stage2',return_value=dates),patch.object(p,'source_stage4',return_value=types):
                with self.assertRaisesRegex(RuntimeError,'PRICE_DAY_SHA_DRIFT'):
                    m.verify(s6,s5,px,'unused','unused',pathlib.Path(d)/'out',strict=False)

    def test_verify_cross_source_value_disagreement_is_explicit_fail(self):
        with tempfile.TemporaryDirectory() as d:
            s5,s6,px,dates,types=setup_8_day(d,conflict=True)
            with patch.object(p,'source_stage2',return_value=dates),patch.object(p,'source_stage4',return_value=types):
                with self.assertRaisesRegex(RuntimeError,'CAP_VS_ORIGINAL_PRICE_VALUE_CONFLICTS'):
                    m.verify(s6,s5,px,'unused','unused',pathlib.Path(d)/'out',strict=False)
            x=m.load(pathlib.Path(d)/'out'/'stage6_historical_market_cap_audit_summary.json')
            self.assertEqual(x['amount_mismatch_rows'],1)
            self.assertFalse(x['strategy_ready'])

    def test_no_new_workflow_and_do_not_escalate_type_or_halts(self):
        code=(ROOT/'research_pit'/'pit_stage6_market_cap.py').read_text()
        workflow=(ROOT/'.github/workflows/stockhunter_pit_stage3_krx_events.yml').read_text()
        self.assertIn('default: stage6',workflow)
        self.assertIn('stockhunter-pit-stage6-cap-shard-',workflow)
        self.assertIn('strategy_ready=False',code)
        self.assertIn('official_daily_suspensions_verified=False',code)
        self.assertNotIn('place_order(',code)
        self.assertNotIn('LIVE_ENABLE=1',workflow)

if __name__=='__main__':unittest.main()
