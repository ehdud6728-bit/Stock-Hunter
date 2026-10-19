import csv
from datetime import date,datetime,timezone,timedelta
import gzip
import io
import json
import tempfile
import unittest
from pathlib import Path
from closebet_research import r1251_capture_integrity_audit as m

ROOT=Path(__file__).resolve().parents[1]
RUN='38977777777'
GIT_SHA='f'*40

def encode(rows,fields):
    t=io.StringIO(newline='')
    w=csv.DictWriter(t,fieldnames=fields,lineterminator='\n')
    w.writeheader();w.writerows(rows)
    return t.getvalue().encode('utf-8')

def sample_files(root):
    root=Path(root);root.mkdir(parents=True,exist_ok=True)
    b=[];mem=[]
    for i in range(600):
        code=f'{i+100000:06}'
        market='KOSPI' if i<250 else 'KOSDAQ' if i<550 else 'KONEX'
        mem.append({'code':code,'market':market})
        b.append({'code':code,'date':'2026-10-19','market':market,'open':'100','high':'105',
            'low':'99','close':'103','volume':'5000','amount':'507000'})
    meta={
        'revision':m.EXPECTED_PRODUCER_REV,'status':'CAPTURED_MEMBERSHIP_PARITY_CHECKED_HISTORY_PENDING',
        'github_run_id':RUN,'github_checkout_sha':GIT_SHA,'research_only':True,
        'observed_at_utc':'2026-10-19T07:40:02+00:00','observed_at_kst':'2026-10-19T16:40:02+09:00',
        'market_date':'2026-10-19','timestamp_provenance':'RUNNER_UTC_CLOCK_NOT_INDEPENDENT_TRUSTED_TIMESTAMP',
        'source':'PYKRX_KRX_DAILY_CROSS_SECTION_ALL_UNVERIFIED_OFFICIAL_PARITY','provider_version':'1.2.9',
        'coverage':{'member_count':600,'quote_count':600,'missing_member_quotes_count':0,
            'extra_unlisted_quotes_count':0,'exact_market_membership_match':True,
            'member_market_counts':{'KOSPI':250,'KOSDAQ':300,'KONEX':50},
            'invalid_or_suspended_bars':0,'eod_snapshot_scope_pass':True}
    }
    for v in ('trade_approved','oos_complete','D20_complete','prospective_full_market_pit_complete',
        'historical_warmup_full_market_pit','official_source_independent_audit_complete',
        'historical_posthoc_universe_reused','LOW224_R1C1_and_R124_gates_replayed'):
        meta[v]=False
    for v in ('no_live_changed','no_shadow_changed','no_telegram_changed','no_order_changed'):
        meta[v]=True
    save(root,b,mem,meta)
    return b,mem,meta

def save(root,b,mem,meta):
    raw=gzip.compress(encode(b,m.SCHEMA),mtime=0)
    mr=encode(mem,m.MEMBER_SCHEMA)
    (root/'r1250_full_market_ohlcva.csv.gz').write_bytes(raw)
    (root/'r1250_listed_membership.csv').write_bytes(mr)
    meta['files_sha256']={'r1250_full_market_ohlcva.csv.gz':m.digest(raw),'r1250_listed_membership.csv':m.digest(mr)}
    (root/'r1250_capture_manifest.json').write_text(json.dumps(meta))

class R1251Tests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path=Path(self.temp.name)
        self.b,self.mem,self.meta=sample_files(self.path)
    def audit(self):return m.audit_capture(self.path,RUN,producer_git_sha=GIT_SHA)
    def mutated(self):save(self.path,self.b,self.mem,self.meta)
    def test_01_valid_future_realistic_source(self):
        r=self.audit();self.assertEqual(r['status'],'PASS_CAPTURE_SOURCE_INTEGRITY_NOT_OOS_NOT_TRADE_APPROVED')
        self.assertEqual(r['quote_tickers'],600);self.assertFalse(r['oos_complete'])
    def test_02_immutable_producer_hash_valid(self):
        b=b'abcd';self.assertEqual(m.git_blob(b),'85df50785d62d3b05ab03d9cbf7e4a0b49449730')
    def test_03_missing_snapshot_fails(self):
        (self.path/'r1250_capture_manifest.json').unlink()
        with self.assertRaisesRegex(ValueError,'NO_CAPTURE_MANIFEST'):self.audit()
    def test_04_tampered_compressed_source(self):
        with (self.path/'r1250_full_market_ohlcva.csv.gz').open('ab') as f:f.write(b'junk')
        with self.assertRaisesRegex(ValueError,'SNAPSHOT_SHA256_BAD'):self.audit()
    def test_05_missing_member(self):
        self.mem.pop();self.meta['coverage']['member_count']-=1
        self.meta['coverage']['missing_member_quotes_count']=0
        self.meta['coverage']['extra_unlisted_quotes_count']=1
        self.meta['coverage']['exact_market_membership_match']=False
        self.meta['coverage']['eod_snapshot_scope_pass']=False
        self.meta['status']='BLOCKED_MEMBERSHIP_INCOMPLETE_NOT_OOS'
        self.meta['coverage']['member_market_counts']['KONEX']-=1
        self.mutated()
        self.assertEqual(self.audit()['status'],'BLOCKED_INCOMPLETE_CENSUS_NOT_OOS')
    def test_06_wrong_market_assignment(self):
        self.b[0]['market']='KOSDAQ';self.mutated()
        self.assertEqual(self.audit()['status'],'BLOCKED_INCOMPLETE_CENSUS_NOT_OOS')
    def test_07_wrong_day(self):
        self.b[3]['date']='2026-10-20';self.mutated()
        with self.assertRaisesRegex(ValueError,'WRONG_DAY'):self.audit()
    def test_08_duplicate_quote(self):
        self.b[1]['code']=self.b[0]['code'];self.mutated()
        with self.assertRaisesRegex(ValueError,'DUPLICATE_CENSUS'):self.audit()
    def test_09_six_digit_ticker_enforced(self):
        self.b[0]['code']='12345';self.mutated()
        with self.assertRaisesRegex(ValueError,'TICKER_FORMAT'):self.audit()
    def test_10_volume_amount_proxy_not_allowed(self):
        self.b[0]['amount']='0';self.mutated()
        with self.assertRaisesRegex(ValueError,'PRICE_VOLUME_AMOUNT_CONFLICT'):self.audit()
    def test_11_non_integral_amount_rejected(self):
        self.b[0]['amount']='5.10';self.mutated()
        with self.assertRaisesRegex(ValueError,'INVALID_INTEGER_AMOUNT'):self.audit()
    def test_12_ohlc_contradiction(self):
        self.b[0]['high']='101';self.mutated()
        with self.assertRaisesRegex(ValueError,'INVALID_OHLC_BAR'):self.audit()
    def test_13_future_leak_status(self):
        self.meta['D20_complete']=True;self.mutated()
        with self.assertRaisesRegex(ValueError,'UNWARRANTED_AUTHORITY'):self.audit()
    def test_14_correct_run_id(self):
        with self.assertRaisesRegex(ValueError,'PRODUCER_RUN_ID_MISMATCH'):m.audit_capture(self.path,'128')
    def test_15_checkout_sha_guard(self):
        with self.assertRaisesRegex(ValueError,'PRODUCER_CHECKOUT_SHA_MISMATCH'):m.audit_capture(self.path,RUN,'xyz')
    def test_16_kst_time_mismatch(self):
        self.meta['observed_at_kst']='2026-10-19T16:40:03+09:00';self.mutated()
        with self.assertRaisesRegex(ValueError,'CAPTURE_CLOCK_CONFLICT'):self.audit()
    def test_17_after_16_only(self):
        self.meta['observed_at_utc']='2026-10-19T06:40:02+00:00'
        self.meta['observed_at_kst']='2026-10-19T15:40:02+09:00';self.mutated()
        with self.assertRaisesRegex(ValueError,'NON_CONTEMPORANEOUS'):self.audit()
    def test_18_asof_future_date_only(self):
        self.meta['market_date']='2026-10-16';self.mutated()
        with self.assertRaisesRegex(ValueError,'PRE_FREEZE'):self.audit()
    def test_19_preflight_artifact_has_no_snapshot(self):
        p=self.path/'preflight';p.mkdir();r=m.report(p,RUN,self.path/'report')
        self.assertEqual(r['status'],'FAIL_CLOSED_NOT_OOS')
        self.assertTrue((self.path/'report'/m.OUTPUT).is_file())
    def test_20_missing_market_blocks(self):
        for row in self.mem[-50:]:row['market']='KOSDAQ'
        for row in self.b[-50:]:row['market']='KOSDAQ'
        self.meta['coverage']['member_market_counts']={'KOSPI':250,'KOSDAQ':350,'KONEX':0}
        self.mutated()
        self.assertEqual(self.audit()['status'],'BLOCKED_INCOMPLETE_CENSUS_NOT_OOS')
    def test_21_broken_status_fails(self):
        self.meta['status']='PASS_INDEPENDENT_OOS';self.mutated()
        with self.assertRaisesRegex(ValueError,'CAPTURE_STATUS_INVALID'):self.audit()
    def test_22_suspended_zero_allowed(self):
        self.b[0].update(open='0',high='0',low='0',close='102',volume='0',amount='0')
        self.meta['coverage']['invalid_or_suspended_bars']=1;self.mutated()
        self.assertEqual(self.audit()['zero_or_suspended_bars'],1)
    def test_23_suspended_zero_trade_fails(self):
        self.b[0].update(open='0',high='0',low='0',close='102',volume='1',amount='3')
        self.meta['coverage']['invalid_or_suspended_bars']=1;self.mutated()
        with self.assertRaisesRegex(ValueError,'ZERO_BAR_HAS_TRADING'):self.audit()
    def test_24_status_fail_on_mismatched_flag(self):
        self.meta['coverage']['eod_snapshot_scope_pass']=False;self.mutated()
        with self.assertRaisesRegex(ValueError,'MANIFEST_SCOPE_FLAG_CONFLICT'):self.audit()
    def test_25_protected_workflow_manual_only(self):
        f=ROOT/'.github/workflows/closing_bet_r1251_capture_integrity_audit.yml'
        text=f.read_text()
        self.assertIn('workflow_dispatch:',text)
        self.assertIn('contents: read',text)
        self.assertIn('actions: read',text)
        self.assertNotIn('  schedule:',text)
        self.assertNotIn('secrets.',text)
    def test_26_optional_root_copy(self):
        a=ROOT/'.github/workflows/closing_bet_r1251_capture_integrity_audit.yml'
        b=ROOT/'closing_bet_r1251_capture_integrity_audit.yml'
        self.assertTrue(a.is_file())
        if b.is_file():self.assertEqual(a.read_bytes(),b.read_bytes())
    def test_27_d0_no_signal_claim(self):
        r=self.audit()
        self.assertFalse(r['independent_krx_certification'])
        self.assertFalse(r['full_market_pit_224_sessions'])
        self.assertFalse(r['permanent_archive'])
    def test_28_source_schema_fixed(self):
        self.assertEqual(m.SCHEMA,['code','date','market','open','high','low','close','volume','amount'])

if __name__=='__main__': unittest.main()
