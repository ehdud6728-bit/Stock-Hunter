"""Provider-free PIT Stage5 offline source and cross-endpoint contracts."""
import csv
import gzip
import io
import json
import pathlib
import sys
import tempfile
import unittest
from unittest.mock import patch
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]/'research_pit'))
import pit_stage5_daily_roster as p

BASE=pathlib.Path(__file__).resolve().parents[1]


def make_dates(n=8):return ['2020-01-%02d'%i for i in range(2,2+n)]
def roster_codes():return {m:['%06d'%x for x in range(start,start+15)] for m,start in [('KOSPI',100),('KOSDAQ',200),('KONEX',300)]}

def synthetic(tmp, number=8):
    base=pathlib.Path(tmp);dates=make_dates(number);M=roster_codes();codes=sorted(set().union(*(set(v) for v in M.values())))
    s2=base/'s2';s2.mkdir()
    p.wjson(s2/'lifecycle_summary.json',{'source_run_id':p.P_SOURCE_RUN,'source_head_sha':p.P_SOURCE_SHA,'observed_codes':len(codes),'price_rows':len(dates)*len(codes)})
    p.wjson(s2/'source_fullmarket_price_verification.json',{'full_market_price_rows':len(dates)*len(codes),'sessions':len(dates),'whole_market_pit_strategy_ready':False})
    p.wcsv(s2/'all_session_observation_counts.csv',['date','raw_price_rows'],[{'date':d,'raw_price_rows':len(codes)} for d in dates])
    p.wjson(s2/'dated_market_membership_snapshots.json',[{'date':d,'market_codes':M} for d in (dates[0],dates[-1])])
    s4=base/'s4';s4.mkdir();p.wjson(s4/'stage4_event_type_audit_summary.json',{'observed_codes':len(codes),'codes_without_usable_event_type':len(codes),'strategy_ready':False,'source_stage2_run':p.P_STAGE2_RUN,'source_stage3_run':39364301572})
    p.wcsv(s4/'all_3341_event_type_evidence.csv',['code','type_evidence_label'],[{'code':c,'type_evidence_label':'UNRESOLVED_NO_2019_2026_EVENT'} for c in codes])
    prices=base/'pitdata';rosters=base/'rosters'
    for shard,day in enumerate(dates):
        b=prices/f'shard_{shard:02d}';b.mkdir(parents=True)
        out=io.StringIO(newline='');wr=csv.writer(out)
        wr.writerow(['code','date','open','high','low','close','volume','amount'])
        for c in codes:wr.writerow([c,day,1,1,1,1,1,1])
        content=gzip.compress(out.getvalue().encode(),mtime=0)
        (b/(day.replace('-','')+'.csv.gz')).write_bytes(content)
        p.wjson(b/f'manifest_shard_{shard:02d}.json',{'shard':shard,'shards':8,'status':'COMPLETE_PRICE_COLLECTION_ONLY','universe_filter':'NONE','files':[{'date':day,'file':day.replace('-','')+'.csv.gz','sha256':p.digest(content),'stored_rows':45}]})
        bb=rosters/f'shard_{shard:02d}';bb.mkdir(parents=True)
        snap=p.normalize_roster(day,M)
        bbytes=gzip.compress(p.jbytes(snap),mtime=0)
        (bb/'days').mkdir()
        (bb/'days'/(day.replace('-','')+'.json.gz')).write_bytes(bbytes)
        p.wjson(bb/'manifest.json',{'schema':p.SCHEMA,'status':'COMPLETE_DATED_MARKET_ROSTER_EVIDENCE_ONLY','canary':False,'shard_index':shard,'shard_count':8,'source_price_run':p.P_SOURCE_RUN,'source_stage2_run':p.P_STAGE2_RUN,'files':[{'date':day,'file':'days/'+day.replace('-','')+'.json.gz','sha256':p.digest(bbytes),'market_rows':45,'expected_price_rows':45}]})
    return base,M,dates,codes


class FakeKRX:
    def __init__(self,dicts):self.dicts=dicts;self.calls=[]
    def get_market_ticker_list(self,date,market):
        self.calls.append((date,market));return self.dicts[market]

class Stage5Tests(unittest.TestCase):
    def test_metadata_cannot_claim_pit_security_type_or_strategy_ready(self):
        snap=p.normalize_roster('2020-01-02',roster_codes())
        self.assertFalse(snap['security_type_verified']);self.assertFalse(snap['strategy_ready'])
        src=(BASE/'research_pit/pit_stage5_daily_roster.py').read_text()
        self.assertNotIn('place_order(',src)
        self.assertNotIn('LIVE_ENABLE\', \'1\'',src)

    def test_historical_alphanumeric_code_preserved(self):
        sample=roster_codes();sample['KOSPI'][-1]='0099X0';sample['KOSDAQ'][4]='03473K'
        res=p.normalize_roster('2020-01-02',sample)
        self.assertIn('0099X0',res['market_codes']['KOSPI'])
        self.assertIn('03473K',res['market_codes']['KOSDAQ'])

    def test_duplicate_market_membership_rejected(self):
        rows=roster_codes();rows['KONEX'][0]=rows['KOSPI'][0]
        with self.assertRaisesRegex(RuntimeError,'MULTIPLE_MARKETS'):p.normalize_roster('2020-01-02',rows)

    def test_nonstock_or_synthetic_code_shape_rejected(self):
        rows=roster_codes();rows['KOSDAQ'][0]='KR7005930003'
        with self.assertRaisesRegex(RuntimeError,'BAD_KRX_MARKET_CODE'):p.normalize_roster('2020-01-02',rows)

    def test_live_fetch_retries_but_never_replaces_unavailable_with_zero(self):
        class Failed:
            def get_market_ticker_list(self,day,market):raise OSError('unavailable')
        with self.assertRaisesRegex(RuntimeError,'RETRY_EXHAUSTED'):p.query_one_day(Failed(),'2020-01-02',pause=0,retries=1)

    def test_canary_identical_to_pinned_stage2_snapshots(self):
        with tempfile.TemporaryDirectory() as tmp:
            root,M,days,_=synthetic(tmp)
            fake=FakeKRX(M)
            report=p.scan(root/'s2',root/'canary',fake,canary=True,pause=0,strict=False)
            self.assertEqual(report['sessions'],2)
            self.assertEqual(len(fake.calls),6)
            M['KOSPI'][0]='100000' # shape valid, nonidentical; replace, maintain count
            with self.assertRaisesRegex(RuntimeError,'CANARY_IDENTITY_MISMATCH'):
                p.scan(root/'s2',root/'bad',fake,canary=True,pause=0,strict=False)

    def test_original_price_shas_enforced(self):
        with tempfile.TemporaryDirectory() as tmp:
            root,_,days,_=synthetic(tmp)
            f=root/'pitdata/shard_00'/(days[0].replace('-','')+'.csv.gz')
            f.write_bytes(f.read_bytes()+b'corrupt')
            with self.assertRaisesRegex(RuntimeError,'BAD_PRICE_DAY_SHA'):
                p.verify_full(root/'rosters',root/'pitdata',root/'s2',root/'s4',root/'reports',strict=False)

    def test_full_daily_exact_memberships_match_price_codes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root,_,days,codes=synthetic(tmp)
            audit=p.verify_full(root/'rosters',root/'pitdata',root/'s2',root/'s4',root/'reports',strict=False)
            self.assertEqual(audit['sessions'],8);self.assertEqual(audit['price_rows'],8*45)
            self.assertEqual(audit['mismatch_days'],0)
            self.assertEqual(audit['unresolved_stage4_type_codes'],45)
            self.assertFalse(audit['historical_security_type_verified'])
            self.assertFalse(audit['pit_ordinary_share_master_ready'])
            with (root/'reports/unresolved_2023_dated_market_profiles.csv').open(newline='') as f:
                self.assertEqual(sum(1 for _ in csv.DictReader(f)),45)

    def test_price_vs_roster_code_mismatch_is_fail_closed_even_if_counts_equal(self):
        with tempfile.TemporaryDirectory() as tmp:
            root,M,days,codes=synthetic(tmp)
            snap_path=root/'rosters/shard_00/days'/(days[0].replace('-','')+'.json.gz')
            snap=json.loads(gzip.decompress(snap_path.read_bytes()))
            snap['market_codes']['KOSPI'][0]='990001';snap['market_codes']['KOSPI'].sort()
            new=gzip.compress(p.jbytes(snap),mtime=0)
            snap_path.write_bytes(new)
            manifest_path=root/'rosters/shard_00/manifest.json'
            meta=p.rjson(manifest_path);meta['files'][0]['sha256']=p.digest(new);p.wjson(manifest_path,meta)
            with self.assertRaisesRegex(RuntimeError,'FULL_DATE_PRICE_ROSTER_IDENTITIES_NOT_APPROVED'):
                p.verify_full(root/'rosters',root/'pitdata',root/'s2',root/'s4',root/'reports',strict=False)
            report=p.rjson(root/'reports/stage5_dated_market_roster_audit_summary.json')
            self.assertEqual(report['mismatch_days'],1)

    def test_existing_stage3_stage4_yml_reused_without_new_workflow_name(self):
        yml=(BASE/'.github/workflows/stockhunter_pit_stage3_krx_events.yml').read_text()
        self.assertIn('stage4-retrospective-type-evidence:',yml)
        self.assertIn('official-event-audit:',yml)
        self.assertIn('stage5-date-roster-eight-shards:',yml)
        self.assertIn('stage5-verified-dated-market-merge:',yml)
        self.assertIn("default: stage5",yml)
        self.assertIn("if: ${{ github.event.inputs.audit_phase == 'stage3' }}",yml)
        self.assertNotIn('github.event.inputs.audit_phase != \'stage4\'',yml)

    def test_missing_full_day_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root,M,days,codes=synthetic(tmp)
            m=root/'rosters/shard_07/manifest.json';a=p.rjson(m);a['files']=[];p.wjson(m,a)
            with self.assertRaisesRegex(RuntimeError,'SHARD_DATE_GAP'):
                p.verify_full(root/'rosters',root/'pitdata',root/'s2',root/'s4',root/'reports',strict=False)

if __name__=='__main__':unittest.main()
