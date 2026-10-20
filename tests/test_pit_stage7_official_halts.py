"""Provider-free fail-closed Stage7 audit tests; no KRX credentials needed."""
import csv
import importlib.util
import io
import json
import pathlib
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT=pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'research_pit'))
import pit_stage7_official_halts as m

class Tests(unittest.TestCase):
    def test_selection_not_random_and_controls(self):
        profiles={f'{i:06d}':{'zero_volume_proxy_sessions':str(i),'observed_cap_sessions':'200'} for i in range(20)}
        codes={x:'KR'+x+'0000' for x in profiles}
        c=m.choose(profiles,codes,6)
        self.assertEqual(len(c),6)
        self.assertEqual([x['code'] for x in c[:3]],['000019','000018','000017'])
        self.assertEqual([x['code'] for x in c[3:]],['000000','000001','000002'])
        self.assertTrue(all(x['selection']=='NONRANDOM_ZERO_VOLUME_DIAGNOSTIC' for x in c))

    def test_request_isin_and_date_bounded(self):
        b=m.request_body('418250','KR7418250007','20250101','20261012')
        self.assertEqual(b['bld'],'dbms/MDC/STAT/issue/MDCSTAT21301')
        self.assertEqual(b['isuCd'],'KR7418250007')
        self.assertEqual(b['isuCd2'],'418250')
        self.assertNotIn('mktId',b)
        for args in [('418250','BAD','20250101','20261012'),('BAD','KR7418250007','20250101','20261012'),
                     ('418250','KR7418250007','20240101','20261012'),('418250','KR7418250007','20260102','20260101')]:
            with self.subTest(args=args),self.assertRaises(RuntimeError):m.request_body(*args)

    def test_post_uses_official_issue_isin_not_issuer_id(self):
        class Resp:
            text='{}'
            def raise_for_status(self): pass
            def json(self):return {'output':[]}
        class Auth:
            def __init__(self):self.last=None
            def post(self,u,data,timeout):self.last=(u,data,timeout);return Resp()
        a=Auth();r=m.get_source('halt','418250','KR7418250007','20250101','20261012',a)
        self.assertEqual(r['output'],[])
        self.assertEqual(a.last[1]['isuCd2'],'418250')
        self.assertEqual(a.last[1]['isuCd'],'KR7418250007')

    def test_decode_valid_empty_is_not_positive(self):
        self.assertEqual(m.decode({'output':[]}),[])
        self.assertEqual(m.decode({'OutBlock_1':[]}),[])
        for bad in ({}, {'output':{}},{'output':[1]},{'_error_code':'LOGOUT','output':[]},[]):
            with self.subTest(bad=bad),self.assertRaises(RuntimeError):m.decode(bad)

    def test_interpret_known_code_and_date_only(self):
        issue={'code':'418250','official_listing_event_isin':'KR7418250007'}
        row={'ISU_SRT_CD':'418250','TRD_HALT_DD':'2025/05/15','TRD_RSM_DD':'2025/06/17'}
        result=m.interpret_row(row,issue,'20250101','20261012')
        self.assertEqual(result['status'],'SOURCE_EVENT_CODE_DATE_MATCH_ONLY')
        self.assertEqual(result['halt_date'],'2025-05-15')
        self.assertEqual(result['resume_date'],'2025-06-17')

    def test_interpret_wrong_identifier_or_unrecognized_date_undecided(self):
        issue={'code':'418250','official_listing_event_isin':'KR7418250007'}
        cases=[({'ISU_SRT_CD':'418251','TRD_HALT_DD':'2025/05/15'},'ROW_IDENTITY_CONFLICT'),
               ({'ISU_SRT_CD':'418250','ISU_CD':'KR7005930003','TRD_HALT_DD':'2025/05/15'},'ROW_ISIN_CONFLICT'),
               ({'ISU_SRT_CD':'418250','TRD_DD':'2025/05/15'},'SCHEMA_OR_DATE_UNVERIFIED'),
               ({'TRD_HALT_DD':'2025/05/15'},'SCHEMA_OR_DATE_UNVERIFIED'),
               ({'ISU_SRT_CD':'418250','TRD_HALT_DD':'2024/05/15'},'SCHEMA_OR_DATE_UNVERIFIED'),
               ({'ISU_SRT_CD':'418250','TRD_HALT_DD':'2025/05/15','TRD_RSM_DD':'2025/01/01'},'BAD_RESUME_BEFORE_START')]
        for data,exp in cases:
            with self.subTest(data=data): self.assertEqual(m.interpret_row(data,issue,*m.WINDOW)['status'],exp)

    def test_duplicate_or_ambiguous_candidate_identity_not_silently_joined(self):
        with tempfile.TemporaryDirectory() as d:
            s3=pathlib.Path(d)/'s3';s6=pathlib.Path(d)/'s6';s3.mkdir();s6.mkdir()
            s3s={'source_price_run_id':m.SOURCE_STAGE1_RUN,'source_price_head_sha':m.SOURCE_STAGE1_SHA,
                 'official_new_listing_event_rows':m.EXPECTED_NEWLIST,'observed_codes':m.EXPECTED_CODES,'strategy_ready':False}
            s6s={'source_stage1_run':m.SOURCE_STAGE1_RUN,'source_stage5_run':39383970402,
                 'source_stage4_run':39373355974,'cap_rows':m.EXPECTED_CAP_ROWS,'zero_volume_proxy_rows':m.EXPECTED_ZERO_VOLUME,
                 'distinct_codes':m.EXPECTED_CODES,'stage4_unresolved_type_codes':m.EXPECTED_UNRESOLVED_TYPES,
                 'historical_market_cap_price_source_agrees':True,'strategy_ready':False}
            m.dump(s3/'stage3_event_crosscheck_summary.json',s3s)
            m.dump(s6/'stage6_historical_market_cap_audit_summary.json',s6s)
            # synthetic actual count shape: 3341 rows, all zero-volume counts sum to 229867
            fields=['code','zero_volume_proxy_sessions','observed_cap_sessions',
                    'original_historical_security_type_verified','official_suspension_verified']
            p=[]
            for i in range(3341):
                p.append({'code':f'{i:06d}','zero_volume_proxy_sessions':'229867' if i==0 else '0',
                          'observed_cap_sessions':'229867' if i==0 else '1',
                          'original_historical_security_type_verified':'False','official_suspension_verified':'False'})
            m.outcsv(s6/'all_3341_market_cap_observation_profiles.csv',p,fields)
            window=[]
            for i in range(16):
                kind='new_listing' if i<8 else 'delisted';r=[]
                if i==0:
                    r=[{'ISU_CD':'000001','ISU_CD_FULL':'KR7000001000'}]*1014 + [{'ISU_CD':'000001','ISU_CD_FULL':'KR7000001001'}]
                raw=(json.dumps({'output':r},sort_keys=True,separators=(',',':'))+'\n').encode()
                rel='raw_krx/'+str(i)+'.json';m.save(s3/rel,raw)
                window.append({'endpoint':kind,'start':str(i),'end':str(i),'raw_file':rel,'raw_sha256':m.sha(raw),'rows':len(r)})
            m.dump(s3/'official_event_source_manifest.json',{'windows':window})
            profiles,ids,conf=m.evidence(s3,s6)
            self.assertNotIn('000001',ids)
            self.assertIn('000001',conf)
            self.assertEqual(len(profiles),m.EXPECTED_CODES)
            # If raw is tampered fail closed instead of trusting the manifest.
            m.save(s3/'raw_krx/0.json',b'{"output":[]}\n')
            with self.assertRaisesRegex(RuntimeError,'RAW_SHA_MISMATCH'):m.evidence(s3,s6)

    def test_refuse_claim_from_zero_volume_proxy(self):
        source=(ROOT/'research_pit/pit_stage7_official_halts.py').read_text()
        self.assertIn("'historical_halt_all_codes_verified':False",source)
        self.assertIn("'zero_volume_halt_day_join_verified':False",source)
        self.assertIn("'strategy_ready':False",source)
        self.assertNotIn('place_order(',source)
        self.assertNotIn('github.rest.pulls.merge',source)

    def test_provider_empty_all_fails_closed_but_saves_raw(self):
        # Mock only evidence links; no network. Query n=2 sample codes.
        profiles={f'{i:06d}':{'zero_volume_proxy_sessions':str(i),'observed_cap_sessions':'200'} for i in range(3)}
        ids={c:'KR'+c+'0000' for c in profiles}
        # 2 + 1 or 2 minimum tests: select code works with 3 eligible
        with tempfile.TemporaryDirectory() as d,patch.object(m,'evidence',return_value=(profiles,ids,{})):
            with self.assertRaisesRegex(RuntimeError,'NO_MAPPED_POSITIVE'):
                m.run('ignored','ignored',d,n=2,pause=0,client=lambda *_:{'output':[]})
            report=m.readjson(pathlib.Path(d)/'stage7_official_halt_canary_summary.json')
            self.assertFalse(report['official_halt_coverage_claim'])
            self.assertEqual(report['provider_empty_response_codes'],2)
            self.assertEqual(len(list((pathlib.Path(d)/'raw_krx').glob('*.json'))),2)

    def test_provider_mapped_only_returns_research_gate_false(self):
        profiles={f'{i:06d}':{'zero_volume_proxy_sessions':str(i),'observed_cap_sessions':'200'} for i in range(3)}
        ids={c:'KR'+c+'0000' for c in profiles}
        with tempfile.TemporaryDirectory() as d,patch.object(m,'evidence',return_value=(profiles,ids,{})):
            def mocked(issue,start,end):
                return {'output':[{'ISU_SRT_CD':issue['code'],'TRD_HALT_DD':'2025/09/09'}]}
            report=m.run('ignored','ignored',d,n=2,pause=0,client=mocked)
            self.assertFalse(report['strategy_ready'])
            self.assertFalse(report['official_halt_coverage_claim'])
            self.assertEqual(report['provider_mapped_halt_event_rows'],2)

    def test_unknown_schema_keeps_raw_and_fails_closed(self):
        profiles={f'{i:06d}':{'zero_volume_proxy_sessions':str(i),'observed_cap_sessions':'200'} for i in range(3)}
        ids={c:'KR'+c+'0000' for c in profiles}
        with tempfile.TemporaryDirectory() as d,patch.object(m,'evidence',return_value=(profiles,ids,{})):
            def mocked(issue,start,end):
                return {'output':[{'ISU_SRT_CD':issue['code'],'NEW_UNEXPECTED_DATE_KEY':'2025/09/09'}]}
            with self.assertRaisesRegex(RuntimeError,'NO_MAPPED_POSITIVE'):
                m.run('ignored','ignored',d,n=2,pause=0,client=mocked)
            self.assertTrue((pathlib.Path(d)/'halt_provider_rows_schema_audit.csv').is_file())
            rows=m.readcsv(pathlib.Path(d)/'halt_provider_rows_schema_audit.csv')
            self.assertEqual(rows[0]['status'],'SCHEMA_OR_DATE_UNVERIFIED')

if __name__=='__main__':unittest.main()
