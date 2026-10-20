"""Offline, provider-free Stage3 KRX event audit contracts."""
import csv
import importlib.util
import json
import pathlib
import sys
import tempfile
import unittest
from unittest.mock import patch

BASE=pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0,str(BASE/'research_pit'))
import pit_official_events_audit as p


def fixture(tmp):
    out=pathlib.Path(tmp)
    (out/'lifecycle_summary.json').write_text(json.dumps({
        'source_run_id':p.EXPECTED_SOURCE_RUN,'source_head_sha':p.EXPECTED_SOURCE_SHA,
        'observed_codes':3,'codes_last_observed_before_final':2,'price_rows':p.EXPECTED_PRICE_ROWS,
        'status':'PRICE_OBSERVATIONS_ONLY_SECURITY_MASTER_UNVERIFIED','strategy_ready':False}),encoding='utf-8')
    (out/'source_fullmarket_price_verification.json').write_text(json.dumps({
        'full_market_price_rows':p.EXPECTED_PRICE_ROWS,'sessions':1907,
        'whole_market_pit_strategy_ready':False}),encoding='utf-8')
    with (out/'observed_ticker_lifecycles.csv').open('w',newline='',encoding='utf-8') as f:
        wr=csv.DictWriter(f,['code','first_seen','last_seen','last_observation_before_end'])
        wr.writeheader()
        for row in [
         {'code':'000020','first_seen':'2019-01-02','last_seen':'2026-10-12','last_observation_before_end':'False'},
         {'code':'03473K','first_seen':'2019-01-02','last_seen':'2022-08-01','last_observation_before_end':'True'},
         {'code':'0099X0','first_seen':'2025-02-01','last_seen':'2026-09-01','last_observation_before_end':'True'},
        ]:wr.writerow(row)
    return out


def events():
    return [
        {'kind':'delisted','date':'2022-08-05','code':'03473K','raw_issue_id':'03473K','name':'가짜A','market':'KOSPI','security_class':'우선주','reason':'합병','identity_matchable':True,'source_row_number':1,'response_window':'20220101_20221231'},
        {'kind':'new_listing','date':'2026-02-03','code':'0099X0','raw_issue_id':'0099X0','name':'가짜B','market':'KOSPI','security_class':'보통주','reason':'','identity_matchable':True,'source_row_number':1,'response_window':'20260101_20261012'},
    ]


class Tests(unittest.TestCase):
    def test_yearly_windows_pinned_no_future(self):
        w=p.yearly_windows()
        self.assertEqual(len(w),8)
        self.assertEqual(w[0],('20190102','20191231'))
        self.assertEqual(w[-1],('20260101','20261012'))
        self.assertEqual(len(set(w)),8)

    def test_short_codes_preserved_isin_not_coerced(self):
        for code in ['03473K','0099X0','005930']:
            self.assertEqual(p.norm_code(code),code)
        self.assertEqual(p.norm_code('KR7005930003'),'')
        self.assertEqual(p.norm_code('0347*K'),'')

    def test_official_delist_schema(self):
        rows=[{'ISU_CD':'03473K','ISU_NM':'가짜','MKT_NM':'KOSPI',
               'DELIST_DD':'2022/08/05','DELIST_RSN_DSC':'합병','SECU_GRP_NM':'주권'}]
        n=p.normalize_events(rows,'delisted','20220101','20221231')
        self.assertEqual(n[0]['code'],'03473K')
        self.assertEqual(n[0]['date'],'2022-08-05')
        self.assertEqual(n[0]['reason'],'합병')

    def test_wrong_dates_and_response_fail_closed(self):
        with self.assertRaisesRegex(RuntimeError,'OUTSIDE_WINDOW'):
            p.normalize_events([{'ISU_CD':'005930','DELIST_DD':'2023-01-01'}], 'delisted','20220101','20221231')
        for obj in ({'foo':[]},{'output':[]},{'_error_code':'LOGOUT','output':[]}):
            with self.subTest(obj=obj),self.assertRaises(RuntimeError):
                p.decode_json(obj,'delisted','20220101','20221231')

    def test_source_summary_pinned_and_terminal_counts(self):
        with tempfile.TemporaryDirectory() as temp, patch.object(p,'EXPECTED_CODES',3), patch.object(p,'EXPECTED_TERMINAL',2):
            f=fixture(temp)
            self.assertEqual(len(p.check_stage2(f)),3)
            x=json.loads((f/'lifecycle_summary.json').read_text())
            x['source_head_sha']='bad';(f/'lifecycle_summary.json').write_text(json.dumps(x))
            with self.assertRaisesRegex(RuntimeError,'UNPINNED_STAGE2_SUMMARY'):p.check_stage2(f)

    def test_event_matches_do_not_claim_delisting_or_strategy_ready(self):
        with tempfile.TemporaryDirectory() as temp:
            x=pathlib.Path(temp);obs={
                '03473K':{'first_seen':'2019-01-02','last_seen':'2022-08-01','last_observation_before_end':'True'},
                '0099X0':{'first_seen':'2025-02-01','last_seen':'2026-09-01','last_observation_before_end':'True'},
                '000020':{'first_seen':'2019-01-02','last_seen':'2026-10-12','last_observation_before_end':'False'}}
            r=p.compare(obs,events(),x)
            self.assertFalse(r['strategy_ready'])
            self.assertFalse(r['isin_issue_identity_verified'])
            self.assertEqual(r['terminal_code_date_event_matches'],1)
            self.assertEqual(r['terminal_without_matching_2019_2026_delist_event'],1)
            with (x/'priority_472_terminal_issue_codes.csv').open(encoding='utf-8') as fd:
                rr={x['code']:x for x in csv.DictReader(fd)}
            self.assertEqual(rr['03473K']['event_match_status'],'KRX_DELIST_EVENT_CODE_DATE_MATCH_ISIN_UNVERIFIED')
            self.assertEqual(rr['0099X0']['event_match_status'],'NO_MATCH_IN_2019_2026_KRX_DELIST_EVENT_REGISTER')

    def test_pre_observation_delist_not_misread_as_current_delist(self):
        obs={'005930':{'first_seen':'2019-01-02','last_seen':'2026-10-12','last_observation_before_end':'False'}}
        old={**events()[0],'code':'005930','raw_issue_id':'005930','date':'2021-01-01'}
        with tempfile.TemporaryDirectory() as d:
            r=p.compare(obs,[old],d)
            self.assertEqual(r['terminal_before_cutoff'],0)
            with (pathlib.Path(d)/'all_3341_issue_code_event_crosscheck.csv').open() as f:
                row=next(csv.DictReader(f))
            self.assertIn('CONFLICTING_',row['event_match_status'])

    def test_16_yearly_windows_raw_sha_reproducible(self):
        with tempfile.TemporaryDirectory() as d:
            def fake(k,start,end):
                key='DELIST_DD' if k=='delisted' else 'LIST_DD'
                return {'output': [{'ISU_CD':'000020',key:start,'ISU_NM':'가짜'}]}
            rows,manifests=p.fetch_all(d,client=fake,pause=0)
            self.assertEqual(len(rows),16)
            self.assertEqual(len(manifests),16)
            for e in manifests:
                raw=(pathlib.Path(d)/e['raw_file']).read_bytes()
                self.assertEqual(e['raw_sha256'],p.sha(raw))

    def test_official_new_listing_payload_differs_from_delisting_payload(self):
        start,end='20190102','20191231'
        listed=p.build_krx_request('new_listing',start,end)
        deleted=p.build_krx_request('delisted',start,end)
        self.assertEqual(listed['bld'],'dbms/MDC/STAT/issue/MDCSTAT20001')
        for key in ('isurCd','isurCd2','listClssCd','secugrpTp','cntrIsoCd'):
            self.assertEqual(listed[key],'ALL')
        self.assertEqual(listed['strtDd'],start)
        self.assertEqual(listed['endDd'],end)
        for key in ('isuCd','isuCd2'):
            self.assertNotIn(key,listed)
            self.assertEqual(deleted[key],'ALL')
        for key in ('isurCd','isurCd2'):
            self.assertNotIn(key,deleted)
        with self.assertRaisesRegex(RuntimeError,'UNKNOWN_KRX_REGISTRY'):
            p.build_krx_request('unknown',start,end)

    def test_live_like_new_listing_outblock_schema_and_empty_fail_closed(self):
        class Response:
            text='{}'
            def raise_for_status(self):pass
            def json(self):
                return {'CURRENT_DATETIME':'2026.10.20 AM 09:18:12',
                        'OutBlock_1':[{'ISU_CD':'KR7005930003','ISU_SRT_CD':'005930',
                                       'ISUR_NM':'가짜발행회사','LIST_DD':'2019/01/03'}]}
        class FakeAuth:
            def __init__(self):self.received=[]
            def post(self,url,data,timeout):
                self.received.append((url,data,timeout));return Response()
        a=FakeAuth();got=p.fetch_krx('new_listing','20190102','20191231',auth=a)
        self.assertEqual(a.received[0][1]['isurCd'],'ALL')
        self.assertNotIn('isuCd',a.received[0][1])
        normalized=p.normalize_events(p.decode_json(got,'new_listing','20190102','20191231'),
                                      'new_listing','20190102','20191231')
        self.assertEqual(normalized[0]['code'],'005930')
        self.assertEqual(normalized[0]['date'],'2019-01-03')
        self.assertTrue(normalized[0]['identity_matchable'])
        with self.assertRaisesRegex(RuntimeError,'KRX_EMPTY_OR_NOT_LIST'):
            p.decode_json({'CURRENT_DATETIME':'2026.10.20','output':[]},'new_listing','20190102','20191231')

    def test_issuer_identifier_is_never_treated_as_issue_code(self):
        # Issuer codes and issue-level securities codes use different namespaces.
        # A six-character issuer ID must not accidentally join to a stock ticker.
        raw=[{'ISUR_CD':'005930','ISUR_NM':'가짜발행인','LIST_DD':'2020/02/03'}]
        n=p.normalize_events(raw,'new_listing','20200101','20201231')
        self.assertEqual(n[0]['code'],'')
        self.assertEqual(n[0]['raw_issuer_id'],'005930')
        self.assertFalse(n[0]['identity_matchable'])

    def test_no_live_or_strategy_mutations(self):
        src=(BASE/'research_pit'/'pit_official_events_audit.py').read_text(encoding='utf-8')
        self.assertIn('strategy_ready\':False',src)
        self.assertNotIn('place_order(',src)

if __name__=='__main__':unittest.main()
