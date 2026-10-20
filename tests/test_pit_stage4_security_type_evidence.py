"""Research-only offline Stage4 contracts. No network; no original large archive required."""
import csv
import json
import pathlib
import sys
import tempfile
import unittest
from unittest.mock import patch

BASE=pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0,str(BASE/'research_pit'))
import pit_stage4_security_type_evidence as p


def sample_fixture(path):
    d=pathlib.Path(path);s2=d/'stage2';s3=d/'stage3';s2.mkdir();s3.mkdir()
    p.dumpjson(s2/'lifecycle_summary.json',{'source_run_id':p.SOURCE_RUN,'source_head_sha':p.SOURCE_SHA,
        'observed_codes':3,'codes_last_observed_before_final':2,'price_rows':p.PRICE_ROWS,'strategy_ready':False})
    p.dumpjson(s2/'source_fullmarket_price_verification.json',{'full_market_price_rows':p.PRICE_ROWS,'sessions':1907,
        'whole_market_pit_strategy_ready':False})
    raw=[{'code':'000020','first_seen':'2019-01-02','last_seen':p.END,'last_observation_before_end':'False','seen_sessions':100,'zero_or_nontradeable_proxy_sessions':0},
         {'code':'03473K','first_seen':'2019-01-02','last_seen':'2022-08-01','last_observation_before_end':'True','seen_sessions':90,'zero_or_nontradeable_proxy_sessions':12},
         {'code':'0099X0','first_seen':'2025-02-03','last_seen':'2026-09-01','last_observation_before_end':'True','seen_sessions':65,'zero_or_nontradeable_proxy_sessions':1}]
    p.dumpcsv(s2/'observed_ticker_lifecycles.csv',raw,list(raw[0]))
    p.dumpjson(s3/'stage3_event_crosscheck_summary.json',{
        'source_stage2_run_id':p.STAGE2_RUN,'source_price_run_id':p.SOURCE_RUN,'source_price_head_sha':p.SOURCE_SHA,
        'strategy_ready':False,'observed_codes':3,'terminal_before_cutoff':2,
        'official_delisting_event_rows':8,'official_new_listing_event_rows':8,
        'official_event_identity_unmatchable_rows':0,'terminal_code_date_event_matches':2,
        'terminal_without_matching_2019_2026_delist_event':0})
    p.dumpcsv(s3/'all_3341_issue_code_event_crosscheck.csv',raw,list(raw[0]))
    p.dumpcsv(s3/'priority_472_terminal_issue_codes.csv',raw[1:],list(raw[0]))
    windows=[];norm=[]
    for kind in ('new_listing','delisted'):
        for year in range(2019,2027):
            start=f'{year}0101' if year>2019 else '20190102'
            end=f'{year}1231' if year<2026 else '20261012'
            code='0099X0' if kind=='new_listing' else '03473K'
            row={'ISU_CD':code,'LIST_DD':f'{year}/06/03','DELIST_DD':f'{year}/08/05',
                'SECUGRP_NM':'주권','KIND_STKCERT_TP_NM':'보통주',
                'ISU_NM':'테스트 기업' if kind=='new_listing' else '테스트 스팩'}
            file=f'raw_krx/{kind}_{start}_{end}.json'
            content=(json.dumps({'output':[row]},ensure_ascii=False,sort_keys=True,separators=(',',':'))+'\n').encode('utf-8')
            where=s3/file;where.parent.mkdir(exist_ok=True);where.write_bytes(content)
            windows.append({'endpoint':kind,'start':start,'end':end,'rows':1,
                           'raw_file':file,'raw_sha256':p.sha(content)})
            norm.append({'kind':kind,'date':f'{year}-06-03' if kind=='new_listing' else f'{year}-08-05',
                'code':code,'security_class':'주권','response_window':f'{start}_{end}','source_row_number':1})
    p.dumpjson(s3/'official_event_source_manifest.json',{'schema':p.MANIFEST_FORMAT,'windows':windows})
    p.dumpcsv(s3/'all_official_event_rows_normalized.csv',norm,list(norm[0]))
    return s2,s3


class ContractTests(unittest.TestCase):
    def setUp(self):
        patches=[patch.object(p,'EXPECTED_CODES',3),patch.object(p,'EXPECTED_TERMINAL',2),
                 patch.object(p,'EXPECTED_DELIST',8),patch.object(p,'EXPECTED_NEWLIST',8),
                 patch.object(p,'EXPECTED_UNMATCHABLE',0)]
        for item in patches:item.start();self.addCleanup(item.stop)

    def test_code_is_six_without_issuer_isin_coercion(self):
        self.assertEqual(p.is_six_code('0099X0'),'0099X0')
        self.assertEqual(p.is_six_code('03473K'),'03473K')
        self.assertEqual(p.is_six_code('KR7005930003'),'')
        self.assertEqual(p.is_six_code('05882019'),'')
        self.assertEqual(p.is_six_code('!005930'),'')

    def test_official_stock_class_decision_is_conservative(self):
        raw={'group':'주권','share_kind':'보통주','name':'가나다 회사'}
        self.assertEqual(p.class_of_event(raw),'RETROSPECTIVE_EVENT_COMMON_SHARE_CANDIDATE')
        self.assertEqual(p.class_of_event({**raw,'name':'다람쥐스팩1호'}),'SPAC_NAME_REVIEW_NOT_OFFICIAL_SPAC_CLASS')
        self.assertEqual(p.class_of_event({**raw,'share_kind':'신형우선주'}),'RETROSPECTIVE_EVENT_PREFERRED_OR_CLASS_SHARE')
        self.assertEqual(p.class_of_event({**raw,'group':'주식예탁증권'}),'OFFICIAL_EVENT_NON_STOCK_OR_SPECIAL_SECURITY')
        self.assertEqual(p.class_of_event({**raw,'share_kind':''}),'UNRESOLVED_UNKNOWN_STOCK_KIND')

    def test_observation_and_stage3_source_pins(self):
        with tempfile.TemporaryDirectory() as d:
            s2,s3=sample_fixture(d)
            obs,_,_=p.preflight(s2,s3)
            self.assertEqual(len(obs),3)
            bad=p.readjson(s3/'stage3_event_crosscheck_summary.json');bad['source_stage2_run_id']=0
            p.dumpjson(s3/'stage3_event_crosscheck_summary.json',bad)
            with self.assertRaisesRegex(RuntimeError,'STAGE3_PIN'):p.preflight(s2,s3)

    def test_complete_manifest_sha_and_raw_normalized_parity(self):
        with tempfile.TemporaryDirectory() as d:
            s2,s3=sample_fixture(d)
            events=p.official_raw_events(s3,{})
            self.assertEqual(len(events),16)
            self.assertEqual(events[0]['code'],'0099X0')
            item=s3/'raw_krx/new_listing_20190102_20191231.json'
            item.write_text('{}')
            with self.assertRaisesRegex(RuntimeError,'RAW_SHA_MISMATCH'):p.official_raw_events(s3,{})

    def test_derived_output_never_releases_backtest_gate(self):
        with tempfile.TemporaryDirectory() as d:
            s2,s3=sample_fixture(d)
            result=p.audit(s2,s3,pathlib.Path(d)/'report')
            self.assertFalse(result['strategy_ready'])
            self.assertFalse(result['historical_pit_security_type_verified'])
            self.assertFalse(result['official_daily_suspensions_verified'])
            items=p.readcsv(pathlib.Path(d)/'report'/'all_3341_event_type_evidence.csv')
            self.assertEqual(len(items),3)
            self.assertEqual({x['code'] for x in items},{'000020','03473K','0099X0'})
            self.assertTrue(all(x['strategy_ready']=='False' for x in items))
            self.assertTrue(all(x['historical_pit_security_type_verified']=='False' for x in items))
            self.assertEqual(len(p.readcsv(pathlib.Path(d)/'report'/'priority_472_terminal_type_evidence.csv')),2)

    def test_conflicts_and_no_event_are_not_force_classified(self):
        with tempfile.TemporaryDirectory() as d:
            s2,s3=sample_fixture(d)
            r=p.audit(s2,s3,pathlib.Path(d)/'out')
            self.assertEqual(r['type_evidence_label_counts'].get('UNRESOLVED_NO_2019_2026_EVENT'),1)
            self.assertEqual(r['type_evidence_label_counts'].get('SPAC_NAME_REVIEW_NOT_OFFICIAL_SPAC_CLASS'),1)

    def test_untrusted_manifest_missing_window_is_blocked(self):
        with tempfile.TemporaryDirectory() as d:
            s2,s3=sample_fixture(d)
            x=p.readjson(s3/'official_event_source_manifest.json');x['windows'].pop()
            p.dumpjson(s3/'official_event_source_manifest.json',x)
            with self.assertRaisesRegex(RuntimeError,'SOURCE_MANIFEST_SCHEMA_OR_COVERAGE'):
                p.official_raw_events(s3,{})

    def test_does_not_modify_live_modules_or_order_calls(self):
        txt=(BASE/'research_pit'/'pit_stage4_security_type_evidence.py').read_text('utf-8')
        self.assertNotIn('place_order(',txt)
        self.assertNotIn('LIVE_ENABLE = 1',txt)
        self.assertIn("'strategy_ready':False",txt)


if __name__=='__main__':unittest.main()
