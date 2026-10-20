"""Stage7-C source-canary tests; no KRX network, no credentials, no LIVE code."""
import importlib.util
import json
import pathlib
import sys
import tempfile
import unittest
from unittest.mock import patch
ROOT=pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'research_pit'))
import pit_stage7c_kind_resume_source as m


def fixture(stage='2026-07-22',code='418250',name='시큐레터',title='주권매매거래정지기간변경(개선기간 부여)',receipt='20260722900001'):
    return f'''<html><body><table class="list type-00 mt10"><thead><tr><th>번호</th></tr></thead>
        <tbody><tr><td>1</td><td>{stage} 18:00</td><td>
        <img class="legend" alt="코스닥"/><a onclick="companysummary_open('09876')">{name}</a></td>
        <td><a title="{title}" onclick="openDisclsViewer('{receipt}','')">{title}</a></td>
        <td>코스닥시장본부</td></tr></tbody></table></body></html>'''.encode('utf8')


def empty():
    return b'<html><body><table class="list type-00 mt10"><tbody><tr><td>\xec\xa1\xb0\xed\x9a\x8c\xeb\x90\x9c \xea\xb2\xb0\xea\xb3\xbc\xea\xb0\x80 \xec\x97\x86\xec\x8a\xb5\xeb\x8b\x88\xeb\x8b\xa4</td></tr></tbody></table></body></html>'


def source_summary():
    return {'source_stage1_run':m.STAGE1_RUN,'source_stage1_commit':m.PINNED_PRICE_SHA,
        'source_stage7a_run':m.STAGE7A_RUN,'source_stage7a_commit':'d60639f9f9920290b51dbb9e502ead0a4cbe254f',
        'status':'OFFICIAL_HALT_START_VS_IMMUTABLE_PRICE_OBSERVATION_MATCH_ONLY',
        'official_halt_start_event_rows':2,'official_resume_event_rows':0,
        'focus_price_observations':1809,'source_original_daily_price_sha_verified':True,
        'strategy_ready':False,'reviewed_codes':['377460','418250'],
        'focus_event_comparisons':[
            {'code':'377460','official_halt_event_date':'2023-11-28','actual_official_resume_date_confirmed':False,'official_resume_date':None},
            {'code':'418250','official_halt_event_date':'2024-04-05','actual_official_resume_date_confirmed':False,'official_resume_date':None}]}


class Stage7CTests(unittest.TestCase):
    def make_stage7b(self,root):
        src=root/'stage7b';src.mkdir()
        m.writejson(src/'stage7b_halt_price_bridge_summary.json',source_summary())
        return src

    def test_windows_cover_research_period_without_overlap_or_gap(self):
        windows=m.half_year_windows()
        self.assertEqual(windows[0][0],'2023-10-01')
        self.assertEqual(windows[-1][-1],'2026-10-12')
        self.assertEqual(len(windows),7)
        self.assertTrue(all(m.checked_date(b)>=m.checked_date(a) for a,b in windows))

    def test_form_is_specific_code_and_historical_market_action_filters(self):
        d=m.kind_form('418250','2026-04-01','2026-09-30',1)
        self.assertEqual(d['repIsuSrtCd'],'A418250')
        self.assertEqual(d['disclosureType02'],'0311|0305|')
        self.assertEqual(d['pDisclosureType02'],'0311|0305|')
        self.assertEqual(d['disclosureTypeArr02'],['0311','0305'])
        self.assertEqual(d['lastReport'],'')
        self.assertEqual(d['pageIndex'],'1')
        self.assertEqual(d['searchCorpName'],'')

    def test_wrong_code_and_page_fail_closed(self):
        for code,pg in [('005930',1),('418250',0),('418250',16)]:
            with self.subTest(code=code,page=pg),self.assertRaises(RuntimeError):
                m.kind_form(code,'2026-04-01','2026-09-30',pg)

    def test_parse_actual_kind_disclosure_metadata_as_candidate_not_resume(self):
        rows=m.parse_kind_html(fixture(),'418250','2026-04-01','2026-09-30',1)
        self.assertEqual(len(rows),1)
        self.assertEqual(rows[0]['accession'],'20260722900001')
        self.assertEqual(rows[0]['submission_date_from_accession'],'2026-07-22')
        self.assertEqual(rows[0]['notice_class'],'HALT_PERIOD_CHANGE_TITLE_ONLY')
        self.assertFalse(rows[0]['official_resume_effective_date_verified'])
        self.assertFalse(rows[0]['daily_halt_state_verified'])

    def test_resume_title_does_not_prove_effective_resume(self):
        r=m.parse_kind_html(fixture(title='주권매매거래정지해제'),'418250','2026-04-01','2026-09-30',1)[0]
        self.assertEqual(r['notice_class'],'POSSIBLE_RELEASE_NOTICE_TITLE_ONLY')
        self.assertFalse(r['official_resume_effective_date_verified'])

    def test_empty_html_table_valid_but_not_full_coverage(self):
        self.assertEqual(m.parse_kind_html(empty(),'418250','2026-04-01','2026-09-30',1),[])

    def test_unknown_html_and_mismatched_company_fails(self):
        with self.assertRaisesRegex(RuntimeError,'TABLE_MISSING'):
            m.parse_kind_html('<html><body>maintenance</body></html>','418250','2026-04-01','2026-09-30',1)
        with self.assertRaisesRegex(RuntimeError,'FILTER_LEAK'):
            m.parse_kind_html(fixture(name='삼성전자'),'418250','2026-04-01','2026-09-30',1)

    def test_accession_date_must_be_in_requested_window(self):
        with self.assertRaisesRegex(RuntimeError,'RECEIPT_OUTSIDE_QUERY_WINDOW'):
            m.parse_kind_html(fixture(),'418250','2025-01-01','2025-06-30',1)

    def test_preflight_pinned_prior_stage7b(self):
        with tempfile.TemporaryDirectory() as t:
            p=self.make_stage7b(pathlib.Path(t))
            self.assertEqual(len(m.preflight(p)['stage7b_source_event_codes']),2)
            j=source_summary();j['official_resume_event_rows']=1
            m.writejson(p/'stage7b_halt_price_bridge_summary.json',j)
            with self.assertRaisesRegex(RuntimeError,'ATTESTATION_DRIFT'):m.preflight(p)

    def test_end_to_end_canary_with_one_positive_while_keeping_strategy_gate_off(self):
        with tempfile.TemporaryDirectory() as t:
            root=pathlib.Path(t);src=self.make_stage7b(root)
            calls=[]
            def provider(form):
                calls.append((form['repIsuSrtCd'],form['fromDate'],form['toDate']))
                if form['repIsuSrtCd']=='A418250' and form['fromDate']=='2026-04-01':
                    # 2026-04-01 query window matches the expected 2026 Jul receipt
                    return fixture()
                return empty()
            # Rolling six months: 2023-10, 2024-04, 2024-10, 2025-04, 2025-10, 2026-04, 2026-10
            out=root/'out';j=m.run(src,out,provider=provider,pause=0)
            self.assertEqual(j['request_count'],14)
            self.assertEqual(len(calls),14)
            self.assertEqual(j['notice_candidate_count'],1)
            self.assertFalse(j['strategy_ready'])
            self.assertFalse(j['historical_daily_halt_status_verified'])
            self.assertFalse(j['official_resume_effective_date_verified'])
            self.assertTrue(j['raw_replay_evidence']['raw_source_sha_verified'])
            self.assertEqual(len(m.loadjson(out/'source_manifest.json')['files']),14)
            self.assertEqual(j['matched_codes'],['418250'])
            f=out/'raw_kind/418250_20260401_20260930_p01.html'
            f.write_bytes(b'HACK')
            with self.assertRaisesRegex(RuntimeError,'RAW_SHA_MISMATCH'):
                m.verify_saved_raw(out)

    def test_empty_all_sources_does_not_silently_approve(self):
        with tempfile.TemporaryDirectory() as t:
            root=pathlib.Path(t);src=self.make_stage7b(root);out=root/'out'
            with self.assertRaisesRegex(RuntimeError,'NO_POSITIVE_KIND_SOURCE_CONTROL'):
                m.run(src,out,provider=lambda form:empty(),pause=0)
            self.assertFalse(m.loadjson(out/'stage7c_kind_resume_source_summary.json')['strategy_ready'])
            self.assertEqual(len(m.loadjson(out/'source_manifest.json')['files']),14)

    def test_no_production_or_trade_imports(self):
        s=(ROOT/'research_pit/pit_stage7c_kind_resume_source.py').read_text()
        for forbidden in ['place_order(', 'LIVE_ENABLE=True', 'strategy_ready=True', 'allow_trading=True']:
            self.assertNotIn(forbidden,s)
        self.assertIn("'strategy_ready':False",s)


if __name__=='__main__':unittest.main()
