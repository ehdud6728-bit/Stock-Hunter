"""Stage7-I strict synthetic + pinned Stage7-H evidence regression tests (no network)."""
import csv
import os
import hashlib
import importlib.util
import inspect
import io
import json
import pathlib
import shutil
import sys
import tempfile
import types
import unittest
from unittest import mock

ROOT=pathlib.Path(__file__).resolve().parents[1]
SYS_SRC=ROOT/'research_pit'
sys.path.insert(0,str(SYS_SRC))

# In a clean ZIP-only test folder, emulate solely the Stage7-C form/parser API.
# On the actual Stock-Hunter repository, the genuine Stage7-C module imports.
if not (SYS_SRC/'pit_stage7c_kind_resume_source.py').exists():
    from datetime import date
    from bs4 import BeautifulSoup
    import re
    source=types.ModuleType('pit_stage7c_kind_resume_source')
    source.KIND_URL='https://kind.krx.co.kr/disclosure/details.do'
    def checked_date(v):return date.fromisoformat(v)
    def kind_form(code,start,end,page):
        return {'method':'searchDetailsSub','repIsuSrtCd':'A'+code,
          'lastReport':'','currentPageSize':'100','pageIndex':str(page),
          'fromDate':start,'toDate':end,'disclosureTypeArr02':['0311','0305'],
          'disclosureType02':'0311|0305|','pDisclosureType02':'0311|0305|',
          'bfrDsclsType':'on','disTypevalue':'','reportNm':''}
    def parse_kind_html(raw,code,start,end,page):
        s=BeautifulSoup(raw.decode('utf-8'),'html.parser')
        tab=s.select_one('table.list.type-00.mt10')
        if tab is None:raise RuntimeError('MISSING_KIND_TABLE')
        out=[]
        for tr in tab.select('tbody > tr'):
            td=tr.find_all('td',recursive=False)
            if len(td)<5:continue
            m=re.search(r"openDisclsViewer\('([0-9]{14})'\)",td[3].find('a')['onclick'])
            if not m:raise RuntimeError('MISSING_RECEIPT')
            ac=m.group(1)
            if not start<=ac[:4]+'-'+ac[4:6]+'-'+ac[6:8]<=end:raise RuntimeError('BAD_DATE')
            if code=='377460' and not any(v in td[2].get_text() for v in ('위니아에이드','큐에이드')):raise RuntimeError('CODE_FILTER_LEAK')
            if code=='418250' and '시큐레터' not in td[2].get_text():raise RuntimeError('CODE_FILTER_LEAK')
            out.append({'accession':ac,'code':code,'title':td[3].find('a')['title'],
                        'submission_date_from_accession':ac[:4]+'-'+ac[4:6]+'-'+ac[6:8]})
        return out
    source.checked_date=checked_date
    source.kind_form=kind_form
    source.parse_kind_html=parse_kind_html
    sys.modules[source.__name__]=source

sp=importlib.util.spec_from_file_location('pit_stage7i',SYS_SRC/'pit_stage7i_bounded_missing_notice_audit.py')
a=importlib.util.module_from_spec(sp)
sp.loader.exec_module(a)
FIX=pathlib.Path(os.environ.get('STAGE7H_EVIDENCE_DIR',str(ROOT/'VALIDATION_ONLY/evidence_stage7h')))


def html_for(code, receipts, title_override=None):
    name='(주)위니아에이드' if code=='377460' else '(주)시큐레터'
    company='WYNIA' if code=='377460' else 'SEC'
    rows=[]
    for receipt in receipts:
        title=title_override.get(receipt,'기타시장안내(상장적격성 실질심사 관련 안내)') if title_override else '기타시장안내(상장적격성 실질심사 관련 안내)'
        rows.append(f'<tr><td>코스닥</td><td>19:00</td><td><a onclick="companysummary_open(\'{company}\')">{name}</a></td>'
                    f'<td><a title="{title}" onclick="openDisclsViewer(\'{receipt}\')">{title}</a></td><td>공시</td></tr>')
    return ('<html><body><table class="list type-00 mt10"><tbody>'+''.join(rows)+
            '</tbody></table></body></html>').encode('utf-8')


def provider_for_anchor_with_extra(extra_by_window=None):
    extra_by_window=extra_by_window or {}
    def provider(form):
        code=form['repIsuSrtCd'][1:]
        start,end=form['fromDate'],form['toDate']
        match=[w for w in a.WINDOWS if w[:3]==(code,start,end)]
        if not match:raise AssertionError('UNAPPROVED_WINDOW')
        if form['pageIndex']!='1':return html_for(code,[])
        receipts=[match[0][3]]+extra_by_window.get((code,start,end),[])
        return html_for(code,receipts,{
            '20240318009999':'정정공시(상장적격성 실질심사)',
            '20240405009999':'주권매매거래정지해제(결정)',
            '20250124009999':'주권매매거래정지기간변경(해제 예정)',
            '20231026009999':'단순 일반 공시 안내',
        })
    return provider


class Stage7ITests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not all((FIX/name).is_file() for name in a.SOURCE_FILES):
            raise RuntimeError('PINNED_STAGE7H_FIXTURES_REQUIRED; copy evidence_stage7h from ZIP')
    def test_01_provenance_count(self):
        self.assertEqual(len(a.SOURCE_FILES),6)
        self.assertEqual(a.SOURCE_RUN,39578003404)
    def test_02_expected_commit(self):
        self.assertEqual(a.SOURCE_COMMIT,'0fb4392da16320c11671f9fd64f5582a28579d00')
    def test_03_sha_exact_evidence(self):
        for p,d in a.SOURCE_FILES.items():self.assertEqual(a.sha((FIX/p).read_bytes()),d)
    def test_04_genuine_57_preflight(self):
        r=a.check_evidence(FIX);self.assertEqual(r['verified_receipt_count'],57)
    def test_05_three_incidents(self):
        self.assertEqual(len(a.check_evidence(FIX)['incidents']),3)
    def test_06_one_candidate_pair(self):
        x=a.check_evidence(FIX)['correction_pair'];self.assertEqual(x['original_receipt_candidate'],'20240318000555')
    def test_07_exact_two_codes(self):
        self.assertEqual({x['code'] for x in a.check_evidence(FIX)['documents']},a.CODE_SET)
    def test_08_genuine_input_does_not_grant_approval(self):
        for x in a.check_evidence(FIX)['documents']:
            self.assertFalse(x['strategy_ready']);self.assertEqual(x['official_daily_halt_status'],'UNKNOWN')
    def test_09_wrong_sha_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            shutil.copytree(FIX,pathlib.Path(d)/'data')
            p=pathlib.Path(d)/'data'/'stage7h_fail_closed_summary.json'
            p.write_bytes(p.read_bytes()+b' ')
            with self.assertRaisesRegex(RuntimeError,'PINNED_STAGE7H_FILE_MISSING_OR_CHANGED'):
                a.check_evidence(p.parent)
    def test_10_missing_evidence_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaisesRegex(RuntimeError,'PINNED_STAGE7H_FILE_MISSING_OR_CHANGED'):
                a.check_evidence(d)
    def test_11_summary_promotion_rejected_even_if_sha_re_pinned(self):
        with tempfile.TemporaryDirectory() as d:
            shutil.copytree(FIX,pathlib.Path(d)/'data');p=pathlib.Path(d)/'data'/'stage7h_fail_closed_summary.json'
            obj=json.loads(p.read_text());obj['strategy_ready']=True
            p.write_text(json.dumps(obj),encoding='utf-8')
            with mock.patch.dict(a.SOURCE_FILES,{p.name:a.sha(p.read_bytes())}):
                with self.assertRaisesRegex(RuntimeError,'STAGE7H_SUMMARY_INVARIANT'):a.check_evidence(p.parent)
    def test_12_csv_mismatch_rejected_even_if_sha_re_pinned(self):
        with tempfile.TemporaryDirectory() as d:
            shutil.copytree(FIX,pathlib.Path(d)/'data');p=pathlib.Path(d)/'data'/'stage7h_57_notice_correction_classification_RESEARCH_ONLY.csv'
            rows=list(csv.DictReader(io.StringIO(p.read_text(encoding='utf-8-sig'))))
            rows[0]['title']='변조제목'
            stream=io.StringIO();writer=csv.DictWriter(stream,fieldnames=rows[0]);writer.writeheader();writer.writerows(rows)
            p.write_text(stream.getvalue(),encoding='utf-8')
            with mock.patch.dict(a.SOURCE_FILES,{p.name:a.sha(p.read_bytes())}):
                with self.assertRaisesRegex(RuntimeError,'STAGE7H_CSV_JSON_ROW_DRIFT'):a.check_evidence(p.parent)
    def test_13_window_positive_anchor(self):
        self.assertTrue(a.validate_windows(a.check_evidence(FIX)['by_receipt']))
    def test_14_budget_max_16(self):
        self.assertEqual(a.MAX_REQUESTS,16);self.assertEqual(len(a.WINDOWS),8)
    def test_15_window_scope_only_two_codes(self):
        self.assertEqual({w[0] for w in a.WINDOWS},a.CODE_SET)
    def test_16_expanded_form_keeps_identity_and_dates(self):
        f=a.expanded_form('377460','2024-03-14','2024-03-27',1)
        self.assertEqual(f['repIsuSrtCd'],'A377460');self.assertEqual(f['fromDate'],'2024-03-14')
    def test_17_expanded_form_removes_category_arr(self):
        f=a.expanded_form('377460','2024-03-14','2024-03-27',1)
        self.assertTrue(all(not k.startswith('disclosureTypeArr') for k in f))
    def test_18_original_form_retained_late_report_empty(self):
        self.assertEqual(a.expanded_form('418250','2024-04-03','2024-04-17',1)['lastReport'],'')
    def test_19_release_title_candidate(self):
        self.assertEqual(a.candidate_class('주권매매거래정지해제(해제)'), 'RELEASE_TITLE_CANDIDATE')
    def test_20_period_change_not_release(self):
        self.assertEqual(a.candidate_class('주권매매거래정지기간변경(투자자보호)'),
                         'HALT_PERIOD_CHANGE_CONTEXT_NOT_RELEASE')
        self.assertEqual(a.candidate_class('주권매매거래정지기간변경(거래정지해제 예정)'),
                         'HALT_PERIOD_CHANGE_CONTEXT_NOT_RELEASE')
    def test_21_cancellation_title_candidate(self):
        self.assertEqual(a.candidate_class('주권매매거래정지 취소'),'HALT_CANCELLATION_TITLE_CANDIDATE')
    def test_22_correction_title_candidate(self):
        self.assertEqual(a.candidate_class('정정신고(보고)'), 'CORRECTION_TITLE_CANDIDATE')
    def test_23_context_not_verified_release(self):
        self.assertEqual(a.candidate_class('기타시장안내(상장폐지 관련)'),'MARKET_REVIEW_CONTEXT_CANDIDATE')
    def test_24_unrelated_title_out_of_scope(self):
        self.assertEqual(a.candidate_class('임원 임기 변경'),'UNRELATED_TITLE_IN_BOUNDED_QUERY')
    def test_25_offline_does_not_call_provider(self):
        with tempfile.TemporaryDirectory() as d:
            r=a.run(FIX,d,'offline',provider=lambda form: (_ for _ in ()).throw(AssertionError('network')))
            self.assertEqual(r['targeted_requests_executed'],0)
            self.assertFalse(r['strategy_ready'])
    def test_26_scope_exhaustiveness_false(self):
        x=a.offline_scope(a.check_evidence(FIX));self.assertFalse(x['existing_scope']['document_versions_and_categories_exhaustive'])
    def test_27_canary_synthetic_positive_controls(self):
        with tempfile.TemporaryDirectory() as d:
            r=a.run(FIX,d,'canary',provider_for_anchor_with_extra(),pause=0)
            self.assertTrue(r['canary_queries_resolved']);self.assertEqual(r['targeted_requests_executed'],8)
            self.assertEqual(r['observed_existing_57_overlap'],8)
    def test_28_candidate_outside_57_requires_review(self):
        with tempfile.TemporaryDirectory() as d:
            r=a.run(FIX,d,'canary',provider_for_anchor_with_extra({a.WINDOWS[2][:3]:['20240318009999']}),pause=0)
            self.assertEqual(r['potentially_missing_notice_candidates_needing_document_review'],1)
            self.assertEqual(r['confirmed_missing_original_57_document_relationships'],0)
            x=json.loads((pathlib.Path(d)/'stage7i_manual_gap_review_queue.json').read_text())
            self.assertFalse(x[0]['original_or_correction_relationship_verified'])
    def test_29_candidate_release_not_approval(self):
        with tempfile.TemporaryDirectory() as d:
            r=a.run(FIX,d,'canary',provider_for_anchor_with_extra({a.WINDOWS[5][:3]:['20240405009999']}),pause=0)
            self.assertFalse(r['daily_tradability_verified']);self.assertEqual(r['official_daily_halt_status'],'UNKNOWN')
    def test_30_unrelated_out_of_scope(self):
        with tempfile.TemporaryDirectory() as d:
            r=a.run(FIX,d,'canary',provider_for_anchor_with_extra({a.WINDOWS[0][:3]:['20231026009999']}),pause=0)
            self.assertEqual(r['unclassified_new_notice_receipts_not_excluded'],1)
    def test_31_missing_positive_control_fails_closed(self):
        with tempfile.TemporaryDirectory() as d:
            r=a.run(FIX,d,'canary',lambda _:html_for('377460',[]),pause=0)
            self.assertFalse(r['canary_queries_resolved']);self.assertEqual(r['windows_positive_controls_missed'],8)
    def test_32_network_failure_is_unknown_not_zero_missing(self):
        with tempfile.TemporaryDirectory() as d:
            r=a.run(FIX,d,'canary',lambda _: (_ for _ in ()).throw(IOError('simulated 503')),pause=0)
            self.assertFalse(r['canary_queries_resolved']);self.assertTrue(r['errors'])
    def test_33_duplicate_window_not_allowed(self):
        with mock.patch.object(a,'WINDOWS',a.WINDOWS[:7]+(a.WINDOWS[0],)):
            with self.assertRaisesRegex(RuntimeError,'DUPLICATED_SCOPE_WINDOW'):a.validate_windows(a.check_evidence(FIX)['by_receipt'])
    def test_34_raw_sha_on_saved_responses(self):
        with tempfile.TemporaryDirectory() as d:
            r=a.run(FIX,d,'canary',provider_for_anchor_with_extra(),pause=0)
            mf=json.loads((pathlib.Path(d)/'stage7i_raw_query_manifest.json').read_text())
            self.assertEqual(len(mf['files']),8)
            for x in mf['files']:
                self.assertEqual(a.sha((pathlib.Path(d)/x['raw_file']).read_bytes()),x['response_sha256'])
    def test_35_offline_never_uses_live_functions(self):
        src=inspect.getsource(a.offline_scope)
        self.assertNotIn('requests.',src);self.assertNotIn('KindProvider',src)
    def test_36_unchanged_unknown_in_canary(self):
        with tempfile.TemporaryDirectory() as d:
            r=a.run(FIX,d,'canary',provider_for_anchor_with_extra(),pause=0)
            self.assertFalse(r['all_relevant_market_action_categories_searched'])
            self.assertFalse(r['lookahead_safe_strategy_filter_approved'])



class WorkflowDirectUploadTests(unittest.TestCase):
    def workflows(self):
        import yaml
        p=ROOT/'.github/workflows/stockhunter_pit_stage7i_missing_notice.yml'
        self.assertTrue(p.is_file())
        return yaml.safe_load(p.read_text(encoding='utf-8'))
    def test_37_root_yml_copy_identical(self):
        a=(ROOT/'.github/workflows/stockhunter_pit_stage7i_missing_notice.yml').read_bytes()
        b=(ROOT/'stockhunter_pit_stage7i_missing_notice.yml').read_bytes()
        self.assertEqual(a,b)
    def test_38_original_workflow_identity_if_present(self):
        original=ROOT/'.github/workflows/stockhunter_pit_stage3_krx_events.yml'
        if original.is_file():
            data=original.read_bytes()
            actual=hashlib.sha1(b'blob '+str(len(data)).encode()+b'\x00'+data).hexdigest()
            self.assertEqual(actual,'f46d0d2c8aa7bcfc1393bdc4d1bc838c8fa6ff5a')
        else:
            self.assertTrue((ROOT/'.github/workflows/stockhunter_pit_stage7i_missing_notice.yml').is_file())
    def test_39_only_stage7i_jobs_present(self):
        self.assertEqual(set(self.workflows()['jobs']),
            {'stage7i-offline-contracts','stage7i-bounded-category-canary'})
    def test_40_offline_dep_precedes_canary(self):
        self.assertEqual(self.workflows()['jobs']['stage7i-bounded-category-canary']['needs'],
            'stage7i-offline-contracts')
    def test_41_dispatch_research_only_stage7i(self):
        y=self.workflows()
        on=y.get('on',y.get(True))
        self.assertEqual(on['workflow_dispatch']['inputs']['audit_phase']['options'],['stage7i'])
    def test_42_permissions_read_only(self):
        self.assertEqual(self.workflows()['permissions'],{'contents':'read','actions':'read'})
    def test_43_exact_stage7h_id_and_sha(self):
        y=(ROOT/'.github/workflows/stockhunter_pit_stage7i_missing_notice.yml').read_text()
        self.assertIn('run-id: 39578003404',y)
        self.assertIn('0fb4392da16320c11671f9fd64f5582a28579d00',y)
        self.assertIn('18bee4ef59c5ce7d9cc50483973b469dca2aabeee8c53b9bc9ba320e20bbed04',y)
    def test_44_source_evidence_before_tests_and_never_live(self):
        y=(ROOT/'.github/workflows/stockhunter_pit_stage7i_missing_notice.yml').read_text()
        part=y[y.index('  stage7i-offline-contracts:'):y.index('  stage7i-bounded-category-canary:')]
        self.assertLess(part.index('Reuse only immutable Stage7-H'),part.index('Require at least 36 discovered'))
        self.assertIn('STAGE7H_EVIDENCE_DIR: stage7h-source',part)
        for k in ('LIVE_ENABLE','SCORE_RANK_ENABLE','ORDER_ENABLE'):
            self.assertIn('STOCKHUNTER_'+k+": '0'",y)


class FailureEvidenceTests(unittest.TestCase):
    def test_45_parser_failure_preserves_raw_sha(self):
        with tempfile.TemporaryDirectory() as d:
            def invalid_provider(form):
                return b'<html><body>UNRECOGNIZED_KIND_HTML'+b'Z'*150+b'</body></html>'
            r=a.run(FIX,d,'canary',invalid_provider,pause=0)
            self.assertFalse(r['canary_queries_resolved'])
            mf=json.loads((pathlib.Path(d)/'stage7i_raw_query_manifest.json').read_text())
            self.assertEqual(len(mf['files']),8)
            self.assertEqual(mf['files'][0]['state'],'RAW_CAPTURED_PENDING_PARSE')
            first=mf['files'][0]
            self.assertEqual(a.sha((pathlib.Path(d)/first['raw_file']).read_bytes()),first['response_sha256'])
    def test_46_two_page_saturation_is_not_completeness(self):
        with tempfile.TemporaryDirectory() as d:
            first=a.WINDOWS[0]
            def saturated(form):
                code=form['repIsuSrtCd'][1:]
                w=[w for w in a.WINDOWS if w[:3]==(code,form['fromDate'],form['toDate'])][0]
                if w!=first:return html_for(code,[w[3]])
                rows=[w[3]]
                i=int(form['pageIndex'])
                rows.extend('20231026'+str(i*1000+j).zfill(6) for j in range(99))
                return html_for(code,rows)
            r=a.run(FIX,d,'canary',saturated,pause=0)
            self.assertFalse(r['canary_queries_resolved'])
            self.assertTrue(any('PAGINATION_SATURATED' in x.get('detail','') for x in r['errors']))
            self.assertLessEqual(r['targeted_requests_executed'],16)

if __name__=='__main__':unittest.main()
