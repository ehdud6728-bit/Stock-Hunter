"""Strict PIT7-J research-only source, SHA, collector, and workflow contracts."""
from __future__ import annotations
import contextlib
import csv
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import unittest
from unittest import mock

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'research_pit'))
import pit_stage7j_selected_notice_fullbody as j

SOURCE_ROOT=Path(os.environ.get('STAGE7J_EVIDENCE_ROOT',str(ROOT/'local_fixture')))
H=SOURCE_ROOT/'7h';I=SOURCE_ROOT/'7i';D=SOURCE_ROOT/'7d';F=SOURCE_ROOT/'7f';G=SOURCE_ROOT/'7g'


def evidence():return j.check_all_sources(H,I,D,F,G)


def quiet_run(*args,**kwargs):
    with contextlib.redirect_stdout(io.StringIO()):return j.execute(*args,**kwargs)


class MockOfficialKindProvider:
    """Offline only. Every body is synthetic; cannot support market claims."""
    def __init__(self,bad_receipt=None,multiple=False):
        self.bad_receipt=bad_receipt;self.multiple=multiple;self.calls=[]
    def shell(self,receipt):
        self.calls.append(('shell',receipt))
        if receipt==self.bad_receipt:raise ConnectionError('synthetic_official_endpoint_503')
        flags=f'<option value="{receipt}|Y">Latest</option>'
        if self.multiple: flags+=f'<option value="{receipt[:8]}999999|N">Prior</option>'
        return ('<html><select>'+flags+'</select></html>').encode('ascii')
    def resolve(self,cid):
        self.calls.append(('resolve',cid))
        ac=cid
        url=f'https://kind.krx.co.kr/external/{ac[:4]}/{ac[4:6]}/{ac[6:8]}/{ac[8:]}/{ac}/1.htm'
        return ('<html><a href="'+url+'">Full document</a></html>').encode('ascii')
    def content(self,url):
        self.calls.append(('content',url))
        ac=url.split('/')[-2]
        code='418250' if ac in j.PRIORITY_RECEIPTS[3:] else '377460'
        corp='시큐레터' if code=='418250' else '위니아에이드'
        text=(f'<html><body><table><tr><th>회사명</th><td>{corp}</td></tr>'
              f'<tr><th>내역</th><td>상장폐지 사유 변경 공시 맥락 증거 서류에 기재한 일반 사항</td></tr>'
              f'<tr><th>공시 번호</th><td>{ac}</td></tr></table>'
              f'<p>이 본문은 단위 테스트 목적의 합성문서이며 실제 공식 공시가 아닙니다.</p></body></html>')
        return text.encode('utf-8')


class PinnedPriorSourceTests(unittest.TestCase):
    def test_01_stage7h_receipts_57(self):self.assertEqual(len(evidence()['h']['documents']),57)
    def test_02_stage7h_incidents_three(self):self.assertEqual(len(evidence()['h']['incidents']),3)
    def test_03_stage7h_candidate_one(self):self.assertEqual(evidence()['h']['correction_pair']['original_receipt_candidate'],'20240318000555')
    def test_04_stage7i_unique_74(self):self.assertEqual(evidence()['i']['verified_7i_replayed_notices'],74)
    def test_05_stage7i_8_sha_pinned_raw(self):self.assertEqual(evidence()['i']['verified_7i_list_raw_files'],8)
    def test_06_prior_raw_172(self):self.assertEqual(evidence()['previous']['previous_172_raw_objects_sha_verified'],172)
    def test_07_prior_body_57(self):self.assertEqual(evidence()['previous']['previous_57_full_bodies_sha_verified'],57)
    def test_08_prior_shell_57(self):self.assertEqual(evidence()['previous']['previous_57_viewer_shells_sha_verified'],57)
    def test_09_original_three_lanes(self):self.assertEqual(evidence()['previous']['prior_document_lane_counts'],{'7D':10,'7F':11,'7G':36})
    def test_10_exact_six_outside_cohort(self):
        r=evidence()['i']['rows']
        self.assertEqual({x['receipt'] for x in r if x['coverage_state']=='POSSIBLE_MISSING'},set(j.PRIORITY_RECEIPTS))
    def test_11_outside_cohort_45_not_auto_promoted(self):
        r=evidence()['i']['rows']
        self.assertEqual(sum(x['coverage_state']!='ALREADY_COVERED' for x in r),45)
        self.assertTrue(all(x['official_daily_halt_status']=='UNKNOWN' for x in r))
    def test_12_four_context_present_in_39(self):
        r=evidence()['i']['by_receipt']
        for ac in j.CONTEXT_RECEIPTS:self.assertEqual(r[ac]['coverage_state'],'TITLE_ONLY_UNRESOLVED')
    def test_13_ten_distinct_receipts(self):
        self.assertEqual(len(set(j.TARGET_RECEIPTS)),10);self.assertEqual(len(j.TARGET_RECEIPTS),j.MAX_NEW_DOCUMENTS)
    def test_14_eight_original_positive_control_receipts(self):
        self.assertEqual(len(set(w[3] for w in j.prior.WINDOWS)),8)
    def test_15_exact_7i_zip_sha_manifest(self):
        for filename,expected in j.PINNED_7I_SHA.items():self.assertEqual(j.sha((I/filename).read_bytes()),expected)
    def test_16_reject_tampered_7i_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp);(p/'stage7i_fail_closed_summary.json').write_bytes((I/'stage7i_fail_closed_summary.json').read_bytes()+b' ')
            with self.assertRaisesRegex(RuntimeError,'PINNED_STAGE7I_SOURCE_SHA_MISMATCH'):
                j.verify_7i(p,evidence()['h']['documents'])
    def test_17_reject_missing_7i_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(RuntimeError,'PINNED_STAGE7I_SOURCE_SHA_MISMATCH'):
                j.verify_7i(tmp,evidence()['h']['documents'])
    def test_18_reject_tampered_prior_body(self):
        with tempfile.TemporaryDirectory() as tmp:
            f=Path(tmp)
            for original,folder in [(D,'7d'),(F,'7f'),(G,'7g')]:shutil.copytree(original,f/folder)
            target=next((f/'7d'/'raw_kind_document').glob('*_full_body.htm'))
            target.write_bytes(target.read_bytes()+b'X')
            with self.assertRaisesRegex(RuntimeError,'ORIGINAL_RAW_SHA_MISMATCH'):
                j.verify_original_172(evidence()['h']['documents'],f/'7d',f/'7f',f/'7g',j.readj(H/'stage7h_fail_closed_summary.json'))
    def test_19_require_original_manifest(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(RuntimeError,'PINNED_ORIGINAL_MANIFEST_TAMPER'):
                j.verify_original_172(evidence()['h']['documents'],tmp,F,G,j.readj(H/'stage7h_fail_closed_summary.json'))
    def test_20_offline_no_provider_network_calls(self):
        with tempfile.TemporaryDirectory() as tmp:
            x=quiet_run(H,I,D,F,G,tmp,mode='offline',provider=MockOfficialKindProvider())
            self.assertEqual(x['captured_new_document_count'],0)
            self.assertEqual(x['status'],'TEN_TARGET_SCOPE_VERIFIED_NO_NEW_BODY_FETCH')
    def test_21_offline_never_trade_approves(self):
        with tempfile.TemporaryDirectory() as tmp:
            x=quiet_run(H,I,D,F,G,tmp,mode='offline')
            for key in ('strategy_ready','full_market_pit_backtest_approved','historical_daily_tradeability_verified'):
                self.assertFalse(x[key])
            self.assertEqual(x['official_daily_halt_status'],'UNKNOWN')
    def test_22_all_ten_plan_research_only(self):
        q=j.target_plan(evidence());self.assertEqual(len(q),10)
        self.assertTrue(all(x['strategy_ready'] is False and x['body_downloaded'] is False for x in q))
    def test_23_context_and_priority_counts(self):
        q=j.target_plan(evidence());c={v:sum(x['role']==v for x in q) for v in ['SIX_MARKET_ACTION_REVIEW','FOUR_REORGANIZATION_CONTEXT']}
        self.assertEqual(c,{'SIX_MARKET_ACTION_REVIEW':6,'FOUR_REORGANIZATION_CONTEXT':4})
    def test_24_candidate_links_not_verified_by_same_day(self):
        q=j.target_plan(evidence())
        self.assertTrue(any(x['related_existing_57_near_date_count']>0 for x in q))
        self.assertTrue(all(all(r['candidate_relation_only'] is True for r in x['related_existing_57_near_date_candidates']) for x in q))


class BoundedDocumentTests(unittest.TestCase):
    def test_25_off_domain_url_blocked(self):
        with self.assertRaisesRegex(RuntimeError,'OFF_DOMAIN_FULL_BODY_URL'):
            j.full_body_url('https://evil.example/external/2024/03/20/002747/20240320002747/1.htm',j.PRIORITY_RECEIPTS[0])
    def test_26_different_receipt_path_blocked(self):
        with self.assertRaisesRegex(RuntimeError,'FULL_BODY_URL_RECEIPT_IDENTITY_NOT_PROVEN'):
            j.full_body_url('https://kind.krx.co.kr/external/2024/03/20/999999/20240320002747/1.htm',j.PRIORITY_RECEIPTS[0])
    def test_27_path_traversal_blocked(self):
        with self.assertRaisesRegex(RuntimeError,'BAD_FULL_BODY_PATH'):
            j.full_body_url('https://kind.krx.co.kr/external/2024/03/20/002747/../../20240320002747/1.htm',j.PRIORITY_RECEIPTS[0])
    def test_28_allow_same_domain_exact_receipt(self):
        ac=j.PRIORITY_RECEIPTS[0]
        u=f'https://kind.krx.co.kr/external/{ac[:4]}/{ac[4:6]}/{ac[6:8]}/{ac[8:]}/{ac}/1.htm'
        self.assertEqual(j.full_body_url(u,ac),u)
    def test_29_viewer_current_flag_not_historical(self):
        ac=j.PRIORITY_RECEIPTS[0]
        ids,flags=j.resolve_shell(f'<html><option value="{ac}|N"><option value="20240320999999|Y"></html>'.encode(),ac)
        self.assertEqual(len(ids),2)
        self.assertIn('not_asof',next(iter(flags[0].keys()-{'content_id'})))
    def test_30_missing_viewer_version_rejected(self):
        with self.assertRaisesRegex(RuntimeError,'INVALID_SHELL_CONTENT_OPTIONS'):
            j.resolve_shell(b'<html>No options</html>',j.PRIORITY_RECEIPTS[0])
    def test_31_duplicate_viewer_content_rejected(self):
        ac=j.PRIORITY_RECEIPTS[0]
        with self.assertRaisesRegex(RuntimeError,'INVALID_SHELL_CONTENT_OPTIONS'):
            j.resolve_shell(f'<option value="{ac}|Y"><option value="{ac}|N">'.encode(),ac)
    def test_32_resolver_cross_domain_blocked(self):
        with self.assertRaisesRegex(RuntimeError,'NO_OFFICIAL_FULL_BODY_URL_IN_RESOLVER'):
            j.resolve_document(b'<html>https://evil.example/external/2024/03/20/002747/20240320002747/1.htm</html>',j.PRIORITY_RECEIPTS[0])
    def test_33_document_identifies_company_and_remains_unknown(self):
        ac=j.PRIORITY_RECEIPTS[0]
        b=b'<html><body>'+('위니아에이드 관리종목 해제 검토 사유 관련 참고 사실'*12).encode('utf8')+b'</body></html>'
        x=j.document_examination(b,{'code':'377460','receipt':ac},{'20240320002749'})
        self.assertTrue(x['company_identity_corroborated']);self.assertFalse(x['actual_daily_trading_eligibility_verified'])
        self.assertEqual(x['official_daily_halt_status'],'UNKNOWN')
    def test_34_wrong_company_identity_rejected(self):
        with self.assertRaisesRegex(RuntimeError,'DOCUMENT_COMPANY_IDENTITY_UNCORROBORATED'):
            j.document_examination(b'<html><body>'+b'A'*180+b'</body></html>',{'code':'418250','receipt':'20250124000732'},set())
    def test_35_explicit_old_receipt_remains_candidate(self):
        ac=j.PRIORITY_RECEIPTS[0]
        b=('<html><body>'+('위니아에이드 정정사항 정정전 관련 기존공시 20240320002749 '*15)+'</body></html>').encode()
        x=j.document_examination(b,{'code':'377460','receipt':ac},{'20240320002749'})
        self.assertEqual(x['explicit_reference_to_existing_57_receipts'],['20240320002749'])
        self.assertFalse(x['explicit_original_correction_link_verified_complete'])
    def test_36_reorganization_does_not_imply_tradeability(self):
        ac=j.CONTEXT_RECEIPTS[0];b=('<html><body>'+('큐에이드 회생절차개시신청 및 법원 판결 사항 '*10)+'</body></html>').encode()
        x=j.document_examination(b,{'code':'377460','receipt':ac},set())
        self.assertFalse(x['strategy_ready']);self.assertFalse(x['actual_daily_trading_eligibility_verified'])
    def test_37_provider_ten_mock_bodies_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            provider=MockOfficialKindProvider()
            r=quiet_run(H,I,D,F,G,tmp,'collect',provider,pause=0)
            self.assertEqual(r['captured_new_document_count'],10)
            self.assertEqual(r['status'],'TEN_SELECTED_BODIES_COLLECTED_CHAIN_UNVERIFIED')
            self.assertEqual(r['actual_provider_calls'],30)
            self.assertFalse(r['strategy_ready'])
            self.assertEqual(len([c for c in provider.calls if c[0]=='shell']),10)
            self.assertEqual(len([c for c in provider.calls if c[0]=='content']),10)
            self.assertEqual(len(j.readj(Path(tmp)/'stage7j_selected_ten_full_document_review_RESEARCH_ONLY.json')),10)
    def test_38_mock_network_failure_retains_partial_sha_and_returns_incomplete(self):
        with tempfile.TemporaryDirectory() as tmp:
            r=quiet_run(H,I,D,F,G,tmp,'collect',MockOfficialKindProvider(bad_receipt=j.PRIORITY_RECEIPTS[2]),pause=0)
            self.assertEqual(r['unresolved_new_document_count'],1)
            self.assertEqual(r['status'],'TEN_TARGET_BODY_COLLECTION_INCOMPLETE_FAIL_CLOSED')
            self.assertFalse(r['complete_target_collection'])
            self.assertEqual(j.readj(Path(tmp)/'stage7j_new_raw_sha_manifest.json')['status'],'PARTIAL_FAIL_CLOSED')
    def test_39_mock_two_viewers_not_asof(self):
        with tempfile.TemporaryDirectory() as tmp:
            r=quiet_run(H,I,D,F,G,tmp,'collect',MockOfficialKindProvider(multiple=True),pause=0)
            self.assertEqual(len(r['viewer_multiple_version_receipts_candidate_only']),10)
            self.assertFalse(r['all_original_and_correction_versions_verified'])
    def test_40_pause_safety(self):
        with self.assertRaisesRegex(RuntimeError,'INVALID_RATE_LIMIT'):
            j.execute(H,I,D,F,G,'unused','collect',pause=99)
    def test_41_new_body_collection_capped_at_ten(self):
        self.assertEqual(j.MAX_NEW_DOCUMENTS,10);self.assertLessEqual(j.MAX_PROVIDER_CALLS,50)
    def test_42_no_order_or_strategy_imports(self):
        from inspect import getsource
        text=getsource(j)
        self.assertNotIn('place_order(',text);self.assertNotIn('submit_order(',text)
        self.assertNotIn('224ma_signal',text);self.assertNotIn('LIVE_ENABLE = True',text)


class WorkflowSafetyTests(unittest.TestCase):
    def yaml(self):
        import yaml
        f=ROOT/'.github/workflows/stockhunter_pit_stage7j_selected_notice_fullbody.yml'
        self.assertTrue(f.exists())
        return yaml.safe_load(f.read_text())
    def test_43_workflow_yml_root_copy_equal(self):
        a=(ROOT/'.github/workflows/stockhunter_pit_stage7j_selected_notice_fullbody.yml').read_bytes()
        b=(ROOT/'stockhunter_pit_stage7j_selected_notice_fullbody.yml').read_bytes()
        self.assertEqual(a,b)
    def test_44_only_two_research_jobs(self):
        self.assertEqual(set(self.yaml()['jobs']),{'stage7j-offline-contracts','stage7j-official-selected-full-bodies'})
    def test_45_offline_precedes_network(self):
        self.assertEqual(self.yaml()['jobs']['stage7j-official-selected-full-bodies']['needs'],'stage7j-offline-contracts')
    def test_46_no_live_or_write_permissions(self):
        y=self.yaml();self.assertEqual(y['permissions'],{'contents':'read','actions':'read'})
        text=(ROOT/'.github/workflows/stockhunter_pit_stage7j_selected_notice_fullbody.yml').read_text()
        for key in ('LIVE_ENABLE','SCORE_RANK_ENABLE','ORDER_ENABLE'):self.assertIn(f"STOCKHUNTER_{key}: '0'",text)
    def test_47_prior_artifact_run_pins_exact(self):
        y=(ROOT/'.github/workflows/stockhunter_pit_stage7j_selected_notice_fullbody.yml').read_text()
        for id in [39708812035,39578003404,39523929718,39541621602,39570031470]:self.assertIn(str(id),y)
        for id in [12311588532,12320020824,12331038075,12335870064,12396655109]:self.assertIn(str(id),y)
    def test_48_research_dispatch_only(self):
        y=self.yaml();self.assertEqual(y['name'],'StockHunter PIT Stage7-J Selected Official Full Bodies (RESEARCH ONLY)')
        key=y.get('on',y.get(True))
        self.assertEqual(key['workflow_dispatch']['inputs']['audit_phase']['options'],['stage7j'])
    def test_49_root_original_stage3_workflow_never_rewritten(self):
        existing=ROOT/'.github/workflows/stockhunter_pit_stage3_krx_events.yml'
        if existing.exists():
            b=existing.read_bytes();head=hashlib.sha1(b'blob '+str(len(b)).encode()+b'\x00'+b).hexdigest()
            self.assertEqual(head,'f46d0d2c8aa7bcfc1393bdc4d1bc838c8fa6ff5a')
        self.assertNotEqual('stockhunter_pit_stage3_krx_events.yml','stockhunter_pit_stage7j_selected_notice_fullbody.yml')
    def test_50_complete_raw_upload_even_on_failure(self):
        y=(ROOT/'.github/workflows/stockhunter_pit_stage7j_selected_notice_fullbody.yml').read_text()
        self.assertGreaterEqual(y.count('if: always()'),2)
        self.assertIn('stage7j-bodies/**',y)

if __name__=='__main__':unittest.main()
