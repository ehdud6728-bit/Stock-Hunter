"""Stage7-J recovery: no false as-of approval; exact prior artifact SHA, two calls/doc."""
from __future__ import annotations
import hashlib, importlib, json, os, pathlib, shutil, sys, tempfile, unittest
from unittest import mock

ROOT=pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'research_pit'))
r=importlib.import_module('pit_stage7j_three_residual_recovery')
j=importlib.import_module('pit_stage7j_selected_notice_fullbody')

OLD=pathlib.Path(os.environ.get('STAGE7J_OLD_BODY_DIR',''))
if not OLD.is_dir():
    # Prefer real GitHub Actions artifact in CI; fail there, local developer can
    # use supplied env path. Relative '' resolves '.' and is not a valid fixture.
    OLD=None
PREFIX=os.environ.get('STAGE7J_SOURCE_ROOT','')
SOURCES=pathlib.Path(PREFIX) if PREFIX else None

class RecoveryUnitTests(unittest.TestCase):
    def test_01_identities(self):
        self.assertEqual(r.OLD_RUN,39716439150)
        self.assertEqual(r.OLD_COMMIT,'5ed7756851bdc5968f67f8fa25dfcac4318c3ccb')
    def test_02_limited_receipts(self):
        self.assertEqual(set(r.RECOVERY_RESOLVER_RECEIPTS),{'20231120000604','20240320001600'})
        self.assertEqual(r.NO_NAME_BODY_RECEIPT,'20250813000836')
    def test_03_budget_four(self):
        self.assertEqual(r.MAX_NEW_HTTP_CALLS,4)
    def test_04_max_version_bound_covers_15(self):
        self.assertGreaterEqual(r.MAX_SHELL_OPTIONS,15)
        self.assertLess(r.MAX_SHELL_OPTIONS,100)
    def test_05_no_auto_approval(self):
        self.assertEqual(r.SCHEMA,'STOCKHUNTER_PIT_STAGE7J_3_RESIDUAL_PINNED_REPLAY_AND_TWO_RESOLVE_V1')
    def test_06_reject_empty_viewer(self):
        with self.assertRaisesRegex(RuntimeError,'VIEWER_RECEIPT_IDENTITY_NOT_VISIBLE'):
            r.selected_viewer_option(b'<html>garbage</html>','20231120000604')
    def test_07_one_selected_not_current(self):
        ac='20231120000604'; raw=(f'{ac}<option value="20231116000529|N" selected>old</option>'
            '<option value="20250324002125|Y">new</option>').encode()
        x=r.selected_viewer_option(raw,ac)
        self.assertEqual(x['chosen_content_id'],'20231116000529')
        self.assertFalse(x['selected_is_current_Y'])
        self.assertFalse(x['selected_is_historical_asof_proof'])
    def test_08_reject_duplicate_option(self):
        ac='20231120000604';raw=(f'{ac}<option value="20231116000529|N" selected></option>'
            '<option value="20231116000529|Y"></option>').encode()
        with self.assertRaisesRegex(RuntimeError,'DUPLICATE_VIEWER_CONTENT_ID'):
            r.selected_viewer_option(raw,ac)
    def test_09_reject_multiple_selected(self):
        ac='20231120000604';raw=(f'{ac}<option value="20231116000529|N" selected></option>'
            '<option value="20250324002125|Y" selected></option>').encode()
        with self.assertRaisesRegex(RuntimeError,'VIEWER_SELECTED_OR_Y_AMBIGUOUS'):
            r.selected_viewer_option(raw,ac)
    def test_10_reject_current_flag_ambiguous(self):
        ac='20231120000604';raw=(f'{ac}<option value="20231116000529|Y" selected></option>'
            '<option value="20250324002125|Y"></option>').encode()
        with self.assertRaisesRegex(RuntimeError,'VIEWER_SELECTED_OR_Y_AMBIGUOUS'):
            r.selected_viewer_option(raw,ac)
    def test_11_reject_too_many(self):
        ac='20231120000604'; raw=(ac+''.join(f'<option value="202312{z:08d}|'+('Y' if z==0 else 'N')+'"'+(' selected' if z==0 else '')+'></option>' for z in range(30))).encode()
        with self.assertRaisesRegex(RuntimeError,'VIEWER_VERSION_COUNT_OUT_OF_BOUND'):
            r.selected_viewer_option(raw,ac)
    def test_12_url_must_match_receipt(self):
        with self.assertRaisesRegex(RuntimeError,'FULL_BODY_URL_RECEIPT_IDENTITY_NOT_PROVEN'):
            j.full_body_url('https://kind.krx.co.kr/external/2025/08/13/009999/20250808000536/71185.htm',r.NO_NAME_BODY_RECEIPT)
    def test_13_no_market_orders_import(self):
        code=(ROOT/'research_pit/pit_stage7j_three_residual_recovery.py').read_text()
        self.assertNotIn('import stock_hunter_live',code)
        self.assertNotIn('place_order(',code)
    def test_14_root_readonly_workflow(self):
        import yaml
        ypath=ROOT/'.github/workflows/stockhunter_pit_stage7j_recovery.yml'
        if not ypath.exists():self.skipTest('workflow test on installed package')
        y=yaml.safe_load(ypath.read_text())
        self.assertEqual(y['permissions'],{'contents':'read','actions':'read'})
        self.assertEqual(set(y['jobs']),{'stage7j-recovery-offline','stage7j-recovery-two-resolvers'})
    def test_15_root_copy_equal(self):
        a=ROOT/'stockhunter_pit_stage7j_recovery.yml'
        b=ROOT/'.github/workflows/stockhunter_pit_stage7j_recovery.yml'
        if not a.exists():self.skipTest('workflow test on installed package')
        self.assertEqual(a.read_bytes(),b.read_bytes())
    def test_16_bounded_source_reuse(self):
        s=(ROOT/'research_pit/pit_stage7j_three_residual_recovery.py').read_text()
        self.assertIn('previously_collected_7_unchanged',s)
        self.assertIn('previously_collected_7_unchanged',s)
        self.assertIn('historical_daily_tradeability_verified',s)

@unittest.skipUnless(OLD is not None,'Requires STAGE7J_OLD_BODY_DIR')
class GenuinePreviousRunTests(unittest.TestCase):
    def test_17_partial_27_sha(self):
        obj=r.inspect_prior_partial(OLD)
        self.assertEqual(len(obj['raw']),27)
        self.assertEqual(obj['prior7_successfully_reused'],7)
    def test_18_seven_original_bodies(self):
        obj=r.inspect_prior_partial(OLD)
        self.assertEqual(sum(x['status']=='FULL_BODY_CAPTURED_IDENTITY_VERIFIED_REVIEW_ONLY' for x in obj['records']),7)
    def test_19_15_options_at_both_receipts(self):
        for ac in r.RECOVERY_RESOLVER_RECEIPTS:
            v=r.selected_viewer_option((OLD/f'raw_kind_selected/{ac}_viewer_shell.html').read_bytes(),ac)
            self.assertEqual(v['viewed_version_count'],15)
            self.assertFalse(v['selected_is_current_Y'])
    def test_20_distinct_selected_historical_options(self):
        observed=[r.selected_viewer_option((OLD/f'raw_kind_selected/{ac}_viewer_shell.html').read_bytes(),ac)['chosen_content_id'] for ac in r.RECOVERY_RESOLVER_RECEIPTS]
        self.assertEqual(observed,['20231116000529','20240315003070'])
    def test_21_prior_summary_no_promotion(self):
        obj=r.inspect_prior_partial(OLD)['summary']
        self.assertEqual(obj['official_daily_halt_status'],'UNKNOWN')
        self.assertFalse(obj['strategy_ready'])
    def test_22_name_absent_replay_preserves_uncertainty(self):
        obj=r.inspect_prior_partial(OLD)
        x=r.inspect_name_absent_body(OLD,obj,obj['plan'][-1])
        self.assertEqual(x['receipt'],'20250813000836')
        self.assertFalse(x['company_identity_corroborated_in_body'])
        self.assertFalse(x['independent_company_identity_verified'])
        self.assertFalse(x['strategy_ready'])
        self.assertTrue(x['court_case_numbers_candidate_only'])
    def test_23_manifest_missing_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            target=pathlib.Path(d)/'old';shutil.copytree(OLD,target)
            (target/'stage7j_new_raw_sha_manifest.json').unlink()
            with self.assertRaisesRegex(RuntimeError,'OLD_PARTIAL_REPORT_SHA_DRIFT'):
                r.inspect_prior_partial(target)
    def test_24_raw_tamper_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            target=pathlib.Path(d)/'old';shutil.copytree(OLD,target)
            p=target/'raw_kind_selected/20231026000746_full_body.htm'
            p.write_bytes(p.read_bytes()+b'x')
            with self.assertRaisesRegex(RuntimeError,'OLD_PARTIAL_RAW_SHA_OR_SIZE_MISMATCH'):
                r.inspect_prior_partial(target)
    def test_25_report_tamper_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            target=pathlib.Path(d)/'old';shutil.copytree(OLD,target)
            p=target/'stage7j_fail_closed_summary.json';p.write_bytes(p.read_bytes()+b' ')
            with self.assertRaisesRegex(RuntimeError,'OLD_PARTIAL_REPORT_SHA_DRIFT'):
                r.inspect_prior_partial(target)

@unittest.skipUnless(SOURCES is not None and OLD is not None,'Requires STAGE7J_SOURCE_ROOT / STAGE7J_OLD_BODY_DIR')
class GenuineEndToEndTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.paths=[SOURCES/p for p in ('7h','7i','7d','7f','7g')]
    def test_26_original_source_replay(self):
        sources=j.check_all_sources(*self.paths)
        self.assertEqual(sources['previous']['previous_172_raw_objects_sha_verified'],172)
    def test_27_full_prior_plan_match(self):
        sources=j.check_all_sources(*self.paths)
        old=r.inspect_prior_partial(OLD,j.target_plan(sources))
        self.assertEqual(len(old['plan']),10)
    def test_28_dry_run_no_network(self):
        with tempfile.TemporaryDirectory() as d:
            value=r.recover(*self.paths,OLD,d,dry_run=True,
                    provider=mock.Mock(side_effect=AssertionError('network not allowed')))
            self.assertEqual(value['recovery_network_requests'],0)
            self.assertFalse(value['strategy_ready'])

    def test_29_two_only_mock_calls(self):
        class Provider:
            def __init__(self):self.resolver=[];self.content_urls=[]
            def resolve(self,cid):
                self.resolver.append(cid)
                ac='20231120000604' if cid=='20231116000529' else '20240320001600'
                url=f'https://kind.krx.co.kr/external/{ac[:4]}/{ac[4:6]}/{ac[6:8]}/{ac[8:]}/{cid}/71185.htm'
                return ('<html><a href="'+url+'">official</a></html>').encode('utf-8')
            def content(self,url):
                self.content_urls.append(url)
                return ('<html><body>위니아에이드 회생절차개시결정 공식 원문 후보 '+('sample '*50)+'</body></html>').encode('utf-8')
        with tempfile.TemporaryDirectory() as d:
            p=Provider();value=r.recover(*self.paths,OLD,d,provider=p,pause=0)
            self.assertEqual(p.resolver,['20231116000529','20240315003070'])
            self.assertEqual(len(p.content_urls),2)
            self.assertEqual(value['raw_documents_available_count'],10)
            self.assertEqual(value['status'],'ALL_TEN_RAW_BODIES_CAPTURED_COMPANY_IDENTITY_REVIEW_REMAINS')
            self.assertEqual(value['new_http_requests_made'],4)
            self.assertFalse(value['historical_daily_tradeability_verified'])
            self.assertEqual(value['independent_company_identity_unverified_receipts'],['20250813000836'])
    def test_30_mock_body_without_issuer_is_source_only(self):
        class Provider:
            def resolve(self,cid):
                ac='20231120000604' if cid=='20231116000529' else '20240320001600'
                return f'https://kind.krx.co.kr/external/{ac[:4]}/{ac[4:6]}/{ac[6:8]}/{ac[8:]}/{cid}/71185.htm'.encode()
            def content(self,url):
                if '/2023/11/20/' in url:
                    return ('<html><body>회생절차개시결정 사실에 관한 본문 '+('증빙 맥락 '*30)+'</body></html>').encode()
                return ('<html><body>위니아에이드 회생절차개시결정 '+('맥락 '*30)+'</body></html>').encode()
        with tempfile.TemporaryDirectory() as d:
            val=r.recover(*self.paths,OLD,d,provider=Provider(),pause=0)
            self.assertEqual(val['raw_documents_available_count'],10)
            self.assertEqual(val['new_company_in_body_verified_count'],1)
            self.assertEqual(set(val['independent_company_identity_unverified_receipts']),
                             {'20231120000604','20250813000836'})
            self.assertFalse(val['strategy_ready'])

    def test_31_conflicting_issuer_must_be_rejected(self):
        class Provider:
            def resolve(self,cid):
                ac='20231120000604' if cid=='20231116000529' else '20240320001600'
                return f'https://kind.krx.co.kr/external/{ac[:4]}/{ac[4:6]}/{ac[6:8]}/{ac[8:]}/{cid}/71185.htm'.encode()
            def content(self,url):
                return ('<html><body>시큐레터 회생절차개시결정 '+('잘못 연결된 기업 '*30)+'</body></html>').encode()
        with tempfile.TemporaryDirectory() as d:
            val=r.recover(*self.paths,OLD,d,provider=Provider(),pause=0)
            self.assertEqual(val['raw_documents_available_count'],8)
            self.assertTrue(all('CONFLICTING_COMPANY_IN_BODY_NOT_SOURCE_LINKED' in e['detail'] for e in val['errors']))

    def test_32_mock_fetch_failure_captured(self):
        class Broken:
            def resolve(self,cid):raise IOError('simulated KIND 503')
        with tempfile.TemporaryDirectory() as d:
            x=r.recover(*self.paths,OLD,d,provider=Broken(),pause=0)
            self.assertEqual(x['status'],'RESIDUAL_BODY_RECOVERY_INCOMPLETE_FAIL_CLOSED')
            self.assertEqual(x['raw_documents_available_count'],8)
            self.assertFalse(x['strategy_ready'])

if __name__=='__main__':unittest.main()
