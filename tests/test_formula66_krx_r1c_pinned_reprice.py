"""Strict offline regression for R1C: only PRE TRUE, only pinned historic KRX original."""
import csv
import hashlib
import json
import os
import pathlib
import re
import sys
import tempfile
import unittest
from unittest import mock
import pandas as pd

ROOT=pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from research_formulas import formula66_krx_r1c_pinned_reprice as m
from research_core224 import krx_truth_r1 as core


def sample_metadata():
    c={'id':m.CAPTURE_RUN,'conclusion':'success','head_sha':'b03252627d1dde889415492fd66093839a504bdd'}
    k={'id':m.KRX_RUN,'conclusion':'success','head_sha':'08e328c101f0774a2236c0a44cc1fdbbaa59e96e'}
    cp=[{'id':m.CAPTURE_ARTIFACT_ID,'name':'stockhunter-main7-formula66-r1-capture-capture_truth','digest':m.CAPTURE_ARTIFACT_DIGEST,'expired':False}]
    ka=[{'id':a,'name':f'stockhunter-allkrx-price-shard-{i}','digest':b,'expired':False} for i,(a,b) in m.KRX_SHARD_DIGESTS.items()]
    return dict(capture_run=c,krx_run=k,capture_artifacts=cp,krx_artifacts=ka)


class PinnedR1C(unittest.TestCase):
    def test_01_exact_selected_runs(self):
        self.assertEqual((m.CAPTURE_RUN,m.KRX_RUN),(39738384453,39295295197))
        self.assertEqual(len(m.KRX_SHARD_DIGESTS),8)
    def test_02_sources_exact_metadata_accepted(self):
        self.assertTrue(m.validate_remote_source_metadata(sample_metadata()))
    def test_03_old_run_not_accepted(self):
        obj=sample_metadata();obj['krx_run']['id']=1
        with self.assertRaisesRegex(RuntimeError,'UPSTREAM_RUN_STATUS_OR_ID_INVALID'):m.validate_remote_source_metadata(obj)
    def test_04_success_conclusion_required(self):
        obj=sample_metadata();obj['capture_run']['conclusion']='failure'
        with self.assertRaisesRegex(RuntimeError,'UPSTREAM_RUN_STATUS_OR_ID_INVALID'):m.validate_remote_source_metadata(obj)
    def test_05_no_artifact_digest_drift(self):
        obj=sample_metadata();obj['krx_artifacts'][0]['digest']='sha256:fake'
        with self.assertRaisesRegex(RuntimeError,'ARTIFACT_ID_NAME_DIGEST_MISMATCH'):m.validate_remote_source_metadata(obj)
    def test_06_no_partial_shards(self):
        obj=sample_metadata();obj['krx_artifacts'].pop()
        with self.assertRaisesRegex(RuntimeError,'ARTIFACT_ID_NAME_DIGEST_MISMATCH'):m.validate_remote_source_metadata(obj)
    def test_07_expired_snapshot_rejected(self):
        obj=sample_metadata();obj['capture_artifacts'][0]['expired']=True
        with self.assertRaisesRegex(RuntimeError,'ARTIFACT_ID_NAME_DIGEST_MISMATCH'):m.validate_remote_source_metadata(obj)
    def test_08_source_commit_pins_enforced(self):
        obj=sample_metadata();obj['krx_run']['head_sha']='unexpected'
        with self.assertRaisesRegex(RuntimeError,'UPSTREAM_KRX_HEAD_SHA_DRIFT'):m.validate_remote_source_metadata(obj)
    def test_09_root_yml_exact_copy_and_readonly(self):
        a=ROOT/'.github/workflows/stockhunter_formula66_krx_r1c_pinned_legacy_reprice.yml'
        b=ROOT/'stockhunter_formula66_krx_r1c_pinned_legacy_reprice.yml'
        self.assertEqual(a.read_bytes(),b.read_bytes())
        s=a.read_text();self.assertIn("STOCKHUNTER_LIVE_ENABLE: '0'",s)
        self.assertIn("STOCKHUNTER_ORDER_ENABLE: '0'",s)
        self.assertNotIn('contents: write',s)
    def test_10_no_new_market_provider_used(self):
        script=(ROOT/'research_formulas/formula66_krx_r1c_pinned_reprice.py').read_text()
        self.assertNotIn('finance-datareader',script.lower());self.assertNotIn('yfinance',script.lower())
        self.assertNotIn('import main7_bugfix_2',script)
        self.assertIn('NO_KRX_NATIVE_FORMULA_RESCREEN',script)
    def test_11_missing_source_pins_fail(self):
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaisesRegex(RuntimeError,'SOURCE_BLOB_MISSING_OR_DRIFT'):m.check_source_pins(d)
    def test_12_missing_capture_manifest_fail(self):
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaisesRegex(RuntimeError,'REQUIRED_CAPTURE_INPUT_MISSING'):m.read_pinned_capture(d)
    def test_13_bad_capture_source_digest_fail(self):
        with tempfile.TemporaryDirectory() as d:
            p=pathlib.Path(d)
            (p/'formula66_capture_manifest.json').write_text(json.dumps({'status':'PASS_CAPTURE_PRE_RECALC_NOT_PRICE_VERIFIED','strategy_ready':False,'krx_price_confirmed':False}))
            (p/'formula66_capture_ingest.json').write_text(json.dumps({'source_sha256':'wrong','deduped_rows':1979}))
            (p/'formula66_capture_truth_per_event_formula.csv.gz').write_bytes(b'ignored')
            with self.assertRaisesRegex(RuntimeError,'HISTORIC_SOURCE_CAPTURE_PIN_DRIFT'):m.read_pinned_capture(p)
    def test_14_wrong_capture_totals_fail(self):
        with tempfile.TemporaryDirectory() as d:
            p=pathlib.Path(d)
            (p/'formula66_capture_manifest.json').write_text(json.dumps({'status':'PASS_CAPTURE_PRE_RECALC_NOT_PRICE_VERIFIED','strategy_ready':False,'krx_price_confirmed':False,'captured_rows':1979,'formula_evaluations':130614,'captured_pre_true_total':3884,'post_only_true_total':711,'pre_mismatch':0,'condition_exceptions_logged':0}))
            (p/'formula66_capture_ingest.json').write_text(json.dumps({'source_sha256':m.HISTORIC_CAPTURE_RAW_SHA,'deduped_rows':1979}))
            (p/'formula66_capture_truth_per_event_formula.csv.gz').write_bytes(b'ignored')
            with self.assertRaisesRegex(RuntimeError,'R1B_CAPTURE_COUNTS_DRIFT'):m.read_pinned_capture(p)
    def test_15_missing_KRX_shard_fails(self):
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaisesRegex(RuntimeError,'KRX_SHARD_MANIFEST_MISSING'):m.check_krx_source(d,d+'/x')
    def test_16_8_shard_not_alter_readonly_flags(self):
        self.assertEqual(m.KRX_EXPECTED_RANGE,('2019-01-02','2026-10-12'))
        self.assertEqual(m.KRX_EXPECTED_SHARDS,8)
    def test_17_zero_hit_formulas_in_all66_report(self):
        with tempfile.TemporaryDirectory() as d:
            p=pathlib.Path(d);names=[{'formula_index':i,'formula':'Formula_'+str(i)} for i in range(66)]
            mock_rows=[{'formula_index':0,'formula':'Formula_0','horizon':5,'market_regime_hypothesis':'ALL',
                 'captured_true_rows':1,'krx_price_path_completed':0,'blocked_missing_or_corp_action':1,
                 'mean_net20bps_pct':'','median_net20bps_pct':'','win_net20bps_rate_pct':''}]
            pd.DataFrame(mock_rows).to_csv(p/'report.csv',index=False)
            q=m.export_all_66(names,p/'report.csv',p/'all.csv')
            self.assertEqual(len(q),198)
            self.assertEqual(sum(x['status']=='ZERO_PRE_HITS_FROM_13_LEGACY_DATES' for x in q),197)
            self.assertEqual(q[0]['status'],'EXPLORATORY_NON_EXECUTABLE_NEEDS_KRX_NATIVE_RESCREEN_AND_OOS')
    def test_18_two_same_formula_horizon_reports_refused(self):
        with tempfile.TemporaryDirectory() as d:
            p=pathlib.Path(d);names=[{'formula_index':i,'formula':'F'+str(i)} for i in range(66)]
            pd.DataFrame([{'formula_index':0,'horizon':5,'market_regime_hypothesis':'ALL'}]*2).to_csv(p/'a.csv',index=False)
            with self.assertRaisesRegex(RuntimeError,'REPRICE_REPORT_FORMULA_HORIZON_DUPLICATE'):
                m.export_all_66(names,p/'a.csv',p/'b.csv')
    def test_19_next_open_not_zero_volume_entry(self):
        days=[f'2025-01-{x:02}' for x in range(1,18)]
        rows=[{'code':'123456','date':d,'open':100,'high':102,'low':98,'close':100,'volume':100,'amount':5000} for d in days]
        rows[1]['volume']=0
        self.assertEqual(core.sample_outcome('123456',days[0],{'123456':rows},days)['status'],'NEXT_OPEN_NOT_TRADEABLE_OR_MISSING')
    def test_20_dont_call_extreme_jump_verified_profit(self):
        days=[f'2025-01-{x:02}' for x in range(1,18)]
        rows=[{'code':'123456','date':d,'open':200 if i>3 else 100,'high':201 if i>3 else 101,'low':199 if i>3 else 99,
             'close':200 if i>3 else 100,'volume':100,'amount':5000} for i,d in enumerate(days)]
        self.assertEqual(core.sample_outcome('123456',days[0],{'123456':rows},days)['status'],'CORPORATE_ACTION_OR_SOURCE_JUMP_REVIEW')


    def test_21_all_historical_KRX_pins_are_complete_SHA256(self):
        import re
        self.assertEqual(set(m.KRX_SHARD_DIGESTS),set(range(8)))
        self.assertEqual(len({row[0] for row in m.KRX_SHARD_DIGESTS.values()}),8)
        for shard,(artifact_id,digest) in m.KRX_SHARD_DIGESTS.items():
            with self.subTest(shard=shard):
                self.assertGreater(artifact_id,0)
                self.assertRegex(digest,r'^sha256:[0-9a-f]{64}$')

    def test_22_SHARD5_independent_GitHub_artifact_id_and_digest_pin(self):
        # GitHub Actions 39295295197 artifact 12208443687, checked against the
        # independently retrieved real GitHub artifact metadata (not a mock
        # constructed using the module under test).
        self.assertEqual(m.KRX_SHARD_DIGESTS[5],(
            12208443687,
            'sha256:eb503f7cabcc40991037092c6463437c8a8b71d43fb26fe646ea4f56885b3814'))

    def test_23_truncated_pin_rejected_before_metadata_comparison(self):
        bad=dict(m.KRX_SHARD_DIGESTS)
        bad[5]=(bad[5][0],bad[5][1][:48])
        with mock.patch.object(m,'KRX_SHARD_DIGESTS',bad):
            with self.assertRaisesRegex(RuntimeError,'MALFORMED_PINNED_KRX_SHARD_DIGEST:5'):
                m.validate_remote_source_metadata(sample_metadata())

    def test_24_official_pinned_metadata_exact_8_SHAs_and_CAPTURE(self):
        # Independently pinned GitHub artifact metadata for existing source runs;
        # protects against syntactically valid but incorrect source digests.
        expected={
          0:(12208596109,'sha256:84dd5aa62f0c9538cd9c2d0ed0540c8f74063b21be49d9ecfc81e4b1583368b5'),
          1:(12207608558,'sha256:3d372eebf915c870badf103356c3700d5fa914a32d79ce8057ef1f4c4f89295b'),
          2:(12209930511,'sha256:5c49df64ad10bafbc09dd7ea78b9377804735fda6ad861018750b396ab2a27cb'),
          3:(12209696204,'sha256:7a359f5959e2374622c40d068920adf9d695142b4a61b540d0752a8cb9bb4dd8'),
          4:(12208882046,'sha256:88b35ea4c9a8a6a0ed4b61250a8018d49ee7ba4489c7345f04012915bb60f997'),
          5:(12208443687,'sha256:eb503f7cabcc40991037092c6463437c8a8b71d43fb26fe646ea4f56885b3814'),
          6:(12211336402,'sha256:b3cc055d919c7ab92d7f27b1b3ce6d3e56df787b107af961eaa43dedb885d6cd'),
          7:(12212241289,'sha256:9563747eb8b27928ba0a0d2b7d3cf02e787acd2e9e4a656d410c286139321478'),
        }
        self.assertEqual(m.KRX_SHARD_DIGESTS,expected)


    def test_25_GitHub_both_cli_steps_launch_as_importable_module(self):
        flow=(ROOT/'.github/workflows/stockhunter_formula66_krx_r1c_pinned_legacy_reprice.yml').read_text(encoding='utf-8')
        good='python -m research_formulas.formula66_krx_r1c_pinned_reprice'
        self.assertEqual(flow.count(good),2,
                         'Both preflight and full repricing must share the same importable module entry point')
        self.assertNotIn('python research_formulas/formula66_krx_r1c_pinned_reprice.py',flow)
        self.assertIn('--mode source_preflight',flow)
        self.assertIn('--mode reprice',flow)

    def test_26_module_cli_imports_project_package_with_root_as_sys_path(self):
        import subprocess
        completed=subprocess.run(
            [sys.executable,'-m','research_formulas.formula66_krx_r1c_pinned_reprice','--help'],
            cwd=str(ROOT),capture_output=True,text=True,timeout=15)
        self.assertEqual(completed.returncode,0,completed.stderr)
        self.assertIn('source_preflight',completed.stdout)
        self.assertIn('reprice',completed.stdout)

    def test_27_script_path_form_would_have_bypassed_project_root(self):
        # Guard against a regression to the exact direct-script invocation that
        # produced ModuleNotFoundError: No module named 'research_formulas'.
        flow=(ROOT/'.github/workflows/stockhunter_formula66_krx_r1c_pinned_legacy_reprice.yml').read_text(encoding='utf-8')
        self.assertNotRegex(flow,r'(?m)^\s*python\s+research_formulas/')
        self.assertEqual(flow.count('python -m research_formulas.formula66_krx_r1c_pinned_reprice'),2)


if __name__=='__main__':unittest.main()
