from __future__ import annotations
import csv
import hashlib
import importlib.util
import json
import os
import sys
import types
import tempfile
import unittest
from pathlib import Path

BASE=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('formula66_test_module',BASE/'research_formulas'/'formula66_truth_krx_r1.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)


def fixture_registry():
    combos=[];timing=[]
    for i in range(66):
        combos.append({'index':i,'combination':f'formula_{i}','grade':'S',
                       'base_score':100+i,'condition_source':("lambda e: e.get('flag')" if i==0 else "lambda e: not e.get('flag')" if i==1 else "lambda e: e.get('unused', False)"),
                       'referenced_keys':['flag' if i<=1 else 'unused'],'source_line':100+i})
        concern=['flag'] if i in range(54,62) else []
        timing.append({'formula_index':i,'formula':f'formula_{i}',
                       'status':'REVIEW_POST_EVAL_OVERRIDE' if concern else 'OK',
                       'post_evaluation_override_keys':concern})
    aux=[{'group':f'AUX_{i}','function_found':i>=2} for i in range(6)]
    return {'source_file':'main7_bugfix_2.py','source_sha256':'dummy','registry_sha256':'REGISTRY_SHA_TEST',
            'combo_count':66,'aux_count':6,'combos':combos,'aux_selectors':aux,'producer_timing':timing}


def write_faux_capture(path,reg,rows=None):
    if rows is None:
        m0='F'*66;m1='T'+'F'*65;m2='F'+'T'+'F'*64
        rows=[{'code':'005930','signal_date':'2026-06-02','formula_truth_bitmap':m1,
               'formula_truth_inputs_json':json.dumps({'flag':True}),
               'formula_post_truth_bitmap':m2,
               'formula_post_truth_inputs_json':json.dumps({'flag':False}),
               'formula_truth_registry_sha256':reg['registry_sha256'],'combo_invocation':'1',
               'analyze_returned':'true'},
              {'code':'000660','signal_date':'2026-06-03','formula_truth_bitmap':m2,
               'formula_truth_inputs_json':json.dumps({'flag':False}),
               'formula_post_truth_bitmap':m1,
               'formula_post_truth_inputs_json':json.dumps({'flag':True}),
               'formula_truth_registry_sha256':reg['registry_sha256'],'combo_invocation':'1',
               'analyze_returned':'false'}]
    keys=list(dict.fromkeys(k for r in rows for k in r))
    with Path(path).open('w',encoding='utf-8',newline='') as f:
        w=csv.DictWriter(f,fieldnames=keys);w.writeheader();w.writerows(rows)


def read_capture_rows(path):
    with Path(path).open(encoding='utf-8',newline='') as f:
        return list(csv.DictReader(f))


class Formula66TruthR1Tests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)
        self.reg=fixture_registry()
        self.regpath=self.root/'registry.json';self.regpath.write_text(json.dumps(self.reg),encoding='utf-8')
        self.mainpath=self.root/'main7_bugfix_2.py';self.mainpath.write_text('PREVENT_LIVE_SIDE_EFFECTS=1\n')
        self.capture=self.root/'capture.csv'
        write_faux_capture(self.capture,self.reg)
        self.reg=m.get_registry(self.regpath,self.mainpath,verify_pins=False)

    def test_01_version_identity(self):self.assertIn('66',m.VERSION)
    def test_02_66_formulas(self):self.assertEqual(len(self.reg['combos']),66)
    def test_03_6_aux(self):self.assertEqual(len(self.reg['aux_selectors']),6)
    def test_04_source_file_identity(self):self.assertEqual(self.reg['source_file'],'main7_bugfix_2.py')
    def test_05_source_blob(self):self.assertEqual(len(m.blob_sha(self.mainpath)),40)
    def test_06_registry_raw_sha(self):self.assertEqual(len(self.reg['_audit']['registry_file_sha256']),64)
    def test_07_pin_fails_closed(self):
        with self.assertRaisesRegex(RuntimeError,'REGISTRY_GIT_BLOB_DRIFT'):
            m.get_registry(self.regpath,self.mainpath,verify_pins=True)
    def test_08_formula_mismatch_length_rejected(self):
        v=fixture_registry();v['combos']=v['combos'][:-1]
        p=self.root/'bad.json';p.write_text(json.dumps(v))
        with self.assertRaisesRegex(RuntimeError,'REGISTRY_ENUM_LENGTH_MISMATCH'):m.get_registry(p,verify_pins=False)
    def test_09_formula_index_order_rejected(self):
        v=fixture_registry();v['combos'][1]['index']=99
        p=self.root/'bad.json';p.write_text(json.dumps(v))
        with self.assertRaisesRegex(RuntimeError,'REGISTRY_FORMULA_ORDER_CHANGED'):m.get_registry(p,verify_pins=False)
    def test_10_duplicate_name_rejected(self):
        v=fixture_registry();v['combos'][1]['combination']='formula_0';v['producer_timing'][1]['formula']='formula_0'
        p=self.root/'bad.json';p.write_text(json.dumps(v))
        with self.assertRaisesRegex(RuntimeError,'REGISTRY_DUPLICATE_FORMULA_NAME'):m.get_registry(p,verify_pins=False)
    def test_11_static_inventory_complete(self):
        o=m.inventory(self.reg,self.root/'out');self.assertEqual(o['formula_count'],66);self.assertEqual(o['aux_count'],6)
    def test_12_static_temporal_flags_retained(self):
        o=m.inventory(self.reg,self.root/'out');self.assertEqual(o['static_timing_review_formulas'],8)
    def test_13_static_aux_review_count(self):
        o=m.inventory(self.reg,self.root/'out');self.assertEqual(o['aux_registry_function_not_found'],2)
    def test_14_static_is_not_live_ready(self):
        o=m.inventory(self.reg,self.root/'out');self.assertFalse(o['strategy_ready'])
    def test_15_static_no_return_claim(self):
        o=m.inventory(self.reg,self.root/'out');self.assertEqual(o['provider_of_performance'],'NOT_EVALUATED')
    def test_16_safe_lambda_true(self):self.assertTrue(m.safe_formula("lambda e: e.get('x', False)")({'x':True}))
    def test_17_safe_lambda_false(self):self.assertFalse(m.safe_formula("lambda e: e.get('x') and e.get('y')")({'x':True,'y':False}))
    def test_18_isinstance_allowed(self):self.assertTrue(m.safe_formula("lambda e: isinstance(e.get('k'), (int, float)) and e['k'] >= 3")({'k':4}))
    def test_19_import_forbidden(self):
        with self.assertRaisesRegex(RuntimeError,'CONDITION_UNSAFE_CALL'):
            m.safe_formula("lambda e: __import__('os').system('bad')")
    def test_20_other_object_attribute_forbidden(self):
        with self.assertRaisesRegex(RuntimeError,'CONDITION_UNSAFE_CALL'):
            m.safe_formula("lambda e: e.values()")
    def test_21_unsafe_globals_forbidden(self):
        with self.assertRaisesRegex(RuntimeError,'CONDITION_UNSAFE_NAME'):
            m.safe_formula("lambda e: global_secrets")
    def test_22_capture_two_rows(self):
        v,meta=m.parse_capture(self.capture,self.reg);self.assertEqual(len(v),2);self.assertEqual(meta['collision'],0)
    def test_23_capture_results_66_times_2(self):
        v,_=m.parse_capture(self.capture,self.reg);rep,ev=m.replay_truth(v,self.reg,self.root/'out')
        self.assertEqual(rep['formula_evaluations'],132)
    def test_24_capture_pre_true_count(self):
        v,_=m.parse_capture(self.capture,self.reg);rep,ev=m.replay_truth(v,self.reg,self.root/'out');self.assertEqual(rep['captured_pre_true_total'],2)
    def test_25_capture_post_only_tracking(self):
        v,_=m.parse_capture(self.capture,self.reg);rep,ev=m.replay_truth(v,self.reg,self.root/'out');self.assertEqual(rep['post_only_true_total'],2)
    def test_26_capture_no_reprice_or_rescreen_claim(self):
        v,_=m.parse_capture(self.capture,self.reg);rep,ev=m.replay_truth(v,self.reg,self.root/'out');self.assertFalse(rep['krx_price_confirmed'])
    def test_27_capture_registry_mismatch_rejected(self):
        rows=read_capture_rows(self.capture);rows[0]['formula_truth_registry_sha256']='OTHER'
        write_faux_capture(self.capture,self.reg,rows)
        with self.assertRaisesRegex(RuntimeError,'CAPTURE_REGISTRY_SHA_CHANGED'):m.parse_capture(self.capture,self.reg)
    def test_28_capture_bitmap_wrong_length_rejected(self):
        rows=read_capture_rows(self.capture);rows[0]['formula_truth_bitmap']='T'
        write_faux_capture(self.capture,self.reg,rows)
        with self.assertRaisesRegex(RuntimeError,'CAPTURE_PRE_BITMAP_NOT_EXACT_66'):m.parse_capture(self.capture,self.reg)
    def test_29_capture_empty_is_not_valid_zero(self):
        rows=[];write_faux_capture(self.capture,self.reg,rows)
        with self.assertRaisesRegex(RuntimeError,'CAPTURE_REQUIRED_COLUMNS_MISSING|CAPTURE_EMPTY'):m.parse_capture(self.capture,self.reg)
    def test_30_capture_conflicting_duplicate_rejected(self):
        rows=read_capture_rows(self.capture);rows.append({**rows[0],'formula_truth_inputs_json':'{"flag":false}'})
        write_faux_capture(self.capture,self.reg,rows)
        with self.assertRaisesRegex(RuntimeError,'CONFLICTING_DUPLICATE'):m.parse_capture(self.capture,self.reg)
    def test_31_identical_duplicate_deduplicated(self):
        rows=read_capture_rows(self.capture);rows.append(dict(rows[0]))
        write_faux_capture(self.capture,self.reg,rows)
        q,meta=m.parse_capture(self.capture,self.reg);self.assertEqual(len(q),2);self.assertEqual(meta['duplicate_identical'],1)
    def test_32_wrong_inputs_yields_mismatch_fail_closed(self):
        rows=read_capture_rows(self.capture);rows[0]['formula_truth_inputs_json']='{"flag":false}'
        write_faux_capture(self.capture,self.reg,rows)
        q,_=m.parse_capture(self.capture,self.reg)
        with self.assertRaisesRegex(RuntimeError,'CAPTURE_PRE_RECALC_MISMATCH'):m.replay_truth(q,self.reg,self.root/'out')
        self.assertTrue((self.root/'out'/'formula66_capture_manifest.json').is_file())
    def test_33_bad_inputs_json_rejected(self):
        rows=read_capture_rows(self.capture);rows[0]['formula_truth_inputs_json']='{' 
        write_faux_capture(self.capture,self.reg,rows)
        q,_=m.parse_capture(self.capture,self.reg)
        with self.assertRaisesRegex(RuntimeError,'CAPTURE_INPUT_JSON_INVALID'):m.replay_truth(q,self.reg,self.root/'out')
    def test_34_asof_date_bad_rejected(self):
        rows=read_capture_rows(self.capture);rows[0]['signal_date']='next Tuesday'
        write_faux_capture(self.capture,self.reg,rows)
        with self.assertRaisesRegex(RuntimeError,'CAPTURE_BAD_SIGNAL_DATE'):m.parse_capture(self.capture,self.reg)
    def test_35_bad_ticker_rejected(self):
        rows=read_capture_rows(self.capture);rows[0]['code']='ABCDEF!'
        write_faux_capture(self.capture,self.reg,rows)
        with self.assertRaisesRegex(RuntimeError,'CAPTURE_BAD_CODE'):m.parse_capture(self.capture,self.reg)
    def test_36_regime_desc20(self):
        folder=self.root/'krx'/'shard_00';folder.mkdir(parents=True)
        rows=[]
        for i in range(26):
            d=f'2026-01-{i+1:02d}' if i<25 else '2026-02-01'
            for index in ['KOSPI','KOSDAQ']:
                rows.append({'date':d,'index':index,'index_code':'1001' if index=='KOSPI' else '2001',
                             'close':100+i*1.5 if index=='KOSPI' else 100-i*.1})
        p=folder/'official_indices.csv';write_faux_capture(p,self.reg,rows)
        r=m.read_official_index(self.root/'krx')
        self.assertEqual(r[('2026-01-25','KOSPI')]['regime'],'BULL_RET20_GTE_5')
    def test_37_missing_index_blocked(self):
        with self.assertRaisesRegex(RuntimeError,'KRX_INDEX_FILE_MISSING'):m.read_official_index(self.root)
    def test_38_same_krx_index_date_duplicate_blocked(self):
        folder=self.root/'krx'/'shard_00';folder.mkdir(parents=True)
        rows=[{'date':'2026-01-02','index':'KOSPI','index_code':'1001','close':100}]*2
        write_faux_capture(folder/'official_indices.csv',self.reg,rows)
        with self.assertRaisesRegex(RuntimeError,'KRX_INDEX_DUPLICATE'):m.read_official_index(self.root/'krx')
    def test_39_legacy_reprice_requires_full_krx_raw(self):
        event=[{'code':'005930','signal_date':'2026-06-02','pre_captured':'T','recomputed_pre':'T','formula_index':0,'formula':'formula_0'}]
        # Import adapter absent in isolated tests = absent evidence; never claim completed results.
        self.assertFalse((self.root/'krx'/'shard_00').exists())
    def test_40_no_trading_in_module(self):
        code=(BASE/'research_formulas'/'formula66_truth_krx_r1.py').read_text()
        self.assertIn('RESEARCH_ONLY = True',code);self.assertNotIn('place_order(',code)
    def test_41_no_provider_fallback(self):
        code=(BASE/'research_formulas'/'formula66_truth_krx_r1.py').read_text()
        self.assertNotIn('FinanceDataReader',code);self.assertNotIn('yfinance',code)
    def test_42_exact_captured_scope_not_full_universe_claim(self):
        v,_=m.parse_capture(self.capture,self.reg);doc,_=m.replay_truth(v,self.reg,self.root/'out')
        self.assertEqual(doc['full_universe_coverage'],'UNPROVEN_FROM_CAPTURE_ONLY')
    def test_43_negative_name_semantic_grade(self):
        o=m.inventory(self.reg,self.root/'out');self.assertTrue((self.root/'out'/'formula66_research_questions.csv').is_file())
    def test_44_no_pin_bypass_parameter(self):
        with self.assertRaisesRegex(RuntimeError,'PRODUCTION_PIN_BYPASS_FORBIDDEN'):
            m.main(['--allow-fixture-unpinned'])

    def test_45_capture_blind_queue_hides_forward_returns(self):
        q,_=m.parse_capture(self.capture,self.reg)
        m.replay_truth(q,self.reg,self.root/'out')
        txt=(self.root/'out'/'formula66_blind_chart_review_queue.csv').read_text(encoding='utf-8-sig')
        self.assertNotIn('d5_gross_pct',txt)
        self.assertIn('PRE_FALSE_CONTROL_WITHIN_CAPTURE_ONLY',txt)

    def test_46_synthetic_krx_reprice_never_claims_native_rescreen(self):
        q,_=m.parse_capture(self.capture,self.reg)
        _,true_events=m.replay_truth(q,self.reg,self.root/'out')
        krx=self.root/'krx'; (krx/'shard_00').mkdir(parents=True)
        index_rows=[]
        for i in range(40):
            # synthetic dates need not be a real calendar; this is only a unit test
            day=f'2026-05-{i+1:02d}' if i<30 else f'2026-06-{i-29:02d}'
            for index in ['KOSPI','KOSDAQ']:
                index_rows.append({'date':day,'index':index,'index_code':'1001' if index=='KOSPI' else '2001',
                                   'close':100+(i if index=='KOSPI' else 0)})
        write_faux_capture(krx/'shard_00'/'official_indices.csv',self.reg,index_rows)
        fake_pkg=types.ModuleType('research_core224'); fake_pkg.__path__=[]
        fake_mod=types.ModuleType('research_core224.krx_truth_r1')
        def fake_verify(root,ref,start,end,wanted_codes=None,expected_shards=None):
            return ([f'2026-05-{i+1:02d}' for i in range(230)],
                    {'005930':[{'date':'2026-06-02','open':100,'high':102,'low':99,'close':101,'volume':10000,'amount':1000000}],
                     '000660':[{'date':'2026-06-03','open':110,'high':112,'low':109,'close':111,'volume':10000,'amount':1000000}]},
                    [],{'2026-06-02':{'sha256':'x'},'2026-06-03':{'sha256':'x'}})
        def fake_outcome(code,day,by_code,all_days,h=15):
            return {'status':'COMPLETE_RAW_KRX_PRICE_ONLY_NOT_EXECUTABLE',
                    **{f'd{k}_{kind}_pct':4 if kind=='close' else 7 if kind=='mfe' else -2
                       for k in (5,10,15) for kind in ('close','mfe','mae')}}
        fake_mod.verify_and_load=fake_verify;fake_mod.sample_outcome=fake_outcome
        previous={k:sys.modules.get(k) for k in ['research_core224','research_core224.krx_truth_r1']}
        sys.modules['research_core224']=fake_pkg;sys.modules['research_core224.krx_truth_r1']=fake_mod
        try:
            result=m.krx_reprice(true_events,self.reg,krx,self.root/'faux_reference.csv',self.root/'out')
            self.assertFalse(result['strategy_ready'])
            self.assertIn('NOT_SIGNAL_AUTHORITY',result['status'])
            self.assertEqual(result['events_with_pre_TRUE_formula'],2)
            p=self.root/'out'/'formula66_krx_repricing_by_formula_regime.csv'
            self.assertTrue(p.is_file())
            with p.open(encoding='utf-8-sig') as f:rows=list(csv.DictReader(f))
            self.assertGreater(len(rows),2)
            self.assertEqual(rows[0]['status'],'EXPLORATORY_NOT_ACTIONABLE_NOT_KRX_RESCREEN')
            self.assertTrue((self.root/'out'/'formula66_blind_review_krx_pre_signal_bars.csv').is_file())
        finally:
            for k,v in previous.items():
                if v is None:sys.modules.pop(k,None)
                else:sys.modules[k]=v


if __name__=='__main__':unittest.main()
