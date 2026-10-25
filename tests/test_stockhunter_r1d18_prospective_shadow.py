import csv, io, json, tempfile, unittest, zipfile
from pathlib import Path
from research_formulas import stockhunter_r1d18_prospective_shadow as m

ROOT=Path(__file__).resolve().parents[1]
P=ROOT/'R1D18_PREDECLARED_PROTOCOL.json'

def row(code='0088M0', rank='1', ma='True', sh='True'):
    return dict(code=code,rank=rank,name='테스트',research_level='🟢 연구흐름 좋음',ma_compression=ma,shallow_pullback=sh,gradual_accumulation='True',liquidity_retained='False',strong_wave1='True',research_only='1',production_changed='0',ma224_phase='BELOW')

def dataset(root,rows=None,date='2026-10-26',src_ready=True,capture_time='2026-10-26T17:40:00+09:00'):
    root=Path(root)
    for member,obj in {
        m.SOURCE_META:{'signal_date':date,'capture_status':'CAPTURED_NONEMPTY','source_quality':'READY' if src_ready else 'BLOCKED','trust_eligible':1,'postrun_guard_pass':1,'captured_at_kst':capture_time},
        m.POST_GUARD:{'signal_date':date,'pass':1,'trust_eligible':1},
        m.OVERLAY_META:{'research_id':'REAL_FULL_RESEARCH_OVERLAY_R1','rows':len(rows or [row()]),'research_only':True,'production_search_changed':False,'production_score_changed':False,'production_rank_changed':False,'production_order_changed':False,'same_sample_retuning':False},
    }.items():
        file=root/member;file.parent.mkdir(parents=True,exist_ok=True);file.write_text(json.dumps(obj),encoding='utf8')
    file=root/m.MAIN_OVERLAY;file.parent.mkdir(parents=True,exist_ok=True)
    r=rows if rows is not None else [row()]
    with file.open('w',newline='',encoding='utf8') as f:
        w=csv.DictWriter(f,fieldnames=list(r[0]));w.writeheader();w.writerows(r)
    return root

def provenance():
    return {'workflow_path':'.github/workflows/run_scanner.yml','conclusion':'success','run_id':123,'head_sha':'a'*40,'artifact_id':456,'artifact_name':'stockhunter-causal-universe-full-denom-pattern-oos-exit-reports','artifact_sha256':'b'*64}

class R1D18Tests(unittest.TestCase):
    def setUp(self):self.p=m.load_protocol(P)
    def test_01_lock_and_date(self):self.assertEqual(self.p['effective_signal_date_min'],'2026-10-26')
    def test_02_no_future_lookahead(self):self.assertTrue(all(self.p['research_constraints'].values()))
    def test_03_fixed_treatment(self):self.assertEqual(self.p['treatment_primary'],'ma_compression AND shallow_pullback')
    def test_04_groups(self):self.assertEqual(len({m.group(x,y) for x in (0,1) for y in (0,1)}),4)
    def test_05_correct_combo(self):self.assertEqual(m.group(1,1),'COMPRESSION_AND_SHALLOW')
    def test_06_correct_comp_only(self):self.assertEqual(m.group(1,0),'COMPRESSION_ONLY')
    def test_07_correct_shallow_only(self):self.assertEqual(m.group(0,1),'SHALLOW_ONLY')
    def test_08_correct_neither(self):self.assertEqual(m.group(0,0),'NEITHER')
    def test_09_alphanumeric_identity(self):self.assertEqual(m.exact_code('0088M0'),'0088M0')
    def test_10_no_lossy_alias(self):
        with self.assertRaises(ValueError):m.exact_code('0088M')
    def test_11_no_zero_padding(self):
        with self.assertRaises(ValueError):m.exact_code('00088')
    def test_12_no_fake_legacy_code(self):self.assertNotEqual(m.exact_code('0088M0'),m.exact_code('000880'))
    def test_13_bool_parse_true(self):self.assertTrue(m.as_bool('TRUE'))
    def test_14_bool_parse_false(self):self.assertFalse(m.as_bool('False'))
    def test_15_missing_bool_fail(self):
        with self.assertRaises(ValueError):m.as_bool('nan')
    def test_16_ambiguous_grade_fails(self):
        with self.assertRaises(ValueError):m.grade('연구흐름 좋음')
    def test_17_green_grade(self):self.assertEqual(m.grade('🟢 연구흐름 좋음'),'GREEN')
    def test_18_source_not_scanner(self):
        r=provenance();r['workflow_path']='.github/workflows/static.yml'
        with self.assertRaises(AssertionError):m.validate_provenance(r,self.p)
    def test_19_source_failed(self):
        r=provenance();r['conclusion']='failure'
        with self.assertRaises(AssertionError):m.validate_provenance(r,self.p)
    def test_20_capture_complete(self):
        with tempfile.TemporaryDirectory() as t:
            source=dataset(Path(t)/'src');p=Path(t)/'pv.json';m.dump(p,provenance());v=m.capture(source,P,'2026-10-26',p,Path(t)/'out')
            self.assertEqual(v['signals'],1);self.assertFalse(v['oos_performance_confirmed']);self.assertIsNone(v['success_rate']);self.assertEqual(v['groups']['COMPRESSION_AND_SHALLOW'],1)
            with (Path(t)/'out/R1D18_NEW_SIGNAL_SHADOW_CAPTURE.csv').open(encoding='utf-8') as stream: f=list(csv.DictReader(stream))
            self.assertEqual(f[0]['code'],'0088M0');self.assertEqual(f[0]['outcome_status'],'PENDING_UNOBSERVED')
    def test_21_old_signal_forbidden(self):
        with tempfile.TemporaryDirectory() as t:
            with self.assertRaises(AssertionError):m.frozen_overlay(dataset(Path(t),date='2026-10-23'),'2026-10-23',self.p)
    def test_22_future_date_mismatch(self):
        with tempfile.TemporaryDirectory() as t:
            with self.assertRaises(AssertionError):m.frozen_overlay(dataset(Path(t)),'2026-10-27',self.p)
    def test_23_source_guard_fail_closed(self):
        with tempfile.TemporaryDirectory() as t:
            with self.assertRaises(AssertionError):m.frozen_overlay(dataset(Path(t),src_ready=False),'2026-10-26',self.p)
    def test_24_preclose_capture_forbidden(self):
        with tempfile.TemporaryDirectory() as t:
            with self.assertRaises(AssertionError):m.frozen_overlay(dataset(Path(t),capture_time='2026-10-26T14:50:00+09:00'),'2026-10-26',self.p)
    def test_25_duplicate_exact_code_forbidden(self):
        with tempfile.TemporaryDirectory() as t:
            with self.assertRaises(AssertionError):m.frozen_overlay(dataset(Path(t),[row(rank='1'),row(rank='2')]),'2026-10-26',self.p)
    def test_26_duplicate_rank_forbidden(self):
        with tempfile.TemporaryDirectory() as t:
            with self.assertRaises(AssertionError):m.frozen_overlay(dataset(Path(t),[row(code='0088M0',rank='1'),row(code='000880',rank='1')]),'2026-10-26',self.p)
    def test_27_invalid_phase_forbidden(self):
        with tempfile.TemporaryDirectory() as t:
            r=row();r['ma224_phase']='broken'
            with self.assertRaises(ValueError):m.frozen_overlay(dataset(Path(t),[r]),'2026-10-26',self.p)
    def test_28_production_change_forbidden(self):
        with tempfile.TemporaryDirectory() as t:
            r=row();r['production_changed']='1'
            with self.assertRaises(AssertionError):m.frozen_overlay(dataset(Path(t),[r]),'2026-10-26',self.p)
    def test_29_source_directory_zip_identical(self):
        with tempfile.TemporaryDirectory() as t:
            p=dataset(Path(t)/'src');z=Path(t)/'src.zip'
            with zipfile.ZipFile(z,'w') as zz:
                for f in p.rglob('*'):
                    if f.is_file():zz.write(f,f.relative_to(p))
            x,*_=m.frozen_overlay(p,'2026-10-26',self.p);y,*_=m.frozen_overlay(z,'2026-10-26',self.p)
            self.assertEqual(x,y)
    def test_30_missing_source_fail_closed(self):
        with tempfile.TemporaryDirectory() as t:
            with self.assertRaises(ValueError):m.source_bytes(t,m.SOURCE_META)
    def test_31_strict_baseline_lock(self):
        q=json.loads(P.read_text());q['training_cutoff']='2026-10-27'
        with tempfile.TemporaryDirectory() as t:
            p=Path(t)/'p.json';m.dump(p,q)
            with self.assertRaises(AssertionError):m.load_protocol(p)
    def test_32_treatment_drift_fail(self):
        q=json.loads(P.read_text());q['treatment_primary']='green only'
        with tempfile.TemporaryDirectory() as t:
            p=Path(t)/'p.json';m.dump(p,q)
            with self.assertRaises(AssertionError):m.load_protocol(p)
    def test_33_no_pnl_in_capture(self):
        with tempfile.TemporaryDirectory() as t:
            p=dataset(Path(t)/'src');pv=Path(t)/'pin.json';m.dump(pv,provenance());result=m.capture(p,P,'2026-10-26',pv,Path(t)/'out')
            self.assertEqual(result['future_outcome_rows'],0);self.assertFalse(result['outcome_mature'])
    def test_34_guard_meta_row_count(self):
        with tempfile.TemporaryDirectory() as t:
            p=dataset(Path(t)/'src');z=p/m.OVERLAY_META;v=json.loads(z.read_text());v['rows']=99;m.dump(z,v)
            with self.assertRaises(AssertionError):m.frozen_overlay(p,'2026-10-26',self.p)
    def test_35_root_lab_twin(self):
        a=ROOT/'.github/workflows/stockhunter_r1d3_strong_winners_external_context.yml';b=ROOT/'stockhunter_r1d3_strong_winners_external_context.yml'
        self.assertEqual(a.read_bytes(),b.read_bytes())
    def test_36_reuses_single_lab(self):
        s=(ROOT/'.github/workflows/stockhunter_r1d3_strong_winners_external_context.yml').read_text()
        self.assertEqual(s.count('  r1d18_hypothesis_lock:\n'),1)
        self.assertEqual(s.count('  r1d18_prospective_capture:\n'),1)
        self.assertIn("!startsWith(inputs.scope, 'r1d18_')",s)
    def test_37_capture_no_network(self):
        import inspect
        s=inspect.getsource(m.capture)+inspect.getsource(m.frozen_overlay)
        self.assertNotIn('urllib',s);self.assertNotIn('subprocess',s)

if __name__=='__main__':unittest.main()
