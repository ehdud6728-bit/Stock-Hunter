"""R1-D26I frozen historical source QA (no external network; no LIVE)."""
import os, sys, unittest, tempfile, json, csv
from pathlib import Path
from unittest.mock import patch
from collections import Counter
S=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(S))
from research_formulas import stockhunter_r1d26i_holdout_failure_audit as m

class AuditQA(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        root=os.environ.get('R1D26I_TEST_INPUT_DIR','/mnt/data')
        root=Path(root)
        cls.g=Path(os.environ.get('R1D26I_TEST_G_ZIP',str(root/'StockHunter_R1D26G_GitHub_40287473949_verified.zip')))
        cls.h=Path(os.environ.get('R1D26I_TEST_H_ZIP',str(root/'StockHunter_R1D26H_GitHub_40294290940_results.zip')))
        cls.protocol=S/'R1D26I_PREDECLARED_PROTOCOL.json'
        if not cls.g.exists() or not cls.h.exists():raise unittest.SkipTest('Pinned GitHub upstream archives not available until fetch stage')
        cls.p=json.loads(cls.protocol.read_text())
    def test_real_source_6k_and_disjointness(self):
        old,new,flow,evt,off,*_=m.prepare(self.g,self.h,self.p)
        self.assertEqual([len(old),len(new),len(flow),len(evt),len(off)],[100,200,6000,300,10])
    def test_locked_old_primary_30pp(self):
        old=m.prepare(self.g,self.h,self.p)[0];d=m.extract_complete(old,'G');d=d[d.strategy==m.STRAT_D]
        z=d.groupby('inst_accel').hit15_by_d30.agg(['count','sum'])
        self.assertEqual(list(z['count']),[25,24]);self.assertEqual(list(z['sum']),[5,12])
    def test_locked_holdout_near_zero(self):
        new=m.prepare(self.g,self.h,self.p)[1];d=m.extract_complete(new,'H');d=d[d.strategy==m.STRAT_D]
        z=d.groupby('inst_accel').hit15_by_d30.agg(['count','sum'])
        self.assertEqual(list(z['count']),[51,46]);self.assertEqual(list(z['sum']),[17,16])
    def test_new_source_flags_are_computed_from_original_signed_krw(self):
        # prepare recomputes for every NEW ticker and every 20-session event.
        self.assertEqual(len(m.prepare(self.g,self.h,self.p)[2]),6000)
    def test_modified_zip_is_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            x=Path(d)/'bad.zip';x.write_bytes(self.h.read_bytes()+b'!')
            with self.assertRaisesRegex(ValueError,'UPSTREAM_ARCHIVE_SHA_DRIFT'):m.prepare(self.g,x,self.p)
    def test_correct_source_not_certified_official_without_export(self):
        off=m.prepare(self.g,self.h,self.p)[4];recon,tpl,n,status=m.official_reconcile(off)
        self.assertEqual(n,0);self.assertEqual(status,'PENDING_INDEPENDENT_EXPORT')
        self.assertEqual(len(recon),10)
        self.assertTrue((recon.verification_state=='PENDING_INDEPENDENT_KRX_OFFICIAL_EXPORT').all())
    def test_reject_false_official_export_without_trace(self):
        off=m.prepare(self.g,self.h,self.p)[4].copy()
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'x.csv';off.to_csv(p,index=False)
            with self.assertRaisesRegex(ValueError,'OFFICIAL_INPUT_SCHEMA_MISSING_official_export_sha256'):m.official_reconcile(off,p)
    def test_user_supplied_matching_amounts_not_pit(self):
        off=m.prepare(self.g,self.h,self.p)[4].copy()
        off['official_KRX_foreign_net_krw']=off.pykrx_foreign_net_krw
        off['official_KRX_institution_net_krw']=off.pykrx_institution_net_krw
        off['source_official_export_name']='unverified_user_file.csv'
        off['official_export_sha256']='a'*64
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'x.csv';off.to_csv(p,index=False)
            recon,tpl,n,status=m.official_reconcile(off,p)
            self.assertEqual(n,10)
            self.assertIn('PIT_NOT_VERIFIED',status)
    def test_matching_data_with_wrong_amount_fails_closed(self):
        off=m.prepare(self.g,self.h,self.p)[4].copy()
        off['official_KRX_foreign_net_krw']=off.pykrx_foreign_net_krw
        off['official_KRX_institution_net_krw']=off.pykrx_institution_net_krw
        off.loc[0,'official_KRX_institution_net_krw']+=1000
        off['source_official_export_name']='not_independently_audited.csv';off['official_export_sha256']='b'*64
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'x.csv';off.to_csv(p,index=False)
            recon,tpl,n,status=m.official_reconcile(off,p)
            self.assertEqual(n,9);self.assertEqual(status,'OFFICIAL_AMOUNT_MISMATCH_FAIL_CLOSED')
    def test_no_outcome_opt_or_live(self):
        self.assertTrue(self.p['no_strategy_rule_change']);self.assertFalse(self.p['prospective_OOS'])
        self.assertEqual(self.p['locked_features'],['institution_recent5_accel_pos'])
    def test_main_reproduces_signed_differences_and_hashes(self):
        with tempfile.TemporaryDirectory() as d:
            r=m.generate(self.protocol,self.g,self.h,Path(d))
            self.assertAlmostEqual(r['G_D_diff_hit15_percentage_points'],30)
            self.assertAlmostEqual(r['H_D_diff_hit15_percentage_points'],1.44928,places=3)
            self.assertEqual(len(r['file_sha256']),6)
            for f,h in r['file_sha256'].items():self.assertEqual(m.sha((Path(d)/f).read_bytes()),h)
            self.assertEqual(r['official_amount_rows_matched_if_supplied'],0)
            self.assertFalse(r['strategy_ready'])

if __name__=='__main__':unittest.main()
