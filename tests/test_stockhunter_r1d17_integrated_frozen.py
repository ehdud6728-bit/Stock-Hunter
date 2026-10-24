import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
import zipfile
from contextlib import contextmanager

from research_formulas.stockhunter_r1d17_integrated_frozen import (
    archive_reader,read_member,sha_file,source_sha,load_protocol,
    audit_repo_readonly,pick_archive
)

ROOT=Path(__file__).resolve().parents[1]
PROTO=ROOT/'R1D17_PREDECLARED_PROTOCOL.json'
SCRIPT=ROOT/'research_formulas'/'stockhunter_r1d17_integrated_frozen.py'

class TestR1D17Frozen(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.p=load_protocol(PROTO)
    def test_01_protocol_identity(self):
        self.assertEqual(self.p['protocol_id'],'STOCKHUNTER_R1D17_FROZEN_INTEGRATED_V1')
    def test_02_authority(self):
        self.assertEqual(self.p['authority'],'RESEARCH_ONLY_NOT_OOS_NO_LIVE_NO_ORDERS')
    def test_03_twelve_distinct_days(self):
        d=[x['signal_date'] for x in self.p['daily_sources']];self.assertEqual(len(d),len(set(d)))
    def test_04_fixed_date_range(self):
        self.assertEqual([x['signal_date'] for x in self.p['daily_sources']],["20261002","20261008","20261009","20261013","20261014","20261015","20261016","20261019","20261020","20261021","20261022","20261023"])
    def test_05_fixed_expected_population(self):
        self.assertEqual(self.p['expected']['labels'],180)
        self.assertEqual((self.p['expected']['GREEN'],self.p['expected']['YELLOW'],self.p['expected']['RED']),(52,115,13))
    def test_06_fixed_complete_horizon_numbers(self):
        self.assertEqual([self.p['expected'][f'horizon_{h}'] for h in (1,3,5,10)],[135,105,75,30])
    def test_07_missing_protocol_fail_closed(self):
        with self.assertRaises(FileNotFoundError):load_protocol(ROOT/'never_exist_protocol.json')
    def test_08_protocol_id_mutation_fail_closed(self):
        with tempfile.TemporaryDirectory() as td:
            x=dict(self.p);x['protocol_id']='MODIFIED';f=Path(td)/'p.json';f.write_text(json.dumps(x))
            with self.assertRaises(AssertionError):load_protocol(f)
    def test_09_protocol_authority_mutation_fail_closed(self):
        with tempfile.TemporaryDirectory() as td:
            x=dict(self.p);x['authority']='LIVE';f=Path(td)/'p.json';f.write_text(json.dumps(x))
            with self.assertRaises(AssertionError):load_protocol(f)
    def test_10_all_sha256_locks(self):
        self.assertTrue(all(len(x['zip_sha256'])==64 for x in self.p['daily_sources']+list(self.p['pinned_sources'].values())))
    def test_11_all_head_sha_locks(self):
        self.assertTrue(all(len(x['head_sha'])==40 for x in self.p['daily_sources']+list(self.p['pinned_sources'].values())))
    def test_12_no_actual_orders_in_model_source(self):
        s=SCRIPT.read_text();self.assertNotIn('place_order(',s);self.assertNotIn('requests.get(',s);self.assertNotIn('FinanceDataReader',s)
    def test_13_no_real_amount_authority_claim(self):
        self.assertIn('Close*Volume',self.p['note'])
    def test_14_report_is_not_new_oos(self):
        self.assertIn('NOT_OOS',self.p['authority']);self.assertIn('NOT_RECOMPUTED',self.p['label_policy'])
    def test_15_read_folder_source(self):
        with tempfile.TemporaryDirectory() as td:
            p=Path(td);(p/'reports').mkdir();(p/'reports'/'a.csv').write_bytes(b'a,b\n1,2\n')
            self.assertEqual(read_member(p,'reports/a.csv'),b'a,b\n1,2\n')
    def test_16_read_zip_source(self):
        with tempfile.TemporaryDirectory() as td:
            p=Path(td)/'file.zip'
            with zipfile.ZipFile(p,'w') as z:z.writestr('reports/a.csv',b'a,b\n1,2\n')
            self.assertEqual(read_member(p,'reports/a.csv'),b'a,b\n1,2\n')
    def test_17_same_zip_folder_member_bytes(self):
        with tempfile.TemporaryDirectory() as td:
            d=Path(td);(d/'reports').mkdir();(d/'reports/a.csv').write_bytes(b'abc')
            z=d/'file.zip'
            with zipfile.ZipFile(z,'w') as f:f.writestr('reports/a.csv',b'abc')
            self.assertEqual(read_member(d,'reports/a.csv'),read_member(z,'reports/a.csv'))
    def test_18_missing_member_does_not_fill_zero(self):
        with tempfile.TemporaryDirectory() as td:
            with self.assertRaises(FileNotFoundError):read_member(Path(td),'missing.csv')
    def test_19_path_traversal_rejected(self):
        with tempfile.TemporaryDirectory() as td:
            with self.assertRaises(FileNotFoundError):read_member(Path(td),'../secrets')
    def test_20_missing_archive_rejected(self):
        with self.assertRaises(FileNotFoundError):read_member(ROOT/'missing.zip','a.csv')
    def test_21_corrupt_original_archive_sha_rejected(self):
        with tempfile.TemporaryDirectory() as td:
            f=Path(td)/'f.zip';f.write_bytes(b'fake')
            with self.assertRaises(AssertionError):source_sha(f,'f'*64)
    def test_22_directory_sha_is_metadata_only(self):
        with tempfile.TemporaryDirectory() as td:
            self.assertEqual(source_sha(td,'a'*64),'a'*64)
    def test_23_pick_archive_directory(self):
        with tempfile.TemporaryDirectory() as td:
            (Path(td)/'20261002').mkdir()
            self.assertEqual(pick_archive(td,'20261002'),Path(td)/'20261002')
    def test_24_missing_git_blob_rejected(self):
        with tempfile.TemporaryDirectory() as td:
            with self.assertRaises(FileNotFoundError):audit_repo_readonly(td,{'frozen_code_blobs':{'foo.py':'a'*40}})
    def test_25_wrong_git_blob_rejected(self):
        with tempfile.TemporaryDirectory() as td:
            (Path(td)/'foo.py').write_text('hello')
            with self.assertRaises(AssertionError):audit_repo_readonly(td,{'frozen_code_blobs':{'foo.py':'a'*40}})
    def test_26_matching_git_blob_accepts(self):
        with tempfile.TemporaryDirectory() as td:
            b=b'hello';(Path(td)/'foo.py').write_bytes(b)
            sha=hashlib.sha1(f'blob {len(b)}\0'.encode()+b).hexdigest()
            self.assertTrue(audit_repo_readonly(td,{'frozen_code_blobs':{'foo.py':sha}}))
    def test_27_root_twin_is_identical(self):
        y=ROOT/'stockhunter_r1d3_strong_winners_external_context.yml'
        z=ROOT/'.github/workflows'/'stockhunter_r1d3_strong_winners_external_context.yml'
        self.assertEqual(y.read_bytes(),z.read_bytes())
    def test_28_existing_r1d16_not_mentioned_mutation(self):
        src=(ROOT/'stockhunter_r1d3_strong_winners_external_context.yml').read_text()
        self.assertNotIn('git push',src);self.assertNotIn('actions/cache/save',src)
    def test_29_no_broad_source_search_or_cache_overwrite(self):
        src=(ROOT/'stockhunter_r1d3_strong_winners_external_context.yml').read_text()
        self.assertNotIn('STOCKHUNTER_LIVE_ENABLE: \'1\'',src)
        self.assertNotIn('market-data',src.lower())
    def test_30_expected_original_v1_signals(self):
        self.assertEqual(self.p['expected']['r143_joined_seeds'],150)
        self.assertEqual(self.p['expected']['outcomes'],345)

if __name__=='__main__':unittest.main()
