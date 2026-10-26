import importlib.util
import io
import os
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path

SOURCE=Path(__file__).resolve().parents[1] / 'research_formulas' / 'stockhunter_r1d26d_coverage_outcome_audit.py'
spec=importlib.util.spec_from_file_location('r1d26d_audit',SOURCE)
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)

ROOT=Path(os.environ.get('R1D26D_TEST_INPUT_DIR','/mnt/data'))
FLOW=ROOT/'StockHunter_R1D26C_GitHub_40266290863_result.zip'
EV=ROOT/'StockHunter_R1D26_GitHub_40256526209_results.zip'
PRICE=ROOT/'StockHunter_R1D25_GitHub_40250282796_results.zip'
PROTOCOL=Path(__file__).resolve().parents[1]/'R1D26D_PREDECLARED_PROTOCOL.json'

class Historical20FrozenQA(unittest.TestCase):
    def test_real_400_row_exact_calendar_and_provenance(self):
        p,grp,ed,feats,paths,src,parent=m.read_validated(PROTOCOL,FLOW,EV,PRICE)
        self.assertEqual(len(grp),20)
        self.assertTrue(all(len(v)==20 for v in grp.values()))
        self.assertEqual(len(feats),23266)
        self.assertEqual(len(paths),23266)
        self.assertFalse(src['official_value_cross_source_verified'])

    def test_results_are_descriptive_and_horizon_complete(self):
        with tempfile.TemporaryDirectory() as td:
            r=m.generate(PROTOCOL,FLOW,EV,PRICE,td)
            self.assertEqual(r['source_raw_rows'],400)
            self.assertEqual(r['frozen_signal_events'],20)
            self.assertEqual(r['D30_complete']+r['D30_censored'],20)
            self.assertEqual(r['independent_official_amount_crosscheck'],'PENDING')
            self.assertFalse(r['strategy_ready'])
            import pandas as pd
            x=pd.read_csv(Path(td)/'R1D26D_OUTLIER_LEAVE_ONE_OUT_CAUTION.csv')
            self.assertEqual(len(x),2)

    def test_reject_wrong_upstream_zip_even_if_valid_zip(self):
        with self.assertRaisesRegex(m.QAError,'PINNED_ARCHIVE_DIGEST'):
            m.read_validated(PROTOCOL,EV,EV,PRICE)

    def test_reject_modified_flow_value_and_falsified_event_date(self):
        with zipfile.ZipFile(FLOW) as orig:
            entries={k:orig.read(k) for k in orig.namelist()}
        raw=entries[m.FLOW_CSV]
        a=b'1916788275';self.assertIn(a,raw)
        entries[m.FLOW_CSV]=raw.replace(a,b'1916788276',1)
        with tempfile.TemporaryDirectory() as td:
            q=Path(td)/'changed.zip'
            with zipfile.ZipFile(q,'w') as z:
                for k,v in entries.items():z.writestr(k,v)
            # with original pinned protocol, even changing the zip is fail-closed
            with self.assertRaisesRegex(m.QAError,'PINNED_ARCHIVE_DIGEST'):
                m.read_validated(PROTOCOL,q,EV,PRICE)
            # even after formally re-pinning archive SHA, manifest CSV SHA catches tampering
            import json
            p=json.loads(PROTOCOL.read_text());p['r1d26c_zip_sha256']=m.sha(q.read_bytes())
            t=Path(td)/'test_protocol.json';t.write_text(json.dumps(p))
            with self.assertRaisesRegex(m.QAError,'UPSTREAM_MANIFEST_SHA_MISMATCH'):
                m.read_validated(t,q,EV,PRICE)

    def test_reject_wrong_asof_parent_zip(self):
        with self.assertRaisesRegex(m.QAError,'PINNED_ARCHIVE_DIGEST'):
            m.read_validated(PROTOCOL,FLOW,EV,EV)

    def test_feature_ratio_bounded_and_no_future_input(self):
        with tempfile.TemporaryDirectory() as td:
            m.generate(PROTOCOL,FLOW,EV,PRICE,td)
            import pandas as pd
            f=pd.read_csv(Path(td)/'R1D26D_20EVENTS_TRUE_FLOW_ASOF_AND_D30_EXPLORATORY.csv')
            self.assertEqual(len(f),20)
            for c in ['foreign_signed_abs_ratio20','institution_signed_abs_ratio20']:
                self.assertTrue(f[c].between(-1.0000001,1.0000001).all())
            self.assertTrue((f['pre20_last']<f['date']).all())
            self.assertEqual(f['D30_completed'].sum(),20)

if __name__=='__main__': unittest.main()
