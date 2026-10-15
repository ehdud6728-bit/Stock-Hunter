import io,json,tempfile,unittest,zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import pandas as pd
import numpy as np
from closebet_research import r19_paired_chart_audit as r19
from closebet_research.r18_wave_entry_forensic import make_prewave

class R19IntegrityTests(unittest.TestCase):
    def test_pinned_source_file_corruption_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            f=Path(tmp)/'r18_all_unique_signal_features.csv';f.write_bytes(b'wrong data')
            with self.assertRaisesRegex(RuntimeError,'PINNED_ARTIFACT_SHA256_MISMATCH'):
                r19.read_one_zip(tmp,'r18_all_unique_signal_features.csv')

    def test_pinned_zip_corruption_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            z=Path(tmp)/'test.zip'
            with zipfile.ZipFile(z,'w') as f:f.writestr('r18_research_manifest.json','{}')
            with self.assertRaisesRegex(RuntimeError,'PINNED_ARTIFACT_SHA256_MISMATCH'):
                r19.read_one_zip(z,'r18_research_manifest.json')

    def test_missing_upstream_artifact_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(RuntimeError,'SOURCE_ARTIFACT_FILE_MISSING'):
                r19.read_one_zip(tmp,'r18_research_manifest.json')

    def test_matching_rejects_different_modes(self):
        loc={'2026-08-03':0,'2026-08-04':1}
        a=SimpleNamespace(signal_date='2026-08-03',code='000001',all_modes='B1',wave_setup_stage=1)
        b=SimpleNamespace(signal_date='2026-08-04',code='000002',all_modes='C',wave_setup_stage=1)
        self.assertIsNone(r19._cost(a,b,loc))

    def test_matching_rejects_same_stock(self):
        loc={'2026-08-03':0,'2026-08-04':1}
        a=SimpleNamespace(signal_date='2026-08-03',code='000001',all_modes='B1',wave_setup_stage=1)
        b=SimpleNamespace(signal_date='2026-08-04',code='000001',all_modes='B1',wave_setup_stage=1)
        self.assertIsNone(r19._cost(a,b,loc))

    def test_matching_rejects_far_apart_dates(self):
        loc={'2026-08-03':0,'2026-08-31':20}
        a=SimpleNamespace(signal_date='2026-08-03',code='000001',all_modes='B1',wave_setup_stage=1)
        b=SimpleNamespace(signal_date='2026-08-31',code='000002',all_modes='B1',wave_setup_stage=1)
        self.assertIsNone(r19._cost(a,b,loc))

    def test_mf_only_pre_signal_is_future_blind(self):
        rng=pd.date_range('2025-01-01',periods=355,freq='B')
        d=pd.DataFrame({'close':np.linspace(100,145,355),'high':np.linspace(102,148,355),
             'low':np.linspace(98,142,355),'volume':np.full(355,1000.0),
             'open':np.linspace(99,144,355),'amount':np.full(355,1_000_000.)},index=rng)
        x=make_prewave(d,340)
        d.iloc[341:,d.columns.get_loc('close')]*=2
        d.iloc[341:,d.columns.get_loc('volume')]*=99
        self.assertEqual(x,make_prewave(d,340))

    def test_no_premature_authority_claim(self):
        self.assertEqual(r19.SOURCE_R171,'38664095610')
        self.assertEqual(r19.ASOF,'2026-10-12')

if __name__=='__main__':unittest.main()
