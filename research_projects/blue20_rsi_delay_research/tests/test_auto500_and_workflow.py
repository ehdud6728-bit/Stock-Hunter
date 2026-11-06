import csv
import hashlib
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
import pandas as pd

HERE = Path(__file__).resolve().parents[1]
ROOT = HERE.parents[1]
sys.path.insert(0,str(HERE))
sys.path.insert(0,str(ROOT/'patch_tools'))
import auto500_research
import update_existing_workflow


class TestBlue20AutomationIntegration(unittest.TestCase):
    def test_workflow_idempotency_protected(self):
        # Minimal anchored fragment, not a pretend complete GitHub file.
        x = 'on:\n  workflow_dispatch:\n    inputs:\n      study:\n        options:\n          - r1d118\n          - all\njobs:\n  trendline-research-only:\n' + update_existing_workflow.JOB_OLD+'\n'
        a=update_existing_workflow.patch_content(x)
        self.assertIn('          - blue20_rsi_delay\n',a)
        self.assertIn('  blue20-rsi-delay-research-only:',a)
        self.assertNotIn(update_existing_workflow.JOB_OLD,a)
        with self.assertRaises(ValueError):update_existing_workflow.patch_content(a)
        self.assertEqual(a.count('blue20-rsi-delay-research-only:'),1)

    def test_workflow_requires_original_sha_and_never_modifies_invalid(self):
        with tempfile.TemporaryDirectory() as t:
            p=Path(t)/'workflow.yml'
            p.write_text('never-touch-this\n')
            before=p.read_bytes()
            with self.assertRaisesRegex(ValueError,'WORKFLOW_VERSION_MISMATCH'):
                update_existing_workflow.run(p)
            self.assertEqual(p.read_bytes(),before)

    def test_input_500_identity_preflight_rejects_wrong_count(self):
        with tempfile.TemporaryDirectory() as t:
            p=Path(t);m=p/'manifest.csv'; idx=p/'market_daily.csv'
            m.write_text('code,path,sha256\n005930,ohlcv/a.csv,a\n')
            idx.write_text('market,date\nKOSPI,2023-01-01\nKOSDAQ,2023-01-01\n')
            with self.assertRaisesRegex(ValueError,'EXACT_500_MANIFEST_REQUIRED'):
                auto500_research.materialize_verified_500(m,idx,p/'out')

    def test_repo_path_untouched_by_research(self):
        self.assertNotIn('LIVE',str(auto500_research.HERE))
        self.assertEqual(auto500_research.R118_DATA,auto500_research.REPO/'research_projects/r1d118/auto_data.py')

if __name__ == '__main__':unittest.main()
