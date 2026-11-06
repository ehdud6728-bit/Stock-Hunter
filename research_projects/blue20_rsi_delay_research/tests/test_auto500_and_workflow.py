"""Regression tests for a *directly uploaded* existing GitHub Actions workflow.

Previous versions imported `patch_tools/update_existing_workflow.py`, which was only
needed for local patch generation, not for the user-selected direct-upload workflow.
Avoid that obsolete dependency while still checking the isolated research selector.
"""
from pathlib import Path
import sys
import tempfile
import unittest
import pandas as pd

HERE = Path(__file__).resolve().parents[1]
ROOT = HERE.parents[1]
sys.path.insert(0, str(HERE))
import auto500_research

WORKFLOW = ROOT / '.github/workflows/stockhunter_r1d98_state_structure_research_only.yml'


class TestBlue20AutomationIntegration(unittest.TestCase):
    def test_direct_upload_workflow_has_single_isolated_rsi_job(self):
        yml = WORKFLOW.read_text(encoding='utf-8')
        self.assertEqual(yml.count('          - blue20_rsi_delay\n'), 1)
        self.assertEqual(yml.count('  blue20-rsi-delay-research-only:\n'), 1)
        job = yml.split('  blue20-rsi-delay-research-only:\n', 1)[1]
        self.assertIn("    if: ${{ inputs.study == 'blue20_rsi_delay' }}", job)
        self.assertIn('python -m unittest discover -s research_projects/blue20_rsi_delay_research/tests -v', job)
        self.assertIn('python -u research_projects/blue20_rsi_delay_research/auto500_research.py', job)
        self.assertIn('name: BLUE20_RSI_GC_D0_D10_500_RESEARCH_ONLY', job)
        self.assertIn('  r118-parallel500-shard:', yml)
        self.assertIn('  r118-parallel500-merge:', yml)

    def test_direct_upload_does_not_depend_on_legacy_patch_script(self):
        # The existing YML, not a local patch generator, is the execution artifact.
        yml = WORKFLOW.read_text(encoding='utf-8')
        self.assertNotIn('update_existing_workflow', yml)
        self.assertIn("if: ${{ inputs.study == 'r1d118' && inputs.r118_mode == 'auto500' }}", yml)
        self.assertIn('  trendline-research-only:', yml)
        self.assertEqual(auto500_research.HERE, HERE)

    def test_input_500_identity_preflight_rejects_wrong_count(self):
        with tempfile.TemporaryDirectory() as t:
            p = Path(t)
            m, idx = p / 'manifest.csv', p / 'market_daily.csv'
            m.write_text('code,path,sha256\n005930,ohlcv/a.csv,a\n', encoding='utf-8')
            idx.write_text('market,date\nKOSPI,2023-01-01\nKOSDAQ,2023-01-01\n', encoding='utf-8')
            with self.assertRaisesRegex(ValueError, 'EXACT_500_MANIFEST_REQUIRED'):
                auto500_research.materialize_verified_500(m, idx, p/'out')

    def test_repo_path_untouched_by_research(self):
        self.assertNotIn('LIVE', str(auto500_research.HERE))
        self.assertEqual(auto500_research.R118_DATA,
                         auto500_research.REPO/'research_projects/r1d118/auto_data.py')

if __name__ == '__main__':
    unittest.main()
