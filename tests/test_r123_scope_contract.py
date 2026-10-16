import unittest
from pathlib import Path
from closebet_research.r123_success_failure_microstructure_forensics import sha, PINNED_R122_MANIFEST_SHA
class ContractTests(unittest.TestCase):
 def test_manifest_pin_fixed(self):
  self.assertEqual(len(PINNED_R122_MANIFEST_SHA),64)
 def test_workflow_research_only(self):
  p=Path(__file__).parents[1]/'.github/workflows/closing_bet_r123_success_failure_shakeout_supply_research.yml'
  s=p.read_text()
  self.assertIn('workflow_dispatch:',s);self.assertNotIn('schedule:',s);self.assertNotIn('workflow_run:',s)
  self.assertNotIn('secrets.',s);self.assertNotIn('TELEGRAM',s)
 def test_visible_workflow_copy_identical(self):
  p=Path(__file__).parents[1]
  self.assertEqual(sha(p/'closing_bet_r123_success_failure_shakeout_supply_research.yml'),
    sha(p/'.github/workflows/closing_bet_r123_success_failure_shakeout_supply_research.yml'))
