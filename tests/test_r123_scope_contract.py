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
  # The root-level YML is a convenience copy in the distributed patch ZIP.
  # It is NOT a GitHub Actions install requirement: only .github/workflows
  # needs to be committed. If root copy is committed, verify byte identity.
  p=Path(__file__).parents[1]
  installed=p/'.github/workflows/closing_bet_r123_success_failure_shakeout_supply_research.yml'
  visible=p/'closing_bet_r123_success_failure_shakeout_supply_research.yml'
  self.assertTrue(installed.is_file(),'R123_INSTALLED_WORKFLOW_MISSING')
  self.assertGreater(installed.stat().st_size,100,'R123_INSTALLED_WORKFLOW_EMPTY')
  if visible.exists():
   self.assertTrue(visible.is_file(),'R123_ROOT_WORKFLOW_NOT_FILE')
   self.assertEqual(sha(visible),sha(installed),'R123_OPTIONAL_ROOT_WORKFLOW_DIFFERS')
