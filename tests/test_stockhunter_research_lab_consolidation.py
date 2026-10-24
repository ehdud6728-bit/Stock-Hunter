from pathlib import Path
import copy, hashlib, json, subprocess, sys, unittest, zipfile
import yaml

ROOT=Path(__file__).resolve().parents[1]
WF='stockhunter_r1d3_strong_winners_external_context.yml'
PROTO=ROOT/'RESEARCH_LAB_CONSOLIDATION_PROTOCOL.json'

def read_yml(s):return yaml.load(s,Loader=yaml.BaseLoader)

def gitsha(x):return hashlib.sha1(b'blob '+str(len(x)).encode()+b'\0'+x).hexdigest()

class TestMigration(unittest.TestCase):
 @classmethod
 def setUpClass(cls):
  cls.old_paths={
   'lab':'StockHunter_R1D15_EXACT_CACHE_IDENTITY_QUARANTINE_RESEARCH_ONLY_PATCH_20261024.zip',
   'd16':'StockHunter_R1D16_REALFULL_ODOLI_PARALLEL_RESEARCH_ONLY_PATCH_20261025.zip',
   'd17':'StockHunter_R1D17_FROZEN_GREEN_INTEGRATED_GITHUB_RESEARCH_ONLY_PATCH_20261025.zip'}
  cls.original={}
  for k,name in cls.old_paths.items():
   archive=ROOT/'tests/fixtures'/name
   if not archive.is_file(): continue
   with zipfile.ZipFile(archive) as z:
    p={'lab':WF,'d16':'stockhunter_r1d16_realfull_odoli_parallel.yml','d17':'stockhunter_r1d17_frozen_green_integrated.yml'}[k]
    cls.original[k]=read_yml(z.read(p))
  cls.lab=read_yml((ROOT/'.github/workflows'/WF).read_bytes())
  cls.protocol=json.loads(PROTO.read_text())
 def test_01_matching_workflow_twins(self):self.assertEqual((ROOT/WF).read_bytes(),(ROOT/'.github/workflows'/WF).read_bytes())
 def test_02_only_manual_dispatch(self):self.assertEqual(set(self.lab['on']),{'workflow_dispatch'})
 def test_03_permissions_read_only(self):self.assertEqual(self.lab['permissions'],{'contents':'read','actions':'read'})
 def test_04_old_scopes_preserved(self):
  a=set(self.lab['on']['workflow_dispatch']['inputs']['scope']['options']);self.assertEqual(len(a),18)
  self.assertTrue(set(self.protocol['migrated_scopes']).issubset(a))
 def test_05_default_unchanged(self):self.assertEqual(self.lab['on']['workflow_dispatch']['inputs']['scope']['default'],'source_check')
 def test_06_two_parallel_independent_jobs(self):
  a=self.lab['jobs'];self.assertIn('realfull_shadow_wave_giveback',a);self.assertIn('odoli_progression_after',a)
  self.assertFalse(a['realfull_shadow_wave_giveback'].get('needs'));self.assertFalse(a['odoli_progression_after'].get('needs'))
 def test_07_realfull_scope_gate(self):self.assertEqual(self.lab['jobs']['realfull_shadow_wave_giveback']['if'],"${{ inputs.scope == 'r1d16_both' || inputs.scope == 'r1d16_realfull' }}")
 def test_08_odoli_scope_gate(self):self.assertEqual(self.lab['jobs']['odoli_progression_after']['if'],"${{ inputs.scope == 'r1d16_both' || inputs.scope == 'r1d16_odoli' }}")
 def test_09_green_scope_gate(self):self.assertEqual(self.lab['jobs']['historical_grade_repro']['if'],"${{ inputs.scope == 'r1d17_green_integrated' }}")
 def test_10_no_job_specific_order_permissions(self):
  for name in ('realfull_shadow_wave_giveback','odoli_progression_after','historical_grade_repro'):
   job=self.lab['jobs'][name]; self.assertNotIn('permissions',job)
   self.assertEqual(job['env']['STOCKHUNTER_ORDER_ENABLE'],'0');self.assertEqual(job['env']['STOCKHUNTER_LIVE_ENABLE'],'0')
 def test_11_phase1_does_not_delete(self):
  self.assertEqual(self.protocol['retirement_status'],'NOT_APPROVED_UNTIL_THREE_MIGRATED_JOBS_PASS_GITHUB')
 def test_12_no_new_oos(self):self.assertFalse(self.protocol['new_independent_oos'])
 def test_13_no_order(self):self.assertEqual(self.protocol['orders_placed'],0)
 def test_14_production_freeze(self):self.assertFalse(self.protocol['production_workflows_modified'])
 def test_15_retire_only_two_safely(self):self.assertEqual(len(self.protocol['retire_only_after_verified']),2)
 def test_16_no_caching_mutation(self):
  for name in ('realfull_shadow_wave_giveback','odoli_progression_after','historical_grade_repro'):
   steps=self.lab['jobs'][name]['steps']; self.assertFalse(any('cache/save' in str(x) for x in steps))
 def test_17_existing_jobs_unchanged(self):
  if 'lab' not in self.original:self.skipTest('source archives not bundled')
  original=self.original['lab']
  for name,job in original['jobs'].items():
   actual=copy.deepcopy(self.lab['jobs'][name])
   if name=='read_only_source_check':
    self.assertEqual(actual.pop('if'),"${{ !startsWith(inputs.scope, 'r1d16_') && inputs.scope != 'r1d17_green_integrated' }}")
   self.assertEqual(actual,job)
 def test_18_migrated_job_bodies_exact(self):
  if len(self.original)!=3:self.skipTest('source archives not bundled')
  for old_key, names in [('d16',['realfull_shadow_wave_giveback','odoli_progression_after']),('d17',['historical_grade_repro'])]:
   for name in names:
    old=copy.deepcopy(self.original[old_key]['jobs'][name]);new=copy.deepcopy(self.lab['jobs'][name])
    old.pop('if',None);new.pop('if',None)
    for step in old['steps']:
     if 'run' in step:
      step['run']=step['run'].replace('cmp -s stockhunter_r1d16_realfull_odoli_parallel.yml .github/workflows/stockhunter_r1d16_realfull_odoli_parallel.yml',f'cmp -s {WF} .github/workflows/{WF}').replace('cmp -s stockhunter_r1d17_frozen_green_integrated.yml .github/workflows/stockhunter_r1d17_frozen_green_integrated.yml',f'cmp -s {WF} .github/workflows/{WF}')
    self.assertEqual(old,new)
 def test_19_github_protocol_exact(self):
  self.assertEqual(set(self.protocol['scope_contracts']),set(self.protocol['migrated_scopes']))
 def test_20_r1d17_migration_test_tracks_new_twin(self):
  t=(ROOT/'tests/test_stockhunter_r1d17_integrated_frozen.py').read_text()
  self.assertNotIn('stockhunter_r1d17_frozen_green_integrated.yml',t)
  self.assertIn(WF,t)
 def test_21_old_source_check_isolated_from_new_scopes(self):
  self.assertEqual(self.lab['jobs']['read_only_source_check']['if'],"${{ !startsWith(inputs.scope, 'r1d16_') && inputs.scope != 'r1d17_green_integrated' }}")
 def test_22_historical_pins_retained(self):
  for k in ('realfull_shadow_wave_giveback','odoli_progression_after','historical_grade_repro'):
   s=str(self.lab['jobs'][k]);self.assertIn('digest',s);self.assertIn('R1D',s)
