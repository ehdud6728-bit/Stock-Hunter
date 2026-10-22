import importlib.util
import pathlib
import sys
import unittest
from unittest import mock

ROOT=pathlib.Path(__file__).resolve().parents[1]
SP=importlib.util.spec_from_file_location('formula66_r1d1_blind_chart_renderer',ROOT/'research_formulas/formula66_r1d1_blind_chart_renderer.py')
M=importlib.util.module_from_spec(SP);sys.modules[SP.name]=M;SP.loader.exec_module(M)

class RendererTests(unittest.TestCase):
 def test_01_formula_balanced_selection(self):
  xs=[{'formula_index':str(i),'KRX_asof_chart_bars_available':'1','role_from_legacy_capture':'PRE_TRUE_FROM_CAPTURE','review_id':str(i)} for i in range(66)]
  self.assertEqual(len(M.choose_cases(xs)),66)
 def test_02_prefers_true(self):
  xs=[{'formula_index':'0','KRX_asof_chart_bars_available':'1','role_from_legacy_capture':r,'review_id':str(i)} for i,r in enumerate(['PRE_FALSE_CONTROL_WITHIN_CAPTURE_ONLY','PRE_TRUE_FROM_CAPTURE'])]
  self.assertEqual(M.choose_cases(xs)[0]['role_from_legacy_capture'],'PRE_TRUE_FROM_CAPTURE')
 def test_03_unavailable_chart_excluded(self):
  xs=[{'formula_index':'0','KRX_asof_chart_bars_available':'0','role_from_legacy_capture':'PRE_TRUE_FROM_CAPTURE','review_id':'0'}]
  self.assertEqual(M.choose_cases(xs),[])
 def test_04_honors_limited_sample(self):
  xs=[{'formula_index':str(i),'KRX_asof_chart_bars_available':'1','role_from_legacy_capture':'PRE_TRUE_FROM_CAPTURE','review_id':str(i)} for i in range(66)]
  self.assertEqual(len(M.choose_cases(xs,5)),5)
 def test_05_not_future_text_in_outputs(self):
  import inspect
  x=inspect.getsource(M.render)
  self.assertIn("ds['date']>ds['signal_date']",x)
  self.assertIn('FUTURE_BAR_IN_BLIND_CHART',x)
 def test_06_sealed_identity_not_in_image_name(self):
  import inspect
  x=inspect.getsource(M.render)
  self.assertIn("r['review_id']+'.png'",x)
  self.assertIn('pattern_label_hidden',x)
  self.assertNotIn("r['formula']+'.png'",x)

if __name__=='__main__':unittest.main()
