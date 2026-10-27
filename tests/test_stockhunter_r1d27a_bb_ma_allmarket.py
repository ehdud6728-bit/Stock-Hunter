import unittest,json,tempfile,csv,gzip,sys,types
from collections import deque
from pathlib import Path
import numpy as np
import pandas as pd
from research_formulas import stockhunter_r1d27a_bb_ma_allmarket as v

class D27ATests(unittest.TestCase):
 @classmethod
 def setUpClass(cls):cls.cfg=v.readcfg('R1D27A_PREDECLARED_PROTOCOL.json')
 def hist(self,c,dates=None):
  return deque((i,float(x),float(x),float(x)*1.01,float(x)*.99,float(x),10000.,3e9) for i,x in enumerate(c))
 def market(self):return {'KOSPI_ret20_pct':1.,'KOSDAQ_ret20_pct':-2.,'joint_index_20d_regime':'MIXED'}
 def test_base_periods(self):self.assertEqual(v.MA,(5,10,20,40,60,112,224,448))
 def test_bb_flat(self):
  q=v.bbstate([100]*100,20);self.assertEqual(q[0][2],0)
 def test_bb_width(self):
  x=np.array([100.0,110.0]*50);z=v.bbstate(x,20)
  self.assertAlmostEqual(z[0][2],400*5/105)
 def test_historical_gating(self):self.assertIsNone(v.asof_features(self.hist([100.]*467),466,self.market(),self.cfg))
 def test_corporate_gap_reject(self):
  x=[100.]*468;x[-5]=200.
  self.assertIsNone(v.asof_features(self.hist(x),467,self.market(),self.cfg))
 def test_nonliquid_reject(self):
  q=self.hist([100.]*468)
  self.assertIsNone(v.asof_features(q,467,self.market(),self.cfg))
 def test_contiguous_required(self):
  q=self.hist([1000.]*468);q[-3]=(10000,*q[-3][1:]);self.assertIsNone(v.asof_features(q,467,self.market(),self.cfg))
 def test_break_signal_and_ma(self):
  # Long flat history with one downward cross; setting previous beyond a 2sigma band.
  x=[1500.]*466+[1500.,1300.]
  got=v.asof_features(self.hist(x),467,self.market(),self.cfg)
  self.assertIsNotNone(got)
  f,ev=got;self.assertIn((20,'BREAK'),ev);self.assertEqual(f['BB20_break_today'],1)
  self.assertIn('MA448_slope20_pct',f)
 def test_reclaim_signal(self):
  x=[1500.]*465+[1200.,1190.,1550.]
  g=v.asof_features(self.hist(x),467,self.market(),self.cfg)
  self.assertIsNotNone(g);self.assertIn((20,'RECLAIM'),g[1])
 def test_flow_exact_dates(self):
  idx=pd.to_datetime(['2026-01-02','2026-01-05'])
  df=pd.DataFrame({'외국인합계':[10,-20],'기관합계':[-1,9]},index=idx)
  q=v.parseflow(df,['2026-01-02','2026-01-05']);self.assertEqual(q['2026-01-05'],[-20,9])
  with self.assertRaises(ValueError):v.parseflow(df,['2026-01-02'])
 def test_flow_wrong_schema(self):
  with self.assertRaises(ValueError):v.parseflow(pd.DataFrame({'외국인합계':[1]},index=pd.to_datetime(['2026-01-02'])),['2026-01-02'])
 def test_no_live(self):
  self.assertFalse(self.cfg['live_changed']);self.assertFalse(self.cfg['prospective_OOS'])
 def test_event_columns(self):
  cols=v.event_fields();self.assertEqual(len(cols),12+8*4+3*7)
 def test_binary_cross_clearly_not_touch(self):
  x=[1500.]*468
  self.assertIsNone(v.asof_features(self.hist(x),467,self.market(),self.cfg))

if __name__=='__main__':unittest.main()
