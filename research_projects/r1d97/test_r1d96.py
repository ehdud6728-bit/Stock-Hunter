import sys,unittest
from pathlib import Path
import pandas as pd
import numpy as np
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from research_r1d96 import *

class R1D96Test(unittest.TestCase):
 @classmethod
 def setUpClass(cls):
  cls.a=load('053800');cls.c=load('220260')
 def test_01_point_in_time_confirm(self):
  d=self.a;t=int(d.index[d.date.eq(pd.Timestamp('2024-10-17'))][0])
  q=pivot_candidates(d,t)
  self.assertTrue(any(p['date']=='2024-10-15' and p['confirmed_date']=='2024-10-17' and p['kind']=='HIGH' for p in q))
 def test_02_ahn_actual_local_pivot_is_selected(self):
  r,p,s=choose(self.a,'2024-10-17')
  top=next(x for x in s if x['tier']=='SMALL' and x['kind']=='RESISTANCE')
  self.assertEqual((top['h1'],top['h2']),('2024-10-02','2024-10-15'))
 def test_03_chem_0816_future_guard(self):
  r,p,s=choose(self.c,'2024-08-19')
  self.assertFalse(any(x['date']=='2024-08-16' and x['kind']=='LOW' for x in p))
  self.assertFalse(any(x['h1']=='2024-08-05' and x['h2']=='2024-08-16' for x in s))
 def test_04_chem_0820_confirm(self):
  r,p,s=choose(self.c,'2024-08-20')
  self.assertTrue(any(x['date']=='2024-08-16' and x['confirmed_date']=='2024-08-20' and x['kind']=='LOW' for x in p))
  top=next(x for x in s if x['kind']=='SUPPORT' and x['tier']=='SMALL')
  self.assertEqual((top['h1'],top['h2']),('2024-08-05','2024-08-16'))
 def test_05_wick_structure_warning(self):
  r,_,_=choose(self.c,'2024-08-19')
  q=next(x for x in r if x['h1']=='2024-08-01' and x['h2']=='2024-08-13' and x['kind']=='RESISTANCE')
  self.assertTrue(q['resistance_structure_warning']);self.assertGreater(q['h2_upper_wick_pct'],14)
 def test_06_avoid_untrustworthy_top_chem(self):
  r,p,s=choose(self.c,'2024-08-19')
  self.assertFalse(any(x['h1']=='2024-08-01' and x['h2']=='2024-08-13' for x in s))
 def test_07_no_lookahead_ahn_selected(self):
  d=self.a.copy(deep=True).astype({c:'float64' for c in ('open','high','low','close')});u=d[d.date>'2024-10-17'].index
  d.loc[u,['open','high','low','close']]=d.loc[u,['open','high','low','close']]*1.35
  q=choose(self.a,'2024-10-17')[2];w=choose(d,'2024-10-17')[2]
  self.assertEqual([(r['line_id'],r['rank_score']) for r in q],[(r['line_id'],r['rank_score']) for r in w])
 def test_08_no_lookahead_chem_selected(self):
  d=self.c.copy(deep=True).astype({c:'float64' for c in ('open','high','low','close')});u=d[d.date>'2024-08-19'].index
  d.loc[u,'low']=d.loc[u,'low']*.6;d.loc[u,'close']=d.loc[u,'close']*1.4
  q=choose(self.c,'2024-08-19')[2];w=choose(d,'2024-08-19')[2]
  self.assertEqual([(r['line_id'],r['rank_score']) for r in q],[(r['line_id'],r['rank_score']) for r in w])
 def test_09_retest_on_later_date(self):
  selected=choose(self.a,'2024-10-17')[2];short=next(r for r in selected if r['kind']=='RESISTANCE' and r['tier']=='SMALL')
  d=self.a;i=int(d.index[d.date.eq(pd.Timestamp('2024-10-21'))][0])
  level=calc_line(short['h1_value'],short['h2_value'],short['h1_idx'],short['h2_idx'],i)
  self.assertAlmostEqual(level,52214.28571428572,delta=1)
  self.assertLess(d.loc[i,'low'],level);self.assertGreater(d.loc[i,'close'],level)
 def test_10_chem_support_fails_after_confirm(self):
  short=next(r for r in choose(self.c,'2024-08-20')[2] if r['kind']=='SUPPORT' and r['tier']=='SMALL')
  i=int(self.c.index[self.c.date.eq(pd.Timestamp('2024-08-21'))][0]);v=self.c.iloc[i]
  line=calc_line(short['h1_value'],short['h2_value'],short['h1_idx'],short['h2_idx'],i)
  self.assertLess(v.close,line)
 def test_11_consistent_band_and_ma(self):
  i=int(self.a.index[self.a.date.eq(pd.Timestamp('2024-10-21'))][0])
  x=self.a.iloc[i]
  self.assertAlmostEqual(x.MA224,self.a.close.iloc[i-223:i+1].mean())
  self.assertAlmostEqual(x.BB20_LO,self.a.close.iloc[i-19:i+1].mean()-2*self.a.close.iloc[i-19:i+1].std(ddof=0))
  self.assertAlmostEqual(x.ENV20_10_LO,.9*self.a.close.iloc[i-19:i+1].mean())
 def test_12_unique_line_ids(self):
  for c,d,asof in [('053800',self.a,'2024-10-17'),('220260',self.c,'2024-08-20')]:
   s=choose(d,asof)[2];self.assertEqual(len({r['line_id'] for r in s}),len(s))
 def test_13_slope_signs(self):
  for d,asof in [(self.a,'2024-10-17'),(self.c,'2024-08-20')]:
   for x in choose(d,asof)[2]:
    self.assertTrue(x['slope_pct_per_bar'] < 0 if x['kind']=='RESISTANCE' else x['slope_pct_per_bar'] >0)
 def test_14_pit_no_post_asof_confirm(self):
  for d,asof in [(self.a,'2024-10-17'),(self.c,'2024-08-19'),(self.c,'2024-08-20')]:
   for r in choose(d,asof)[2]: self.assertLessEqual(r['h2_confirm_date'],asof)
 def test_15_old_line_already_broken_is_not_new(self):
  s=choose(self.c,'2024-08-19')[2]
  q=next(x for x in s if x['kind']=='RESISTANCE' and x['tier']=='MIDDLE')
  self.assertTrue(q['prior_close_break'])
  self.assertFalse(q['new_close_break_today'])

if __name__=='__main__':unittest.main()
