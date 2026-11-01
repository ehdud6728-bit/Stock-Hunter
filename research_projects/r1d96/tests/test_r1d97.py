import sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import pandas as pd
import numpy as np
from research_r1d97 import CASES,snapshot,asof_status,forward
from research_r1d96 import load,choose,pivot_candidates

class ThreeNewStocks(unittest.TestCase):
 def test_01_prices_predeclared_asof(self):
  vals={'039840':17030,'071840':10030,'006400':382000}
  for code,_,day in CASES:
   d=load(code)
   i=int(np.flatnonzero(d.date.eq(pd.Timestamp(day)))[0]);self.assertEqual(int(d.iloc[i].close),vals[code])
 def test_02_three_stock_ma_bb_envelope_valid(self):
  for code,_,day in CASES:
   d=load(code);v=snapshot(d,day)
   for x in ['MA112','MA224','MA448','BB20_LO','ENV20_LO','ENV40_LO']:
    self.assertTrue(np.isfinite(v[x]));self.assertGreater(v[x],0)
 def test_03_pit_pivots_confirmed(self):
  for code,_,day in CASES:
   d=load(code); t=int(np.flatnonzero(d.date.eq(pd.Timestamp(day)))[0])
   for p in pivot_candidates(d,t):self.assertLessEqual(p['confirmed_idx'],t)
 def test_04_frozen_no_lookahead_3_cases(self):
  for code,_,day in CASES:
   d=load(code);_,_,s=choose(d,day)
   t=int(np.flatnonzero(d.date.eq(pd.Timestamp(day)))[0])
   d2=d.copy()
   for col in ['open','high','low','close']:
    d2[col]=d2[col].astype(float)
    d2.loc[d2.index>t,col]=d2.loc[d2.index>t,col]*1.217
   _,_,q=choose(d2,day)
   self.assertEqual([(x['line_id'],x['tier'],x['rank_score']) for x in s],[(x['line_id'],x['tier'],x['rank_score']) for x in q])
 def test_05_no_new_breaks_at_predeclared_dates(self):
  for code,_,day in CASES:
   d=load(code);_,_,s=choose(d,day)
   self.assertFalse(any(z['new_close_break_today'] for z in s))
 def test_06_future_report_not_signal(self):
  for code,_,day in CASES:
   d=load(code);_,_,s=choose(d,day)
   a=forward(d,day,s)
   self.assertEqual(len(a),21*len(s))
   for r in a:self.assertTrue(r['source_selection_frozen_asof'])
 def test_07_old_break_distinction(self):
  d=load('071840');_,_,s=choose(d,'2024-03-04')
  prior=[z for z in s if z['kind']=='RESISTANCE' and z['prior_close_break']]
  self.assertGreaterEqual(len(prior),2)
  self.assertTrue(all(asof_status(z,'2024-03-04')=='PRIOR_BREAK_NOT_NEW' for z in prior))
 def test_08_support_already_below(self):
  d=load('071840');_,_,s=choose(d,'2024-03-04')
  self.assertTrue(any(asof_status(x,'2024-03-04')=='SUPPORT_ALREADY_BELOW' for x in s))
 def test_09_3_stocks_below_ma224(self):
  for code,_,day in CASES:self.assertTrue(snapshot(load(code),day)['below224'])
 def test_10_bands_are_not_deemed_trade_signals(self):
  d=load('071840');s=snapshot(d,'2024-03-04');self.assertLess(abs(s['close']-s['BB20_LO']),5)
  self.assertFalse(any(x for x in ['recommended_buy','profit_certified'] if x in s))
if __name__=='__main__':unittest.main()
