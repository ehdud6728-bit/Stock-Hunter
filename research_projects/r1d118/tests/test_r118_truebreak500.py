import unittest,sys
from pathlib import Path
import pandas as pd
import numpy as np
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from r118_truebreak500 import event_candidates,calculate_for_signal,validate_next_market10
from r118_causal_indicators import feat,exit_path

class TrueBreakCausalityTests(unittest.TestCase):
 def states(self):
  d=[]
  for i in range(85,94):
   event='NONE';lid='SAME_LINE';old='';level=100+0.1*(i-85)
   if i==90:event='FIRST_CLOSE_OUTSIDE_PENDING'
   if i==91:event='TWO_CLOSE_BREAK_ARCHIVE';lid='';old='SAME_LINE';level=np.nan
   d.append({'code':'000001','algorithm':'TS_BODY_WICK_PROXY','scale':'MEDIUM','kind':'UPPER',
             'idx':i,'date':f'2023-04-{i-75:02d}','event':event,'line_id':lid,
             'previous_line_id':old,'exact_level':level,'known_idx':70,'known_date':'2023-03-01',
             'close':105 if i in (90,91) else 100,'status':'PENDING_BREAK' if i==90 else 'ACTIVE'})
  return pd.DataFrame(d)
 def test_exact_original_first_and_confirm(self):
  c=event_candidates(self.states());self.assertEqual(2,len(c))
  self.assertEqual(['EARLY_FIRST_CLOSE','CONFIRMED_TWO_CLOSE'],[x['signal_kind'] for x in c])
  self.assertEqual('SAME_LINE',c[1]['line_id'])
  self.assertAlmostEqual(100.6,c[1]['line_level_at_signal'],places=4)
 def test_confirm_rejects_different_line_id(self):
  d=self.states();d.loc[d.idx==91,'previous_line_id']='SWAPPED'
  with self.assertRaisesRegex(ValueError,'CONFIRMED_NOT_LINKED'):event_candidates(d)
 def test_confirm_rejects_missing_first_pending(self):
  d=self.states();d.loc[d.idx==90,'event']='NONE'
  with self.assertRaisesRegex(ValueError,'CONFIRMED_NOT_LINKED'):event_candidates(d)
 def test_one_close_is_not_confirmed(self):
  d=self.states();d.loc[d.idx==91,'event']='NONE'
  self.assertEqual(['EARLY_FIRST_CLOSE'],[x['signal_kind'] for x in event_candidates(d)])
 def test_line_known_after_day_rejected(self):
  d=self.states();d.loc[d.idx==90,'known_idx']=91
  with self.assertRaisesRegex(ValueError,'PIT_LINE_KNOWN_AFTER_SIGNAL'):event_candidates(d)
 def test_cloud_is_not_future_looking(self):
  v=100+np.arange(120)*0.2
  d=pd.DataFrame({'open':v,'high':v+1,'low':v-1,'close':v,'amount':np.full(120,1e8),'volume':np.full(120,1000)})
  a=feat(d);d2=d.copy();d2.loc[115:,'high']+=50;d2.loc[115:,'close']+=35
  b=feat(d2)
  self.assertAlmostEqual(a.at[100,'cloud_top'],b.at[100,'cloud_top'])
  self.assertTrue(np.isnan(a.at[76,'cloud_top']))
 def test_stop_first_ambiguity(self):
  d=pd.DataFrame([{'open':100,'high':100,'low':100,'close':100,'amount':1e8,'volume':1000},
     {'open':100,'high':110,'low':90,'close':104,'amount':1e8,'volume':1000}]+
     [{'open':100,'high':100,'low':100,'close':100,'amount':1e8,'volume':1000}]*10)
  e=exit_path(d,0,3,5)
  self.assertEqual('BOTH_STOP_FIRST',e['exit_reason']);self.assertAlmostEqual(-5.25,e['exit_net_pct'])
 def test_gap_stop_fills_open(self):
  d=pd.DataFrame([{'open':100,'high':100,'low':100,'close':100},
     {'open':100,'high':102,'low':98,'close':100},
     {'open':90,'high':93,'low':89,'close':90}]+[{'open':100,'high':100,'low':100,'close':100}]*9)
  e=exit_path(d,0,3,5)
  self.assertEqual('GAP_STOP',e['exit_reason']);self.assertAlmostEqual(-10.25,e['exit_net_pct'])
 def test_market_holiday_date_must_match(self):
  z={'KOSPI':{'dates':['d0','d1','d2','d3'],'offset':{'d0':0}}}
  self.assertTrue(validate_next_market10(z,'KOSPI','d0',['d1','d2'],days=2))
  self.assertFalse(validate_next_market10(z,'KOSPI','d0',['d1','d3'],days=2))

if __name__=='__main__':unittest.main(verbosity=2)
