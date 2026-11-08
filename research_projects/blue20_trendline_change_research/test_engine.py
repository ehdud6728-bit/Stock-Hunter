import unittest
import pandas as pd
import numpy as np
from engine import prepare,snapshot,value,wait_events,simulate
class TrendTests(unittest.TestCase):
 def data(self,n=170):
  dates=pd.date_range('2023-01-01',periods=n).strftime('%Y-%m-%d');close=100+np.sin(np.arange(n)*.8)*5-np.arange(n)*.03;return pd.DataFrame(dict(date=dates,open=close,high=close+2,low=close-2,close=close,volume=100.,amount=10000.)),pd.Index(dates)
 def test_future_does_not_repaint(self):
  d,cal=self.data();a,p=prepare(d,cal);x=snapshot(a,p,140);d.loc[141:,['high','low','close']]=[1000,1,500];b,q=prepare(d,cal);self.assertEqual(x,snapshot(b,q,140))
 def test_current_bar_does_not_change_anchors(self):
  d,cal=self.data();a,p=prepare(d,cal);x=snapshot(a,p,140)[1];d.loc[140:,['high','low','close']]=[1000,1,500];b,q=prepare(d,cal);self.assertEqual(x,snapshot(b,q,140)[1])
 def test_first_break_not_replaced_after_failure(self):
  d,cal=self.data(80);d,_=prepare(d,cal);d['atr']=1.;d['low']=90.;d['close']=90.;d.loc[[20,23],'close']=110.;line=dict(p1=100.,slope=0.,s1=0);events,_=wait_events(d,{v:i for i,v in enumerate(d.date)},cal,cal[20],line);self.assertEqual(events['C_WAIT_BREAK'],cal[20])
 def test_anchors_known_and_line_passes(self):
  d,cal=self.data();d,p=prepare(d,cal);c,ll=snapshot(d,p,140)
  self.assertTrue(len(ll)>0)
  for line in ll.values():self.assertLess(line['known_date'],d.date.iloc[140]);self.assertAlmostEqual(value(line,line['s1']),line['p1']);self.assertAlmostEqual(value(line,line['s2']),line['p2'])
 def test_unknown_history(self):
  d,cal=self.data();d,p=prepare(d,cal);self.assertEqual(snapshot(d,p,40)[0]['status'],'UNKNOWN_HISTORY_OR_GAP')
 def test_next_market_day_not_next_observed(self):
  d,cal=self.data();d,_=prepare(d,cal);d=d.drop(index=10);pos={v:i for i,v in enumerate(d.date)};self.assertEqual(simulate(d.reset_index(drop=True),pos,cal,cal[9],10)['status'],'MISSING_BAR')
 def test_close_break_exits_next_open(self):
  d,cal=self.data(80);d,_=prepare(d,cal);d['atr']=1.;d.loc[:,['open','high','close']]=[100.,101.,100.];d.loc[22,'close']=98.;d.loc[23,'open']=90.;line=dict(p1=99.5,slope=0.,s1=0)
  r=simulate(d,{v:i for i,v in enumerate(d.date)},cal,cal[20],20,line);self.assertEqual(r['status'],'SUPPORT_EXIT');self.assertEqual(r['exit_date'],cal[23]);self.assertEqual(r['exit_price'],90.)
 def test_target_before_close_break(self):
  d,cal=self.data(80);d,_=prepare(d,cal);d['atr']=1.;d.loc[:,['open','high','close']]=[100.,101.,100.];d.loc[22,['high','close']]=[125.,90.];line=dict(p1=99.5,slope=0.,s1=0);r=simulate(d,{v:i for i,v in enumerate(d.date)},cal,cal[20],20,line);self.assertEqual(r['status'],'TP')
if __name__=='__main__':unittest.main()
