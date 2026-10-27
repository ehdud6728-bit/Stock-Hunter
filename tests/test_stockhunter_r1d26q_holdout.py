import unittest,tempfile,json,io,csv,gzip,zipfile,hashlib,sys,os
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from research_formulas.stockhunter_r1d26q_independent_holdout import *
ROOT=Path(__file__).resolve().parents[1]
P=json.loads((ROOT/'R1D26Q_PREDECLARED_PROTOCOL.json').read_text())
PILOTPATH=Path('/mnt/data/StockHunter_R1D26L_GitHub_40356487576_results.zip')
D25PATH=Path(os.environ.get('R1D26Q_D25_ZIP','/mnt/data/StockHunter_R1D25_GitHub_40250282796_results.zip'))

def make_post(n=30,final=110,stopagain=False,late=False):
 out=[]
 for i in range(n):
  day=f'2025-01-{i+1:02d}' # testing chronological order only, not actual calendar
  if i==0: o,c,lo,hi=105,99,98,106
  elif i==1:o,c,lo,hi=98,101,97,102
  elif i==2:o,c,lo,hi=102,104,101,105
  elif stopagain and i==4:o,c,lo,hi=101,98,97,102
  elif stopagain and i==5:o,c,lo,hi=96,96,94,98
  else:o,c,lo,hi=107,final,106,max(110,final+1)
  out.append({'trade_date':day,'open':o,'close':c,'low':lo,'high':hi,'volume':10000,'amount':30000000})
 return out

def s():
 return {'strategy':D,'date':'2024-12-31','code':'123456','year':'2024','support':100,'signal_close':105,'median_pre20_amount':10000000,'median_pre20_volume':10000,'asof_support_status':'VALID_AT_SIGNAL_CLOSE'}

class R1D26Q(unittest.TestCase):
 def test_selection_sha(self):self.assertEqual(sha((ROOT/FROZEN).read_bytes()),P['selection_sha256'])
 def test_balanced_and_unique(self):
  rr=selection_load(ROOT/FROZEN,P);self.assertEqual(len(rr),125);self.assertEqual(len({r['code'] for r in rr}),125)
 def test_all_D(self):self.assertTrue(all(r['strategy']==D for r in selection_load(ROOT/FROZEN,P)))
 def test_pilot_disjoint(self):
  old={r['code'] for r in read_csv_bytes((ROOT/'data/R1D26L_FROZEN_60_ASOF_EVENTS.csv').read_bytes())};new={r['code'] for r in selection_load(ROOT/FROZEN,P)};self.assertFalse(old&new)
 def test_pinned_D25_recompute_when_available(self):
  if not D25PATH.exists():self.skipTest('D25 archive only inside GitHub job')
  verify_select(D25PATH,P,ROOT/FROZEN)
 def test_mutated_selection_fail_closed(self):
  with tempfile.TemporaryDirectory() as t:
   p=Path(t)/'x.csv';p.write_bytes((ROOT/FROZEN).read_bytes()+b'\n')
   with self.assertRaises(ValueError):selection_load(p,P)
 def test_pre_support_excludes_signal(self):
  from datetime import date,timedelta
  start=date(2023,11,1);pre=[{'trade_date':(start+timedelta(days=i)).isoformat(),'high':90+i%4,'low':80,'close':90,'amount':20000000,'volume':100000} for i in range(70)]
  pre[-1]['high']=200;pre[-1]['close']=110
  rr=support_from_pre(pre,{'date':pre[-1]['trade_date']});self.assertLess(rr['support'],110)
 def test_support_no_after_signal(self):
  pre=[{'trade_date':f'2024-01-{i+1:02d}','high':95,'low':70,'close':100,'amount':1,'volume':1} for i in range(65)]
  with self.assertRaises(ValueError):support_from_pre(pre,{'date':'2024-01-31'})
 def test_no_second_stop(self):
  r=simulate_one(s(),make_post(),P);self.assertEqual(r['reentered'],1);self.assertEqual(r['label'],'SURVIVED_TO_D30');self.assertEqual(r['second_stop'],0)
 def test_second_stop(self):
  r=simulate_one(s(),make_post(stopagain=True),P);self.assertEqual(r['label'],'SECOND_STOP');self.assertEqual(r['second_stop'],1)
 def test_stop_only_better_than_hold_if_down(self):
  r=simulate_one(s(),make_post(final=86,stopagain=True),P);self.assertLess(r['hold_net_pct'],r['stop_only_net_pct'])
 def test_no_future_reclaim_signal(self):
  rr=make_post();rr[1]['close']=98;rr[2]['close']=101
  r=simulate_one(s(),rr,P);self.assertEqual(r['reclaim_close_date'],rr[2]['trade_date']);self.assertEqual(r['reentry_date'],rr[3]['trade_date'])
 def test_reentry_requires_open_above_support(self):
  rr=make_post();rr[2]['open']=95;rr[2]['low']=95;rr[2]['close']=104
  r=simulate_one(s(),rr,P);self.assertEqual(r['reentered'],0)
 def test_second_leg_fees(self):
  r=simulate_one(s(),make_post(),P);self.assertAlmostEqual(r['delta_reentry_vs_stop_pp'],(110/102-1)*100-.2,5)
 def test_reclaim_volume_ratio_asof(self):
  r=simulate_one(s(),make_post(),P);self.assertEqual(r['reclaim_amount_ratio'],3)
 def test_no_valid_support_above_entry(self):
  t=s();t['support']=106;rr=simulate_one(t,make_post(),P);self.assertEqual(rr['eligibility'],'NO_ELIGIBLE_SUPPORT')
 def test_net_flow_no_missing(self):
  import pandas as pd
  df=pd.DataFrame({'외국인합계':[12,3],'기관합계':[0,-4]},index=pd.to_datetime(['2025-01-01','2025-01-02']))
  with self.assertRaises(ValueError):normalize_net(df,{'2025-01-01','2025-01-03'},'krw')
 def test_net_flow_signed(self):
  import pandas as pd
  df=pd.DataFrame({'외국인합계':[12,-3],'기관합계':[0,-4]},index=pd.to_datetime(['2025-01-01','2025-01-02']))
  self.assertEqual(normalize_net(df,{'2025-01-01'},'krw')['2025-01-02'][1],-4)
 def test_manifest_tamper(self):
  with tempfile.TemporaryDirectory() as t:
   p=Path(t);(p/'x.txt').write_text('one');manifest(p,phase='test');(p/'x.txt').write_text('two')
   with self.assertRaises(ValueError):check_dir(p)
 def test_repeated_sweep_is_past_only(self):
  rr=make_post();rr[-1]['low']=95; r=simulate_one(s(),rr,P);self.assertEqual(r['sweep_count_asof_reclaim'],1)
 def test_protocol_not_live(self):self.assertFalse(P['strategy_ready'] or P['live_changed'] or P['prospective_OOS']);self.assertEqual(P['orders_placed'],0)

if __name__=='__main__':unittest.main()

# This test deliberately uses synthetic provider bars and flows, not historical results.
class R1D26QEndToEnd(unittest.TestCase):
 def test_synthetic_125_full_pipeline(self):
  from datetime import timedelta,date
  import pandas as pd
  def fetch_prices(ev,phase):
   d=date.fromisoformat(ev['date']);out=[]
   if phase=='pre':
    for i in range(-79,1):
     dt=d+timedelta(days=i);sg=i==0
     out.append({**{k:ev[k] for k in EVENT},'trade_date':dt.isoformat(),'open':105 if sg else 99,'high':110 if sg else 100,'low':96 if sg else 94,'close':105 if sg else 99,'volume':10000,'amount':10000000})
   else:
    for i in range(30):
     dt=d+timedelta(days=i+1)
     if i==0:op,cl,low,hi=105,98,97,105
     elif i==1:op,cl,low,hi=98,101,97,102
     elif i==2:op,cl,low,hi=102,105,101,107
     elif int(ev['code'][-1])%2==0 and i==6:op,cl,low,hi=103,96,95,104
     elif int(ev['code'][-1])%2==0 and i==7:op,cl,low,hi=94,96,93,98
     else:op,cl,low,hi=105,110,103,111
     out.append({**{k:ev[k] for k in EVENT},'trade_date':dt.isoformat(),'open':op,'high':hi,'low':low,'close':cl,'volume':13000,'amount':30000000})
   return out
  def fake_flow(code,st,en,mode):
   dates=pd.date_range(start=st,end=en,freq='D');pos=10000 if int(code[-1])%2 else -12000
   return pd.DataFrame({'외국인합계':[pos]*len(dates),'기관합계':[-50]*len(dates)},index=dates)
  with tempfile.TemporaryDirectory() as d:
   root=Path(d);pre=root/'pre';post=root/'post';price=root/'price';flow=root/'flow';report=root/'report'
   self.assertEqual(collect_price(ROOT/FROZEN,P,pre,'pre',fetch=fetch_prices,sleep=0)['valid'],125)
   q={**P,'pre_dir':str(pre)}
   self.assertEqual(collect_price(ROOT/FROZEN,q,post,'post',fetch=fetch_prices,sleep=0)['valid'],125)
   st=price_evaluate(ROOT/FROZEN,pre,post,P,price)
   self.assertEqual(st['reentries'],125)
   st2=collect_flow(price,post,flow,fetch=fake_flow,sleep=0)
   self.assertEqual(st2['valid'],125)
   final=evaluate(price,flow,P,report)
   self.assertEqual(final['new_evaluable'],125)
   self.assertEqual(len(read_csv_bytes((report/SUM).read_bytes())),3)
   self.assertEqual(len(read_csv_bytes((report/RES).read_bytes())),125)
   for r in read_csv_bytes((report/RES).read_bytes()):
    self.assertEqual(r['reentry_date'],(date.fromisoformat(r['date'])+timedelta(days=3)).isoformat())
