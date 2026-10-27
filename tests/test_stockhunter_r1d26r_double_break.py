import csv,hashlib,importlib,json,os,random,tempfile,unittest,copy
from pathlib import Path
from research_formulas import stockhunter_r1d26r_double_break_entry_filter as r

ROOT=Path(__file__).resolve().parents[1]
P=json.loads((ROOT/'R1D26R_PREDECLARED_PROTOCOL.json').read_text())

def prebars(e,unstable=False):
 from datetime import date,timedelta
 days=[(date.fromisoformat(e['date'])-timedelta(days=119-i)).isoformat() for i in range(120)]
 # fictitious daily sequence, no requirement of business days for mock tests
 out=[]
 for j,t in enumerate(days):
  o=c=100;lo=90;hi=105
  if j==119:o=120;c=120;lo=115;hi=130
  out.append({**{k:e[k] for k in r.EVENT},'trade_date':t,'open':o,'high':hi,'low':lo,'close':c,'volume':100,'amount':1000})
 return out

def postbars(e, support=105, broken=False, twice=False):
 from datetime import date,timedelta
 out=[]
 for j in range(30):
  o=c=120;h=135;lo=110
  if broken and j==1:c=100;o=115;lo=95;h=120
  if broken and j==2:o=102;lo=99;c=112;h=120 # initial sell next open 102; reclaim EOD
  if broken and j==3:o=113;c=115;lo=106
  if broken and twice and j==4:o=110;c=100;lo=95
  if broken and twice and j==5:o=98;c=102;lo=94
  t=(date.fromisoformat(e['date'])+timedelta(days=j+1)).isoformat()
  out.append({**{k:e[k] for k in r.EVENT},'trade_date':t,'open':o,'high':h,'low':lo,'close':c,'volume':100,'amount':1000})
 return out

class R26R(unittest.TestCase):
 def test_pre_signal_selection_structure(self):
  z=r.selection(ROOT/r.FROZEN,P)
  self.assertEqual(len(z),200)
  self.assertEqual(len({x['code'] for x in z}),200)
  self.assertEqual(len([x for x in z if x['strategy']==r.A]),100)
  self.assertEqual(len([x for x in z if x['strategy']==r.D]),100)
 def test_no_pilot_q_overlap(self):
  mine={x['code'] for x in r.selection(ROOT/r.FROZEN,P)}
  for p in (ROOT/r.OLD, ROOT/r.QSEL):
   self.assertFalse(mine&{x['code'] for x in r.q.read_csv_bytes(p.read_bytes())})
 def test_source_sha_fails_closed(self):
  with tempfile.TemporaryDirectory() as tmp:
   path=Path(tmp)/'x.csv';path.write_bytes((ROOT/r.FROZEN).read_bytes()+b'\n')
   with self.assertRaisesRegex(ValueError,'SHA_DRIFT'):r.selection(path,P)
 def test_asof_support_A_not_C(self):
  e=next(x for x in r.selection(ROOT/r.FROZEN,P) if x['strategy']==r.A)
  s=r.pre_support(prebars(e),e)
  self.assertEqual(s['support_name'],'A_PULLBACK_LOW20_INCLUDING_SIGNAL')
  self.assertEqual(s['support'],90)
 def test_asof_support_D_excludes_signal_high(self):
  e=next(x for x in r.selection(ROOT/r.FROZEN,P) if x['strategy']==r.D)
  s=r.pre_support(prebars(e),e)
  self.assertEqual(s['support_name'],'D_PRE20_BREAKOUT_RETEST')
  self.assertEqual(s['support'],105)
 def test_future_pre_signal_bar_rejected(self):
  e=next(x for x in r.selection(ROOT/r.FROZEN,P) if x['strategy']==r.D)
  b=prebars(e);b[-1]['trade_date']='2099-01-01'
  with self.assertRaises(ValueError):r.pre_support(b,e)
 def test_pre_gate_only_pre_signal_data(self):
  e=next(x for x in r.selection(ROOT/r.FROZEN,P) if x['strategy']==r.D)
  x=prebars(e);a=r.preinstability(x)
  # Mutate only post-price data: the pre-signal count cannot change
  b=postbars(e,broken=True)
  b[6]['close']=100000
  self.assertEqual(a,r.preinstability(x))
 def test_stop_then_buy_next_open_then_stop_again(self):
  e=next(x for x in r.selection(ROOT/r.FROZEN,P) if x['strategy']==r.D)
  s=r.pre_support(prebars(e),e);x=r.simulate(s,postbars(e,broken=True,twice=True),P)
  self.assertEqual(x['usable'],1)
  self.assertEqual(x['first_stop_date'],postbars(e)[2]['trade_date'])
  self.assertEqual(x['reentry_date'],postbars(e)[3]['trade_date'])
  self.assertEqual(x['second_stop'],1)
  self.assertEqual(x['second_stop_date'],postbars(e)[5]['trade_date'])
  self.assertLess(x['reclaim_once_pct'],x['stop_only_pct'])
 def test_two_leg_round_trips_both_pay_fees(self):
  e=next(x for x in r.selection(ROOT/r.FROZEN,P) if x['strategy']==r.D)
  x=r.simulate(r.pre_support(prebars(e),e),postbars(e,broken=True,twice=False),P)
  exp=(102/120-1)*100-.2+(120/113-1)*100-.2
  self.assertAlmostEqual(x['reclaim_once_pct'],exp,5)
 def test_pre_gate_skip_is_zero(self):
  e=next(x for x in r.selection(ROOT/r.FROZEN,P) if x['strategy']==r.D)
  s=r.pre_support(prebars(e),e);s['pre_gate_skip']=1
  x=r.simulate(s,postbars(e,broken=True),P)
  self.assertEqual(x['combined_gates_pct'],0)
  self.assertEqual(x['pre_gate_stop_pct'],0)
  self.assertNotEqual(x['stop_only_pct'],0)
 def test_post_gate_skip_is_stop_only(self):
  e=next(x for x in r.selection(ROOT/r.FROZEN,P) if x['strategy']==r.D)
  s=r.pre_support(prebars(e),e)
  b=postbars(e,broken=True)
  b[0]['low']=99;b[0]['close']=120
  b[2]['low']=100;b[2]['close']=112
  x=r.simulate(s,b,P)
  self.assertEqual(x['reentered'],1)
  self.assertGreaterEqual(x['sweeps_observed_at_reclaim'],2)
  self.assertAlmostEqual(x['reclaim_sweep_gate_pct'],x['stop_only_pct'])
 def test_no_reentry_below_support(self):
  e=next(x for x in r.selection(ROOT/r.FROZEN,P) if x['strategy']==r.D)
  b=postbars(e,broken=True);b[3]['open']=100;b[3]['low']=95
  x=r.simulate(r.pre_support(prebars(e),e),b,P)
  self.assertEqual(x['reentered'],0)
 def test_degenerate_support_no_trades(self):
  e=next(x for x in r.selection(ROOT/r.FROZEN,P) if x['strategy']==r.D)
  s=r.pre_support(prebars(e),e);s['support']=150
  self.assertEqual(r.simulate(s,postbars(e),P)['usable'],0)
 def test_full_200_mock_pipeline_and_seals(self):
  with tempfile.TemporaryDirectory() as tmp:
   pre=Path(tmp)/'pre';post=Path(tmp)/'post';ev=r.selection(ROOT/r.FROZEN,P)
   def fetch(e,phase):return prebars(e) if phase=='pre' else postbars(e,broken=(e['code'][-1] in '13579'))
   cwd=os.getcwd();os.chdir(ROOT)
   try:
    a=r.collect('pre',P,pre,0,fetch=fetch)
    self.assertEqual(a['valid'],200)
    pp={**P,'pre_dir':str(pre)}
    b=r.collect('post',pp,post,0,fetch=fetch)
    self.assertEqual(b['valid'],200)
    c=r.report(pre,post,P,Path(tmp)/'out')
    self.assertEqual(c['n_paired'],200)
    self.assertEqual(c['n_eligible'],200)
    self.assertEqual(c['A_eligible'],100)
    self.assertEqual(c['D_eligible'],100)
    self.assertTrue(r.check_seal(Path(tmp)/'out'))
   finally:os.chdir(cwd)
 def test_manifest_mutation_detected(self):
  with tempfile.TemporaryDirectory() as tmp:
   p=Path(tmp);(p/'test.csv').write_text('abc')
   r.file_seal(p,'pre',attempted=200,pre_decision_sealed=True)
   (p/'test.csv').write_text('changed')
   with self.assertRaises(ValueError):r.check_seal(p)
 def test_research_only_lock(self):
  self.assertTrue(P['research_only'])
  self.assertFalse(P['strategy_ready'])
  self.assertFalse(P['live_changed'])
  self.assertEqual(P['orders_placed'],0)
  self.assertFalse(P['prospective_OOS'])
if __name__=='__main__':unittest.main()
