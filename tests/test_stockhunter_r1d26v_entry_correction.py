import unittest,hashlib,json,io,csv,gzip,zipfile,tempfile,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from research_formulas import stockhunter_r1d26v_entry_correction as v
P=json.loads((Path(__file__).resolve().parents[1]/'R1D26V_PREDECLARED_PROTOCOL.json').read_text())

def b(day,o,c=None,h=None,l=None):
 c=o if c is None else c;h=max(o,c) if h is None else h;l=min(o,c) if l is None else l
 return {'trade_date':f'2022-02-{day:02d}','open':o,'high':h,'low':l,'close':c,'amount':1000,'volume':100}
def post(seq=None):
 seq=seq or {};return [b(i,*seq.get(i,(100,100,105,95))) for i in range(1,31)]
class R1D26V(unittest.TestCase):
 def test_research_guard(self):
  self.assertTrue(P['research_only']);self.assertFalse(P['strategy_ready']);self.assertFalse(P['live_changed']);self.assertFalse(P['prospective_OOS']);self.assertFalse(P['execution_certified']);self.assertEqual(P['orders_placed'],0)
 def test_frozen_new200_sha_years_and_disjoint_u(self):
  path=Path(__file__).resolve().parents[1]/v.VFROZEN
  self.assertEqual(v.digest(path),P['frozen_selection_sha256'])
  rows=v.readfile(path);self.assertEqual(len(rows),200);self.assertEqual(len({r['code'] for r in rows}),200)
  prev={r['code'] for r in v.readfile(Path(__file__).resolve().parents[1]/v.UOLD)}
  self.assertFalse(prev&{r['code'] for r in rows})
  for strategy in (v.A,v.D):
   self.assertEqual(sum(r['strategy']==strategy for r in rows),100)
   for year in map(str,range(2021,2026)):
    self.assertEqual(sum(r['strategy']==strategy and r['year']==year for r in rows),20)
 def test_sha_pins(self):
  self.assertEqual(P['parent_d25_zip_sha256'],v.D25_SHA)
  self.assertEqual(P['u_artifact_zip_sha256'],'e4a6772ce70a7eb53873a5c33505584ed432c52f0cc7fb306c83dede2e2e143d')
 def test_selection_asof_only(self):
  s=io.StringIO();w=csv.DictWriter(s,['strategy','date','code','signal_close','ma224_recomputed']);w.writeheader()
  for strat in (v.A,v.D):
   for year in range(2021,2026):
    for j in range(4):w.writerow({'strategy':strat,'date':f'{year}-04-{j+1:02d}','code':f'{year%100:02d}{int(strat==v.D)}{j:03d}','signal_close':90,'ma224_recomputed':100})
  data=io.BytesIO()
  with zipfile.ZipFile(data,'w') as z:z.writestr(v.ASOF,gzip.compress(s.getvalue().encode()))
  picked=v.select_only_asof(data.getvalue(),set(),{**P,'per_year_per_strategy':2});self.assertEqual(len(picked),20)
  banned={picked[0]['code']};new=v.select_only_asof(data.getvalue(),banned,{**P,'per_year_per_strategy':2})
  self.assertNotIn(picked[0]['code'],{r['code'] for r in new})
 def test_A_asof_support_rejects_future(self):
  evt={'strategy':v.A,'date':'2022-04-20','code':'000001'}
  pre=[b(i,100,100,110,90) for i in range(1,90)];pre[-1]['trade_date']=evt['date']
  self.assertEqual(v.pre_support(evt,pre,P)['S0_support'],90)
  pre[-1]['trade_date']='2099-01-01'
  with self.assertRaises(ValueError):v.pre_support(evt,pre,P)
 def test_D_support_ignores_signal_peak(self):
  evt={'strategy':v.D,'date':'2022-04-20','code':'000001'}
  pre=[b(i,100,100,105,95) for i in range(1,90)];pre[-1]=b(90,120,120,160,115);pre[-1]['trade_date']=evt['date']
  self.assertEqual(v.pre_support(evt,pre,P)['S0_support'],105)
 def test_gap_below_support_rejected_first(self):
  # D0 102 is near S=100. Next OPEN below S -> cannot buy.
  p=post({**{i:(110,110,111,109) for i in range(2,31)},1:(98,96,102,95)})
  x=v.simulate(p,102,100,'E1',P)
  self.assertEqual(x['skip_gap_count'],1);self.assertEqual(x['traded'],0)
  self.assertEqual(x['net_pct'],0)
 def test_same_gap_E0_is_comparable_market_baseline(self):
  p=post({**{i:(110,110,111,109) for i in range(2,31)},1:(98,96,102,95)})
  x=v.simulate(p,102,100,'E0',P)
  self.assertEqual(x['traded'],1);self.assertEqual(x['skip_gap_count'],0)
 def test_open_above_cap_low_cannot_fill(self):
  p=post({i:(110,110,111,95) for i in range(1,31)})
  x=v.simulate(p,102,100,'E1',P)
  self.assertEqual(x['traded'],0);self.assertGreaterEqual(x['skip_cap_count'],1)
 def test_open_used_instead_of_upper_cap(self):
  # D0 102 near S, next open 101 while cap=105.
  p=post({1:(101,101,103,100),2:(110,110,110,110)})
  x=v.simulate(p,102,100,'E1',P)
  self.assertEqual(x['traded'],1)
  self.assertAlmostEqual(x['net_pct'],(100/101-1)*100-.2,places=5) # D30 closes at 100
 def test_gap_guard_second_tranche(self):
  p=post({**{i:(120,120,120,120) for i in range(4,31)},1:(103,103,105,100),2:(103,104,105,102),3:(99,102,103,98)})
  x=v.simulate(p,102,100,'E2',P)
  self.assertEqual(x['traded'],1);self.assertEqual(x['added'],0);self.assertGreaterEqual(x['skip_gap_count'],1)
 def test_split_50_50_and_confirm(self):
  p=post({1:(103,103,105,99),2:(104,104,105,103),3:(106,107,108,105)})
  x=v.simulate(p,102,100,'E2',P)
  self.assertEqual(x['traded'],1);self.assertEqual(x['added'],1)
  self.assertLess(x['capital_days'],30);self.assertGreater(x['capital_days'],15)
 def test_stop_next_open_never_intraday(self):
  p=post({1:(100,90,105,88),2:(81,85,87,80)})
  x=v.simulate(p,100,95,'E0',P)
  self.assertEqual(x['stop_events'],1);self.assertAlmostEqual(x['net_pct'],-19.2,places=5)
 def test_slippage_stress_monotonic(self):
  p=post({1:(102,102,105,99)})
  a=v.simulate(p,102,100,'E1',P,'base')
  b1=v.simulate(p,102,100,'E1',P,'stress_10bp_each_side')
  self.assertEqual(a['traded'],b1['traded']);self.assertLess(b1['net_pct'],a['net_pct'])
 def test_cash_not_full_exposure(self):
  p=post({i:(140,140,145,135) for i in range(1,31)})
  x=v.simulate(p,140,100,'E1',P)
  self.assertEqual(x['traded'],0);self.assertEqual(x['cash_fraction'],1)
 def test_stop_on_last_close_is_not_next_day_execution(self):
  p=post({30:(100,91,104,90)})
  x=v.simulate(p,100,95,'E0',P)
  self.assertEqual(x['stop_events'],0);self.assertEqual(x['late_stop'],1)
 def test_invalid_support_cash(self):
  x=v.simulate(post(),100,0,'E0',P)
  self.assertEqual(x['traded'],0)
 def test_manifest_tamper_fail(self):
  with tempfile.TemporaryDirectory() as d:
   p=Path(d);(p/'x').write_text('hello');v.seal(p,'pre',selection_sha256='lock')
   v.verify_seal(p,'pre');(p/'x').write_text('altered')
   with self.assertRaisesRegex(ValueError,'SEALED_SOURCE_DRIFT'):v.verify_seal(p,'pre')
 def test_bad_ohlc_and_missing_d30_fail(self):
  ev={'strategy':v.A,'date':'2022-01-01','code':'000001'}
  with self.assertRaisesRegex(ValueError,'POST_NOT_D30'):v.check_bars([b(1,100)],ev,'post')
  broken=post();broken[0]['low']=110
  with self.assertRaisesRegex(ValueError,'INVALID_OHLC'):v.check_bars(broken,ev,'post')
 def test_asof_feature_not_changed_by_future(self):
  pre=[b(i,100,100,108,90) for i in range(1,90)];pre[-1]['trade_date']='2022-04-20'
  e={'strategy':v.A,'date':'2022-04-20','code':'000001'}
  s=v.pre_support(e,pre,P)
  future=post();future[0]['close']=10**10
  self.assertEqual(s,v.pre_support(e,pre,P))
 def test_wave_unchanged_predeclared(self):
  pre=[b(i,100,100,102,98) for i in range(1,86)]
  pre[64]=b(65,100,100,123,100)
  for i in (65,66):pre[i]=b(i+1,115,115,116,112)
  for i in range(67,85):pre[i]=b(i+1,110,111,116,106)
  c=v.wave_candidate(pre,P)
  self.assertIsNotNone(c);self.assertEqual(c['support'],106)
 def test_transaction_cost_bound_no_double_claim(self):
  p=post({1:(102,100,105,99)})
  r=v.simulate(p,102,100,'E1',P)
  self.assertEqual(r['traded'],1)
  self.assertAlmostEqual(r['net_pct'],(100/102-1)*100-.2,places=5)

if __name__=='__main__':unittest.main()
