import unittest,hashlib,json,io,csv,gzip,zipfile,tempfile,sys
from pathlib import Path
from datetime import date,timedelta
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from research_formulas import stockhunter_r1d26u_entry_holdout as u
P=json.loads((Path(__file__).resolve().parents[1]/'R1D26U_PREDECLARED_PROTOCOL.json').read_text())

def bar(d,o,c=None,h=None,l=None):
 c=o if c is None else c;h=max(o,c) if h is None else h;l=min(o,c) if l is None else l
 return {'trade_date':d,'open':o,'high':h,'low':l,'close':c,'amount':1000,'volume':100}

def post30(seq=None):
 seq=seq or {}
 b=[]
 for j in range(1,31):
  x=seq.get(j,(100.,100.,105.,95.))
  b.append(bar(f'2022-01-{j:02d}',*x))
 return b

class Check(unittest.TestCase):
 def test_research_guard(self):
  self.assertTrue(P['research_only']);self.assertFalse(P['live_changed']);self.assertFalse(P['prospective_OOS']);self.assertEqual(P['orders_placed'],0)
 def test_frozen_new200_outcome_blind_selection_sha(self):
  src=Path(__file__).resolve().parents[1]/u.UFROZEN
  self.assertEqual(u.digest(src),P['frozen_selection_sha256'])
  rows=u.readfile(src);self.assertEqual(len(rows),200)
  self.assertEqual(len({x['code'] for x in rows}),200)
  for st in (u.A,u.D):
   self.assertEqual(sum(x['strategy']==st for x in rows),100)
   for yr in map(str,range(2021,2026)):
    self.assertEqual(sum(x['strategy']==st and x['year']==yr for x in rows),20)
 def test_d25_pinned(self):
  self.assertEqual(P['parent_d25_zip_sha256'],u.D25_SHA)
 def test_asof_selection_uses_only_signal_file(self):
  s=io.StringIO();w=csv.DictWriter(s,['strategy','date','code','signal_close','ma224_recomputed']);w.writeheader()
  for st in (u.A,u.D):
   for year in range(2021,2026):
    for i in range(4):w.writerow(dict(strategy=st,date=f'{year}-04-{i+1:02d}',code=f'{year%100:02d}{0 if st==u.A else 1}{i:03d}',signal_close='90',ma224_recomputed='100'))
  b=io.BytesIO()
  with zipfile.ZipFile(b,'w') as z:z.writestr(u.ASOF,gzip.compress(s.getvalue().encode()))
  cfg={**P,'per_year_per_strategy':2}
  chosen=u.select_only_asof(b.getvalue(),set(),cfg)
  self.assertEqual(len(chosen),20);self.assertEqual(len({r['code'] for r in chosen}),20)
  rejected={chosen[0]['code']}
  chosen2=u.select_only_asof(b.getvalue(),rejected,cfg)
  self.assertNotIn(next(iter(rejected)),{x['code'] for x in chosen2})
 def test_support_asof_future_forbidden(self):
  e=dict(strategy=u.A,date='2022-04-20',code='000001',year='2022',selection_basis='mock')
  b=[bar(f'2022-01-{i%28+1:02d}',100,100,110,90) for i in range(90)]
  b[-1]['trade_date']=e['date']
  s=u.pre_support(e,b,P);self.assertEqual(s['S0_support'],90.)
  b[-1]['trade_date']='2099-01-01'
  with self.assertRaises(ValueError):u.pre_support(e,b,P)
 def test_D_support_excludes_signal(self):
  e=dict(strategy=u.D,date='2022-04-20',code='000001',year='2022',selection_basis='mock')
  b=[bar('2022-01-01',100,100,105,95) for _ in range(90)];b[-1]=bar(e['date'],120,120,160,115)
  s=u.pre_support(e,b,P);self.assertEqual(s['S0_support'],105.)
 def test_wave_is_asof_latest_confirmed(self):
  b=[bar(f'2022-01-{i+1:02d}',100,100,102,98) for i in range(85)]
  b[64]=bar('x',100,100,123,100)
  for i in (65,66):b[i]=bar('x',115,115,116,112)
  for i in range(67,85):b[i]=bar('x',110,111,116,106)
  w=u.wave_candidate(b,P);self.assertIsNotNone(w);self.assertEqual(w['support'],106.)
 def test_lookahead_stop_next_open(self):
  b=post30({1:(100,90,105,88),2:(81,85,87,80)})
  r=u.simulate(b,100,95,'E0',P)
  self.assertEqual(r['stop_events'],1);self.assertAlmostEqual(r['net_pct'],-19.2)
 def test_gap_cannot_fill_intraday_limit(self):
  # D0 near support and subsequent open > cap, low below cap => still no fill.
  b=post30({i:(110,110,111,95) for i in range(1,31)})
  r=u.simulate(b,102,100,'E1',P)
  self.assertEqual(r['traded'],0);self.assertEqual(r['net_pct'],0)
 def test_split_fills_only_after_confirmation(self):
  b=post30({1:(103,103,105,99),2:(104,104,105,103),3:(106,107,108,105)})
  # first 50% at D1 cap 105, confirmed D2 close> D1, second at D3 open106 <=108
  r=u.simulate(b,102,100,'E2',P)
  self.assertEqual(r['traded'],1);self.assertEqual(r['added'],1)
  self.assertTrue(0<r['capital_days']<=30)
 def test_cash_when_no_near(self):
  b=post30({i:(140,140,145,135) for i in range(1,31)})
  r=u.simulate(b,140,100,'E1',P)
  self.assertEqual(r['net_pct'],0);self.assertEqual(r['cash_fraction'],1.)
 def test_no_S1_candidate_means_zero_policy_cash(self):
  r=u.simulate(post30(),100,0,'E0',P)
  self.assertEqual(r['traded'],0)
 def test_manifest_detects_mutation(self):
  with tempfile.TemporaryDirectory() as d:
   p=Path(d);(p/'a.csv').write_text('a\n');u.seal(p,'pre',selection_sha256='x')
   u.verify_seal(p,'pre');(p/'a.csv').write_text('mutated\n')
   with self.assertRaisesRegex(ValueError,'SEALED_SOURCE_DRIFT'):u.verify_seal(p,'pre')
 def test_limit_conservative_worse_or_equal(self):
  b=post30({1:(102,102,105,99)})
  a=u.simulate(b,102,100,'E1',P,'conservative')
  x=u.simulate(b,102,100,'E1',P,'optimistic')
  self.assertLessEqual(a['net_pct'],x['net_pct'])
 def test_post_anomalies_censored(self):
  e=dict(strategy=u.A,date='2022-01-01',code='000001')
  with self.assertRaisesRegex(ValueError,'POST_NOT_D30'):u.check_bars([bar('2022-01-02',100)],e,'post')
 def test_support_locked_doesnt_change_by_post_outcomes(self):
  b=[bar('2022-01-01',100,100,108,90) for _ in range(89)]+[bar('2022-04-20',110,110,120,106)]
  e=dict(strategy=u.A,date='2022-04-20',code='000001')
  s=u.pre_support(e,b,P)
  p=post30();p[0]['close']=100000
  self.assertEqual(s,u.pre_support(e,b,P))

if __name__=='__main__':unittest.main()
