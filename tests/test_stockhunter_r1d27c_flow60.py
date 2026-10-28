import importlib.util, pathlib, unittest, tempfile
import pandas as pd
from collections import Counter
SRC=pathlib.Path(__file__).resolve().parents[1]/'research_formulas'/'stockhunter_r1d27c_flow60.py'
spec=importlib.util.spec_from_file_location('r1d27c',SRC)
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)

class ResearchChecks(unittest.TestCase):
 def setUp(self):
  self.days=pd.bdate_range('2024-03-01',periods=60).strftime('%Y-%m-%d').tolist()
  self.buy={'기관합계':2300000000,'외국인합계':2600000000,'개인':4100000000,'기타법인':1000000000}
  self.sell={'기관합계':2200000000,'외국인합계':2800000000,'개인':4000000000,'기타법인':1000000000}
 def feeds(self):
  d={}
  for kind in m.BRANCHES:
   for side in m.SIDES:
    x=self.buy if side=='매수' else self.sell if side=='매도' else {a:self.buy[a]-self.sell[a] for a in m.ACTORS}
    factor=100000 if kind=='VOLUME' else 1
    row={a:val//factor for a,val in x.items()};row['전체']=sum(row.values())
    d[kind,side]=pd.DataFrame([row]*len(self.days),index=pd.to_datetime(self.days))
  return d
 def test_valid_gross_net_both(self):self.assertEqual(len(m.verify_six(self.feeds(),self.days)),240)
 def test_missing_calendar(self):
  f=self.feeds();f['VALUE','매수']=f['VALUE','매수'].iloc[:-1]
  with self.assertRaises(m.AuditError):m.verify_six(f,self.days)
 def test_net_qa(self):
  f=self.feeds();f['VOLUME','순매수'].loc[:,'개인']=100
  with self.assertRaises(m.AuditError):m.verify_six(f,self.days)
 def test_bad_integer(self):
  f=self.feeds();f['VALUE','매수'].loc[:,'기관합계']='redacted'
  with self.assertRaises(m.AuditError):m.verify_six(f,self.days)
 def test_missing_actor(self):
  f=self.feeds();f['VALUE','매수']=f['VALUE','매수'].drop(columns=['개인'])
  with self.assertRaises(m.AuditError):m.verify_six(f,self.days)
 def test_negative_gross(self):
  f=self.feeds();f['VALUE','매수'].loc[:,'기관합계']=-100
  with self.assertRaises(m.AuditError):m.verify_six(f,self.days)
 def test_duplicate_date(self):
  f=self.feeds();z=f['VALUE','매수'].copy();z.index=list(z.index[:-1])+[z.index[-2]];f['VALUE','매수']=z
  with self.assertRaises(m.AuditError):m.verify_six(f,self.days)
 def test_future_is_not_needed(self):
  x=m.verify_six(self.feeds(),self.days)
  self.assertEqual(max(r['trade_date'] for r in x),self.days[-1])
 def test_rolling_lookback(self):
  df=pd.DataFrame([r for r in m.verify_six(self.feeds(),self.days) if r['actor']=='기관합계'])
  v=m.actor_features(df)
  self.assertEqual(v['net5_krw'],500000000)
  self.assertEqual(v['net60_krw'],6000000000)
  self.assertEqual(v['net_recent10_vs_prev10_krw'],0)
 def test_rolling_coarse_volume(self):
  df=pd.DataFrame([r for r in m.verify_six(self.feeds(),self.days) if r['actor']=='외국인합계'])
  v=m.actor_features(df)
  self.assertEqual(v['net20_shares'],-40000)
  self.assertEqual(v['positive_days20'],0)
 def test_no_imputation(self):
  df=pd.DataFrame([r for r in m.verify_six(self.feeds(),self.days) if r['actor']=='개인']).iloc[:-1]
  with self.assertRaises(m.AuditError):m.actor_features(df)
 def test_emit_verify(self):
  with tempfile.TemporaryDirectory() as t:
   m.emit(pathlib.Path(t)/'a.csv',[{'a':1}],['a']);mm=m.manifest(t,'test')
   self.assertEqual(m.verify(t,'test')['files_sha256'],mm['files_sha256'])
   (pathlib.Path(t)/'a.csv').write_text('notoriginal')
   with self.assertRaises(m.AuditError):m.verify(t,'test')
 def test_source_matches_net_and_gross(self):
  rows=m.verify_six(self.feeds(),self.days)
  self.assertEqual(set(r['actor'] for r in rows),set(m.ACTORS))
  self.assertEqual(sum(r['net_krw'] for r in rows if r['trade_date']==self.days[0]),0)
 def test_pilot_selection(self):
  rows=[]
  for year in range(2021,2026):
   for bb in ('20','30','40'):
    for ev in ('BREAK','RECLAIM'):
     for k in range(10):rows.append({'year':year,'bb_period':bb,'event_type':ev,'select_hash':f'{year}-{bb}-{ev}-{k:02d}'})
  df=pd.DataFrame(rows)
  self.assertEqual(len(m.subset(df,'pilot')),30)
  self.assertEqual(len(m.subset(df,'full')),300)

if __name__=='__main__':unittest.main()
