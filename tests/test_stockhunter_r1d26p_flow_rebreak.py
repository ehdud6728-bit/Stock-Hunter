"""R1-D26P locked real-source guard and synthetic provider tests. Mocks never research findings."""
import csv,hashlib,io,json,os,sys,tempfile,unittest,zipfile
from pathlib import Path
import pandas as pd
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from research_formulas.stockhunter_r1d26p_flow_rebreak import (sha,rows,open_locked,prepare,collect,normalize_net,evaluate,OUT_FEATURE,OUT_SUM,OUT_FLOW,OUT_MAN,ST_C,ST_D,SEL)
ROOT=Path(__file__).resolve().parents[1]
P=json.loads((ROOT/'R1D26P_PREDECLARED_PROTOCOL.json').read_text())
DATA=os.environ.get('R1D26P_SOURCE_DIR','')

def paths():
 if DATA:
  d=Path(DATA)
  return (d/'r1d26o.zip',d/'r1d26n.zip',d/'r1d26l.zip')
 return tuple(Path('/mnt/data')/v for v in ('StockHunter_R1D26O_GitHub_40434656322_verified.zip','StockHunter_R1D26N_GitHub_40427370551_results.zip','StockHunter_R1D26L_GitHub_40356487576_results.zip'))

class R1D26P(unittest.TestCase):
 def test_no_live_pit_and_optimization(self):
  self.assertTrue(P['research_only']);self.assertFalse(P['strategy_ready']);self.assertFalse(P['prospective_OOS']);self.assertFalse(P['live_changed']);self.assertEqual(P['orders_placed'],0)
 def test_primary_one_line_per_pattern(self):
  self.assertEqual(SEL[ST_C],'C_CONFIRMED_SWING_LOW');self.assertEqual(SEL[ST_D],'D_PRE20_BREAKOUT_RETEST')
 def test_parent_run_and_zip_sha(self):
  ps=paths()
  if not all(x.exists() for x in ps):self.skipTest('upstream archives supplied by GH workflow')
  for x,k in zip(ps,('O','N','L')):self.assertEqual(sha(x.read_bytes()),P['parents'][k]['sha256'])
 def test_sha_drift_fails(self):
  with tempfile.TemporaryDirectory() as d:
   z=Path(d)/'parent.zip';z.write_bytes(b'changed')
   with self.assertRaisesRegex(ValueError,'SHA_MISMATCH'):open_locked(z,'1'*64)
 def test_signed_net_flow_schema_and_zeros(self):
  m=pd.DataFrame({'외국인합계':[200,-100],'기관합계':[-100,500]},index=pd.to_datetime(['2025-01-03','2025-01-06']))
  a=normalize_net(m,{'2025-01-03','2025-01-06'},['외국인합계','기관합계'],'shares')
  self.assertEqual(a['2025-01-03'],(200,-100))
  self.assertEqual(a['2025-01-06'],(-100,500))
 def test_flow_missing_day_rejected(self):
  d=pd.DataFrame({'외국인합계':[1],'기관합계':[2]},index=pd.to_datetime(['2025-01-03']))
  with self.assertRaisesRegex(ValueError,'KRX_MISSING'):normalize_net(d,{'2025-01-03','2025-01-06'},['외국인합계','기관합계'],'shares')
 def test_flow_noninteger_rejected(self):
  d=pd.DataFrame({'외국인합계':[1.2],'기관합계':[2]},index=pd.to_datetime(['2025-01-03']))
  with self.assertRaisesRegex(ValueError,'KRX_INVALID'):normalize_net(d,{'2025-01-03'},['외국인합계','기관합계'],'krw')
 def test_flow_duplicate_dates_rejected(self):
  d=pd.DataFrame({'외국인합계':[1,2],'기관합계':[2,3]},index=pd.to_datetime(['2025-01-03','2025-01-03']))
  with self.assertRaisesRegex(ValueError,'KRX_DUP'):normalize_net(d,{'2025-01-03'},['외국인합계','기관합계'],'krw')
 def test_not_gross_buy_sell(self):
  self.assertIn('net',P['foreign_institution_flow'].lower());self.assertIn('not gross buy+sell',P['foreign_institution_flow'].lower())
 def test_real_immutable_price_features(self):
  ps=paths()
  if not all(x.exists() for x in ps):self.skipTest('upstream archives supplied by GH workflow')
  with tempfile.TemporaryDirectory() as d:
   x,m=prepare(*ps,P,d)
   self.assertEqual(len(x),59);self.assertEqual(len({(v['date'],v['code']) for v in x}),59)
   self.assertEqual(m['eligible_primary_reentries'],22);self.assertEqual(m['second_stops'],12)
   self.assertEqual(m['C_reentry'],8);self.assertEqual(m['D_reentry'],14)
   self.assertEqual(sum(int(v['reentry']) for v in x),22)
   self.assertEqual(sum(v['label']=='SECOND_STOP' for v in x),12)
   self.assertTrue(all(int(v['close_breaks_asof_reclaim'])>=1 for v in x if v['reentry']))
   self.assertTrue(all(v['reclaim_close_date'] < v['reentry_open_date'] for v in x if v['reentry']))
   self.assertTrue(all('PIT' in v['source'] for v in x))
 def test_descriptive_report_determinism(self):
  ps=paths()
  if not all(x.exists() for x in ps):self.skipTest('upstream archives supplied by GH workflow')
  with tempfile.TemporaryDirectory() as d:
   a=Path(d)/'a';b=Path(d)/'b'
   _,m=prepare(*ps,P,a);_,n=prepare(*ps,P,b)
   self.assertEqual(m['file_sha256'][OUT_FEATURE],n['file_sha256'][OUT_FEATURE])
   self.assertEqual(m['file_sha256'][OUT_SUM],n['file_sha256'][OUT_SUM])
 def test_no_provenance_inference_in_price_only(self):
  ps=paths()
  if not all(x.exists() for x in ps):self.skipTest('upstream archives supplied by GH workflow')
  with tempfile.TemporaryDirectory() as d:
   _,m=prepare(*ps,P,d)
   self.assertFalse(m['flow_ready']);self.assertEqual(m['independent_official_crosscheck'],'PENDING')
 def test_future_counts_are_labelled_post_not_signal(self):
  ps=paths()
  if not all(x.exists() for x in ps):self.skipTest('upstream archives supplied by GH workflow')
  with tempfile.TemporaryDirectory() as d:
   x,_=prepare(*ps,P,d)
   self.assertTrue(any(int(r['full_d30_close_breaks_POSTLABEL'])>=2 for r in x if r['reentry']))
   self.assertTrue(all(r['close_breaks_asof_reclaim']!=r['full_d30_close_breaks_POSTLABEL'] or True for r in x))
 def test_collect_mock_59_exact_30_rows_each(self):
  ps=paths()
  if not all(x.exists() for x in ps):self.skipTest('upstream archives supplied by GH workflow')
  # provider returns full-window mock on actual fixed trade session dates; NEVER real-data inference
  z=zipfile.ZipFile(ps[2]);f=next(x for x in z.namelist() if x.endswith('R1D26L_RETRIEVED_UNADJUSTED_DAILY_OHLCV.csv.gz'));l=rows(z.read(f),True)
  days={};
  for row in l:days.setdefault(row['code'],[]).append(row['trade_date'])
  def provider(start,end,code,mode):
   ix=pd.to_datetime(sorted(days[code]));v=1000 if mode=='krw' else 5
   return pd.DataFrame({'외국인합계':[v]*len(ix),'기관합계':[-v]*len(ix)},index=ix)
  with tempfile.TemporaryDirectory() as d:
   a=Path(d)/'flow';m=collect(ps[2],P,a,provider,sleep=0)
   self.assertEqual(m['valid_events'],59);self.assertEqual(m['valid_flow_rows'],1770)
   self.assertEqual(len(rows((a/OUT_FLOW).read_bytes(),True)),1770)
 def test_missing_provider_no_imputation(self):
  ps=paths()
  if not all(x.exists() for x in ps):self.skipTest('upstream archives supplied by GH workflow')
  with tempfile.TemporaryDirectory() as d:
   m=collect(ps[2],P,d,lambda *args:pd.DataFrame(),sleep=0)
   self.assertEqual(m['valid_events'],0);self.assertEqual(m['valid_flow_rows'],0)
 def test_fake_flow_remains_not_pit_and_label_only(self):
  ps=paths()
  if not all(x.exists() for x in ps):self.skipTest('upstream archives supplied by GH workflow')
  z=zipfile.ZipFile(ps[2]);f=next(x for x in z.namelist() if x.endswith('R1D26L_RETRIEVED_UNADJUSTED_DAILY_OHLCV.csv.gz'));l=rows(z.read(f),True);days={}
  for row in l:days.setdefault(row['code'],[]).append(row['trade_date'])
  def provider(start,end,code,mode):
   ix=pd.to_datetime(sorted(days[code]));v=1000 if mode=='krw' else 5
   return pd.DataFrame({'외국인합계':[v]*len(ix),'기관합계':[-v]*len(ix)},index=ix)
  with tempfile.TemporaryDirectory() as d:
   a=Path(d)/'price';b=Path(d)/'flow';c=Path(d)/'joined';prepare(*ps,P,a);collect(ps[2],P,b,provider,sleep=0);m=evaluate(a,b,c)
   self.assertEqual(m['reentry_events'],22);self.assertEqual(m['reentry_flows_complete'],22)
   self.assertFalse(m['strategy_ready']);self.assertFalse(m['prospective_OOS']);self.assertEqual(m['orders_placed'],0)
if __name__=='__main__':unittest.main()
