import copy, csv, io, json, pathlib, tempfile, unittest, os
from research_formulas.stockhunter_r1d26m_fast_stop import evaluate_one,main,parse_source,VERSION

ROOT=pathlib.Path(__file__).resolve().parents[1]
PROTO=ROOT/'R1D26M_PREDECLARED_PROTOCOL.json'
PINNED=os.environ.get('R1D26M_TEST_L_ZIP','/mnt/data/StockHunter_R1D26L_GitHub_40356487576_results.zip')

class QuickStopGuardTests(unittest.TestCase):
 @classmethod
 def setUpClass(cls):
  cls.p=json.loads(PROTO.read_text());cls.z=PINNED
  if not pathlib.Path(cls.z).is_file():
   raise RuntimeError('Provide R1D26M_TEST_L_ZIP with original pinned L artifact; cannot use synthetic as research output')
 @staticmethod
 def bars(d1=-0.03,d3=-0.06,maxh=1.00,d2o=.95,d4o=.94,close30=.92,vol3=100):
  out=[]
  for i in range(30):
   o=1.0 if i==0 else (d2o if i==1 else (d4o if i==3 else .95))
   c=1.0+d1 if i==0 else (1.0+d3 if i==2 else (.94 if i==3 else (close30 if i==29 else .95)))
   h=max(o,c,maxh if i==1 else .98);l=min(o,c,.9)
   out.append(dict(strategy='C_ABOVE224_RISING_WAVE_PULLBACK',date='2021-01-01',code='000001',trade_date=f'2021-01-{i+1:02d}',open=o,high=h,low=l,close=c,amount=vol3 if i==2 else 100,volume=20))
  return out
 def test_pinned_archive_sha(self):
  s,r,h,m,q=parse_source(self.z,self.p);self.assertEqual(len(s),60);self.assertEqual(len(r),1770);self.assertEqual(len(h),59)
 def test_source_tamper_rejected(self):
  p=copy.deepcopy(self.p);p['r1d26l_zip_sha256']='0'*64
  with self.assertRaisesRegex(ValueError,'ORIGINAL_ZIP_SHA_MISMATCH'):parse_source(self.z,p)
 def test_protocol_not_strategy_ready(self):
  self.assertTrue(self.p['research_only']);self.assertFalse(self.p['strategy_ready']);self.assertEqual(self.p['orders_placed'],0)
 def test_d1_close_executes_d2_open(self):
  rr=evaluate_one(self.bars(),self.p);self.assertEqual(rr[1]['exit_reason'],'D1_SIGNAL_EXIT_D2_OPEN');self.assertAlmostEqual(rr[1]['net_return_pct'],-5.2)
 def test_d3_close_executes_d4_open(self):
  rr=evaluate_one(self.bars(d1=-.01),self.p);self.assertEqual(rr[2]['exit_reason'],'D3_SIGNAL_EXIT_D4_OPEN');self.assertAlmostEqual(rr[2]['net_return_pct'],-6.2)
 def test_d3_high_invalidates_no_rebound_rule(self):
  rr=evaluate_one(self.bars(d1=-.01,maxh=1.06),self.p);self.assertEqual(rr[2]['trigger'],0)
 def test_d3_no_rebound_is_not_intraday_stop(self):
  rr=evaluate_one(self.bars(d1=-.01,d3=-.06,d4o=.80),self.p);self.assertAlmostEqual(rr[2]['net_return_pct'],-20.2)
 def test_earliest_d1_prioritized(self):
  rr=evaluate_one(self.bars(),self.p);self.assertEqual(rr[3]['exit_reason'],'D1_SIGNAL_EXIT_D2_OPEN')
 def test_no_trigger_holds_d30(self):
  rr=evaluate_one(self.bars(d1=-.01,d3=-.01,close30=1.2),self.p);self.assertEqual(rr[3]['exit_reason'],'HOLD_D30');self.assertAlmostEqual(rr[3]['net_return_pct'],19.8)
 def test_daily_volume_alert_is_not_exit(self):
  b=self.bars(d1=-.01,d3=-.01,vol3=200)
  for k in range(3):
   b[k]['open']=1.0;b[k]['close']=.99;b[k]['high']=max(b[k]['high'],1.0)
  rr=evaluate_one(b,self.p);self.assertEqual(rr[3]['trigger'],0);self.assertEqual(rr[0]['day3_selling_volume_alert'],1)
 def test_bad_ohlc_rejected(self):
  b=self.bars();b[5]['low']=1.3
  with self.assertRaisesRegex(ValueError,'INVALID_OHLC'):evaluate_one(b,self.p)
 def test_duplicate_day_rejected(self):
  b=self.bars();b[1]['trade_date']=b[0]['trade_date']
  with self.assertRaisesRegex(ValueError,'BAD_DAILY_COUNT'):evaluate_one(b,self.p)
 def test_source_real_complete_summary_invariants(self):
  with tempfile.TemporaryDirectory() as tmp:
   sums,cases,m=main(self.z,PROTO,tmp)
   d={(r['strategy'],r['scenario']):r for r in sums}
   self.assertEqual(m['daily_source_complete'],59);self.assertEqual(m['daily_source_invalid'],1)
   self.assertEqual(m['comparison_scenario_rows'],236)
   self.assertEqual(d[('ALL','COMBINED_EARLIEST')]['triggered'],19)
   self.assertEqual(d[('ALL','COMBINED_EARLIEST')]['loss10_count'],10)
   self.assertEqual(d[('ALL','HOLD_D30')]['loss10_count'],17)
   self.assertEqual(d[('ALL','COMBINED_EARLIEST')]['rescued_loss10_count'],9)
   self.assertEqual(d[('ALL','COMBINED_EARLIEST')]['false_alarm_hold_profit10_count'],4)
   self.assertEqual(sum(r['group']=='MISSED_LARGE_LOSS' for r in cases),8)
   with open(pathlib.Path(tmp)/'R1D26M_INCOMPLETE_SOURCE.csv',encoding='utf8') as f:
    self.assertEqual(len(list(csv.DictReader(f))),1)
 def test_no_same_day_future_price_leak(self):
  b=self.bars(d1=-.01,d3=-.06,d4o=.94)
  first=evaluate_one(b,self.p)[2]['net_return_pct'];b[29]['close']=1.6;b[29]['high']=1.6
  second=evaluate_one(b,self.p)[2]['net_return_pct'];self.assertEqual(first,second)

if __name__=='__main__':unittest.main()
