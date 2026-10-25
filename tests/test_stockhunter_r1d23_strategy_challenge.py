import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import numpy as np
import pandas as pd
from research_formulas import stockhunter_r1d23_strategy_challenge as m

ROOT=Path(__file__).resolve().parents[1]

class FourThesisTests(unittest.TestCase):
 def setUp(self): self.p=m.check_protocol(ROOT/'R1D23_PREDECLARED_PROTOCOL.json')
 def seq(self,n=350,level=10000,amt=1500000000):
  return [(i,level,level*1.01,level*.99,level,150000,amt) for i in range(n)]
 def test_fixed_four_strategies(self):self.assertEqual(len(m.STRATEGIES),4)
 def test_periods_do_not_overlap(self):self.assertLess(m.PERIODS['EARLY_2021_2023'][1],m.PERIODS['LATE_2024_2025'][0])
 def test_source_pinned(self):self.assertEqual(self.p['source']['krx_days'],1907)
 def test_malformed_history_rejected(self):self.assertFalse(m.valid_recent(self.seq(349)))
 def test_session_gap_rejected(self):
  s=self.seq();s[-1]=(351,)+s[-1][1:];self.assertFalse(m.valid_recent(s))
 def test_future_data_cannot_satisfy_350(self):self.assertFalse(m.valid_recent(self.seq(351)))
 def test_nonfinite_rejected(self):
  s=self.seq();s[-1]=(349,np.nan)+s[-1][2:];self.assertFalse(m.valid_recent(s))
 def test_bad_ohlc_rejected(self):
  s=self.seq();s[-1]=(349,10000,9999,9900,10000,150000,1500000000);self.assertFalse(m.valid_recent(s))
 def test_missing_trades_rejected(self):
  s=self.seq();s[-1]=(349,10000,10100,9900,10000,-1,1500000000);self.assertFalse(m.valid_recent(s))
 def test_gap_over40pct_rejected(self):
  s=self.seq();s[-1]=(349,20000,20500,9900,10000,150000,1500000000);self.assertFalse(m.valid_recent(s))
 def test_common_liquidity_floor(self):
  s=self.seq(350,10000,100000000);out,stat=m.detect_strategies(s,self.p);self.assertEqual(stat,'LOW_LIQUIDITY');self.assertFalse(out)
 def test_flat_price_yields_no_events(self):
  out,status=m.detect_strategies(self.seq(),self.p);self.assertEqual(status,'ASOF_OK');self.assertFalse(out)
 def test_no_outcomes_in_lock(self):
  with tempfile.TemporaryDirectory() as d:
   x=type('A',(),dict(mode='lock',protocol=ROOT/'R1D23_PREDECLARED_PROTOCOL.json',out=d))
   m.run(x);j=json.loads((Path(d)/'R1D23_LOCK.json').read_text());self.assertFalse(j['outcomes_read'])
 def test_score_only_complete(self):
  with tempfile.TemporaryDirectory() as d:
   f=pd.DataFrame([dict(strategy=m.STRATEGIES[0],date='2022-01-04',code='000001',period='EARLY_2021_2023',distance224_pct=-5,ma224_slope20_pct=-1,amount_KRW=1e9)])
   p=pd.DataFrame([dict(strategy=m.STRATEGIES[0],date='2022-01-04',code='000001',period='EARLY_2021_2023',horizon=5,status_out='CENSORED_MISSING_OR_ACTION_UNCERTAIN',net_20bps_pct=np.nan,mae_intraday_pct=np.nan,mfe_intraday_pct=np.nan)])
   scores,_=m.controls_and_scores(f,p,d);self.assertEqual(scores.iloc[0]['complete'],0)
 def test_no_mutation_to_live_production(self): self.assertTrue(all(self.p['prohibitions'].values()))

class PricePathTests(unittest.TestCase):
 def fake_data(self,td,gapday=None,missingday=None):
  files={};days=[f'2022-01-{i:02d}' for i in range(1,26)]
  for t,d in enumerate(days):
   path=Path(td)/(d+'.csv.gz')
   row=[dict(code='000002',date=d,open=100,high=110,low=95,close=100,volume=100000,amount=1000000000)] if t==missingday else [dict(code='000001',date=d,open=100 if t else 99,high=110,low=95,
     close=110 if t>=5 else 100,volume=100000,amount=1000000000)]
   if t==gapday:row=[dict(code='000001',date=d,open=170,high=180,low=165,close=175,volume=100000,amount=1000000000)]
   pd.DataFrame(row,columns=['code','date','open','high','low','close','volume','amount']).to_csv(path,index=False,compression='gzip')
   files[d]=(path,'SHA_UNUSED_IN_POST_ASOF_TEST')
  f=pd.DataFrame([dict(strategy=m.STRATEGIES[0],date=days[0],code='000001',period='EARLY_2021_2023',signal_ix=0,signal_close=99.)])
  return days,files,f
 def test_next_open_D5_net_20bps(self):
  with tempfile.TemporaryDirectory() as d:
   days,files,f=self.fake_data(d)
   x=m.outcome_paths(f,days,files,d)
   self.assertEqual(len(x),4)
   self.assertEqual(x.iloc[0].status_out,m.DONE)
   self.assertAlmostEqual(x.iloc[0].net_20bps_pct,9.8,places=6)
 def test_corporate_action_proxy_censored(self):
  with tempfile.TemporaryDirectory() as d:
   days,files,f=self.fake_data(d,gapday=2)
   x=m.outcome_paths(f,days,files,d)
   self.assertTrue(x.status_out.ne(m.DONE).all())
 def test_missing_middle_day_censored(self):
  with tempfile.TemporaryDirectory() as d:
   days,files,f=self.fake_data(d,missingday=3)
   x=m.outcome_paths(f,days,files,d)
   self.assertTrue(x.status_out.ne(m.DONE).all())

if __name__=='__main__':unittest.main()
