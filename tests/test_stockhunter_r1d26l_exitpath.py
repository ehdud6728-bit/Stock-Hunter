"""R1-D26L guardrail regression, synthetic bars explicitly NOT research output."""
import csv, gzip, io, json, os, tempfile, unittest, zipfile
from pathlib import Path
import pandas as pd
from research_formulas.stockhunter_r1d26l_exitpath import (
 C,D,create_frozen_selection,verify_frozen,normalize_ohlcv,exit_fixed,exit_half_then_be,
 simulate,source_collect,evaluation,sha,read_csv,write_csv)

ROOT=Path(__file__).resolve().parents[1]
SELECT=ROOT/'data/R1D26L_FROZEN_60_ASOF_EVENTS.csv'
PROTO=ROOT/'R1D26L_PREDECLARED_PROTOCOL.json'
P=json.loads(PROTO.read_text())
def mk(openp=100,high=102,low=98,close=100,n=30):
    return [{'trade_date':f'2021-01-{i+1:02d}','open':openp,'high':high,'low':low,'close':close} for i in range(n)]
class R1D26LTests(unittest.TestCase):
 def test_selection_integrity_and_balance(self):
    r=verify_frozen(SELECT,P);self.assertEqual(len(r),60)
    self.assertEqual(len({q['code'] for q in r}),60)
    self.assertTrue(all(q['date'][:4]==q['year'] for q in r))
 def test_real_ASOF_selection_recompute(self):
    fn=os.environ.get('R1D26L_TEST_R1D25_ZIP')
    if not fn:self.skipTest('set R1D26L_TEST_R1D25_ZIP for pinned real-source QA')
    x=create_frozen_selection(fn)
    self.assertEqual([{k:str(a[k]) for k in ('strategy','date','code','year','selection_basis')} for a in x],read_csv(SELECT))
 def test_modify_selection_fail_closed(self):
    with tempfile.TemporaryDirectory() as t:
      q=Path(t)/'fake.csv';q.write_bytes(SELECT.read_bytes()+b' ')
      with self.assertRaisesRegex(ValueError,'FROZEN_ASOF_SELECTION_SHA_MISMATCH'):verify_frozen(q,P)
 def test_low_only_stop(self):
    q=mk();q[4]['low']=93
    self.assertEqual(exit_fixed(q,.10,-.05)[2],'STOP_INTRADAY')
    self.assertEqual(exit_fixed(q,.10,-.05)[0],95)
 def test_high_only_tp(self):
    q=mk();q[2]['high']=115
    self.assertEqual(exit_fixed(q,.10,-.05)[2],'TAKE_PROFIT_INTRADAY')
    self.assertAlmostEqual(exit_fixed(q,.10,-.05)[0],110)
 def test_simultaneous_worst_best_and_ambiguity(self):
    q=mk(high=115,low=94)
    a=exit_fixed(q,.10,-.05,'conservative');b=exit_fixed(q,.10,-.05,'optimistic')
    self.assertEqual(a[0],95);self.assertAlmostEqual(b[0],110)
    self.assertTrue(a[3] and b[3])
 def test_gap_stop_fills_open_not_level(self):
    q=mk();q[2].update(open=90,high=92,low=87,close=91)
    self.assertEqual(exit_fixed(q,.10,-.05)[0],90)
    self.assertEqual(exit_fixed(q,.10,-.05)[2],'STOP_GAP_OPEN')
 def test_gap_tp_uses_limit_price(self):
    q=mk();q[2].update(open=121,high=126,low=119,close=124)
    self.assertAlmostEqual(exit_fixed(q,.10,-.05)[0],110)
    self.assertEqual(exit_fixed(q,.10,-.05)[2],'TAKE_PROFIT_GAP_LIMIT')
 def test_no_exit_d30_fees(self):
    q=mk();q[-1]['close']=103
    by={(x['scenario'],x['ordering']):x for x in simulate(q)}
    self.assertAlmostEqual(by['HOLD_D30','conservative']['net_return_pct'],2.8)
    self.assertEqual(by['TP10_SL5','conservative']['exit_day'],q[-1]['trade_date'])
 def test_half_profit_BE_next_trading_day_only(self):
    q=mk();q[0].update(high=111,low=99)
    q[1].update(open=102,high=104,low=98,close=102)
    x=exit_half_then_be(q)
    self.assertEqual(x[1],q[1]['trade_date']);self.assertAlmostEqual(x[0],105)
 def test_half_same_day_high_low_conservative(self):
    q=mk(high=115,low=93)
    a=exit_half_then_be(q,'conservative');b=exit_half_then_be(q,'optimistic')
    self.assertEqual(a[0],95);self.assertTrue(a[3] and b[3])
 def test_normalize_rejects_non_ohlc_data(self):
    dates=pd.date_range('2021-01-04',periods=2)
    df=pd.DataFrame({'시가':[100,100],'고가':[105,90],'저가':[95,91],'종가':[100,100],'거래량':[1000,1000],'거래대금':[100000,100000]},index=dates)
    with self.assertRaisesRegex(ValueError,'OHLC_INCONSISTENT'):normalize_ohlcv(df,read_csv(SELECT)[0])
 def test_normalize_rejects_duplicate_day(self):
    dates=[pd.Timestamp('2021-01-04')]*2
    df=pd.DataFrame({'시가':[100,100],'고가':[105,105],'저가':[95,95],'종가':[100,100],'거래량':[1000,1000],'거래대금':[100000,100000]},index=dates)
    with self.assertRaisesRegex(ValueError,'DUPLICATE_TRADE_DAY'):normalize_ohlcv(df,read_csv(SELECT)[0])
 def test_source_mock_completes_and_rejects_missing(self):
    # Tests explicitly mocked; these values MUST NOT be interpreted as real KRX.
    import pandas as pd
    from datetime import datetime
    with tempfile.TemporaryDirectory() as t:
      def stub(s,a,b):
        dr=pd.date_range(start=a,end=b)
        ix=[0]+[round(x) for x in [i*(len(dr)-1)/29 for i in range(1,29)]]+[len(dr)-1]
        # May have duplicate calendar dates only if interval too short; protocol has 30+ calendar days.
        if len(set(ix))!=30:raise ValueError('BAD_SYNTH_CALENDAR')
        return [{**{k:s[k] for k in ('strategy','date','code')},'trade_date':dr[i].date().isoformat(),
                 'open':100,'high':102,'low':98,'close':100,'volume':1000,'amount':100000,
                 'provider':'MOCK_ONLY','source_PIT':'NOT_CERTIFIED'} for i in ix]
      m=source_collect(SELECT,P,t,stub)
      self.assertEqual(m['collection_attempted_events'],60)
      self.assertEqual(m['source_valid_events'],60)
      self.assertEqual(m['source_raw_rows'],1800)
      self.assertFalse(m['strategy_ready'])
 def test_fail_closed_on_incomplete_collection(self):
    with tempfile.TemporaryDirectory() as t:
      def nofetch(*a):raise ConnectionError('simulated failure')
      m=source_collect(SELECT,P,t,nofetch)
      self.assertEqual(m['source_valid_events'],0)
      with self.assertRaisesRegex(ValueError,'COLLECTION_TOO_SPARSE'):
         evaluation(t,SELECT,P,Path(t)/'nonexistent.zip',Path(t)/'out')
 def test_end_to_end_SYNTHETIC_reference_parity_never_real_returns(self):
    # Build an artificial historical D25 price-horizon file to test mechanics ONLY.
    with tempfile.TemporaryDirectory() as t:
      root=Path(t);src=root/'src';src.mkdir()
      selected=read_csv(SELECT)
      bars=[];baseline=[]
      for i,row in enumerate(selected):
        k='|'.join(row[x] for x in ('strategy','date','code'))
        rg=P['ranges'][k]
        ds=pd.date_range(start=rg['entry_day'],end=rg['exit_day'])
        ix=[round(j*(len(ds)-1)/29) for j in range(30)]
        self.assertEqual(len(set(ix)),30)
        for j in ix:
          bars.append({**{x:row[x] for x in ('strategy','date','code')},'trade_date':ds[j].date().isoformat(),
          'open':100,'high':102,'low':98,'close':100,'volume':1000,'amount':100000,
          'provider':'MOCK_ONLY','source_PIT':'NOT_CERTIFIED'})
        baseline.append({**{x:row[x] for x in ('strategy','date','code')},'horizon':30,
         'status_out':'CENSORED_MISSING_OR_ACTION_UNCERTAIN' if i==0 else 'PRICE_PATH_COMPLETE_NOT_EXECUTION_CERTIFIED',
         'net_20bps_pct':-.2,'mfe_intraday_pct':2.0,'mae_intraday_pct':-2.0,
         'entry_day':rg['entry_day'],'exit_day':rg['exit_day']})
      target=src/'R1D26L_RETRIEVED_UNADJUSTED_DAILY_OHLCV.csv.gz'
      with gzip.open(target,'wt',encoding='utf-8',newline='') as f:
        w=csv.DictWriter(f,fieldnames=list(bars[0]));w.writeheader();w.writerows(bars)
      m={'collection_attempted_events':60,'source_valid_events':60,'source_raw_rows':1800,'source_daily_sha256':sha(target.read_bytes())}
      (src/'R1D26L_COLLECTION_MANIFEST.json').write_text(json.dumps(m))
      buf=io.StringIO();w=csv.DictWriter(buf,fieldnames=list(baseline[0]));w.writeheader();w.writerows(baseline)
      z=root/'SYNTHETIC_NOT_REAL_R1D25.zip'
      with zipfile.ZipFile(z,'w') as f:f.writestr('R1D25_D5_D10_D15_D20_D30_FUTURE_PRICE_PATHS.csv.gz',gzip.compress(buf.getvalue().encode()))
      out=evaluation(src,SELECT,P,z,root/'result')
      self.assertEqual(out['baseline_price_parity'],59)
      self.assertEqual(out['reference_D30_censored_with_complete_OHLC'],1)
      self.assertEqual(len(read_csv(root/'result/R1D26L_EXIT_SIMULATION_PAIRED_EVENTS.csv')),59*12)
      self.assertFalse(out['strategy_ready'])
 def test_protect_no_order_and_no_pit(self):
    self.assertTrue(P['research_only'] and not P['strategy_ready'])
    self.assertEqual(P['orders_placed'],0);self.assertFalse(P['live_changed'])
    self.assertFalse(P['historical_PIT_certified'])
 def test_no_before_entry_on_all_scenarios(self):
    q=mk(high=102,low=98)
    self.assertTrue(all(r['exit_day']>=q[0]['trade_date'] for r in simulate(q)))

if __name__=='__main__':unittest.main()
