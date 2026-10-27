import csv,gzip,io,json,os,tempfile,unittest,zipfile
from datetime import date,timedelta
from pathlib import Path
from research_formulas.stockhunter_r1d26n_structural_support import (
 C,D,levels,pivots,atr14,stop_intraday,stop_close_confirm,events,collect,evaluate,
 grouped_bars,load_original_l,digest,PRE_NAME,OHLC)
ROOT=Path(__file__).resolve().parents[1]
P=json.loads((ROOT/'R1D26N_PREDECLARED_PROTOCOL.json').read_text())
SEL=ROOT/'data/R1D26L_FROZEN_60_ASOF_EVENTS.csv'
ZIP=os.environ.get('R1D26N_TEST_L_ZIP','')

def bars(n=90,day='2023-09-12',price=100):
    x=[];d=date.fromisoformat(day);dates=[]
    while len(dates)<n:
        if d.weekday()<5:dates.append(d.isoformat())
        d-=timedelta(days=1)
    for dt in reversed(dates):x.append({'trade_date':dt,'open':price,'high':price+3,'low':price-2,'close':price,'volume':10000,'amount':1000000})
    return x

class Contracts(unittest.TestCase):
 def test_frozen_60_real_identity_and_sha(self):
    chosen=events(SEL,P)
    self.assertEqual(len(chosen),60)
    self.assertEqual(len({x['code'] for x in chosen}),60)
    self.assertEqual(digest(SEL.read_bytes()),P['selection_csv_sha256'])
 def test_selection_modify_is_rejected(self):
    with tempfile.TemporaryDirectory() as t:
        q=Path(t)/'x.csv';q.write_bytes(SEL.read_bytes().replace(b'028150',b'028151'))
        with self.assertRaisesRegex(ValueError,'SHA'):events(q,P)
 def test_signal_time_only_and_no_post_bars(self):
    ev={'strategy':C,'date':'2023-09-12','code':'123456'}
    q=bars()
    self.assertEqual(len(levels(q,ev)[0]),5)
    q.append(dict(q[-1],trade_date='2023-09-13'))
    with self.assertRaisesRegex(ValueError,'FUTURE'):levels(q,ev)
 def test_confirmed_pivot_uses_two_right_bars(self):
    q=bars()
    q[-3]['low']=70
    self.assertEqual(pivots(q)[-1][1],70)
    q[-1]['low']=50
    self.assertTrue(all(v!=50 for _,v in pivots(q)))
 def test_breakout_previous_20_excludes_signal_bar(self):
    q=bars();q[-1]['high']=300
    d={'strategy':D,'date':q[-1]['trade_date'],'code':'123456'}
    self.assertEqual(levels(q,d)[0]['D_PRE20_BREAKOUT_RETEST'],103)
 def test_pullback_includes_signal_bar(self):
    q=bars();q[-1]['low']=80
    d={'strategy':C,'date':q[-1]['trade_date'],'code':'123456'}
    self.assertEqual(levels(q,d)[0]['C_PULLBACK_LOW20'],80)
 def test_early_gap_uses_open(self):
    q=[dict(trade_date='2023-01-03',open=100,high=101,low=95,close=99),dict(trade_date='2023-01-04',open=88,high=93,low=85,close=90)]
    ret,reason,d=stop_intraday(q,90)
    self.assertEqual(reason,'STOP_GAP_OPEN');self.assertAlmostEqual(ret,-12.2)
 def test_intraday_level_fill_and_close_confirmation_difference(self):
    q=[dict(trade_date='2023-01-03',open=100,high=101,low=89,close=95),dict(trade_date='2023-01-04',open=97,high=105,low=96,close=104)]
    a=stop_intraday(q,90);b=stop_close_confirm(q,90)
    self.assertEqual(a[1],'STOP_INTRADAY_ASSUMED_AT_LEVEL');self.assertEqual(b[1],'HOLD_TO_D30')
 def test_close_break_then_next_open_not_same_close(self):
    q=[dict(trade_date='2023-01-03',open=100,high=101,low=88,close=89),dict(trade_date='2023-01-04',open=83,high=90,low=80,close=85)]
    a=stop_close_confirm(q,90)
    self.assertEqual(a[1],'CONFIRMED_CLOSE_NEXT_OPEN');self.assertAlmostEqual(a[0],-17.2)
 def test_support_not_below_entry_must_be_invalid(self):
    q=[dict(trade_date='2023-01-03',open=100,high=101,low=88,close=90)]
    self.assertEqual(stop_intraday(q,110)[1],'SUPPORT_NOT_BELOW_ENTRY')
 def test_duplicate_and_future_rows_rejected(self):
    ev=dict(strategy=C,date='2023-09-12',code='123456')
    q=[dict(ev,**x) for x in bars()]
    with self.assertRaisesRegex(ValueError,'FUTURE'):grouped_bars([dict(q[-1],trade_date='2023-09-13')],'pre')
    with self.assertRaisesRegex(ValueError,'DUPLICATE'):grouped_bars([q[-1],q[-1]],'pre')
 def test_real_L_archive_integrity_and_parity(self):
    if not ZIP or not Path(ZIP).is_file():self.skipTest('Set R1D26N_TEST_L_ZIP for original L artifact')
    p=load_original_l(ZIP,P['r1d26l_zip_sha256'],P['selection_csv_sha256'])
    self.assertEqual(len(p),59)
    self.assertEqual(sum(len(v) for v in p.values()),1770)
 def test_pre_provider_failure_is_censored_no_fake_data(self):
    with tempfile.TemporaryDirectory() as t:
        def fail(s,delay):raise ValueError('no provider')
        x=collect(SEL,P,t,fetch=fail,sleep_seconds=0)
        self.assertEqual(x['valid_events'],0);self.assertEqual(x['valid_rows'],0)
 def test_mock_pre_60_valid_but_never_as_real(self):
    chosen=events(SEL,P)
    with tempfile.TemporaryDirectory() as t:
        def fake(e,delay):
            return [dict({k:e[k] for k in ('strategy','date','code')},**r) for r in bars(day=e['date'])]
        x=collect(SEL,P,t,fetch=fake,sleep_seconds=0)
        self.assertEqual(x['valid_events'],60)
        self.assertEqual(x['valid_rows'],5400)
        if ZIP and Path(ZIP).is_file():
            out=Path(t)/'result'
            m=evaluate(SEL,P,t,ZIP,out)
            self.assertEqual(m['paired_pre_post_valid_events'],59)
            self.assertTrue(m['support_from_signal_time_only'])
            self.assertFalse(m['strategy_ready'])
 def test_fail_closed_sparse_pre_collection(self):
    with tempfile.TemporaryDirectory() as t:
        collect(SEL,P,t,fetch=lambda ev,delay: (_ for _ in ()).throw(ValueError('provider unavailable')),sleep_seconds=0)
        if ZIP and Path(ZIP).is_file():
            with self.assertRaisesRegex(ValueError,'INSUFFICIENT_PRE_SIGNAL_COVERAGE'):
                evaluate(SEL,P,t,ZIP,Path(t)/'result')
 def test_manifest_tamper_detected(self):
    with tempfile.TemporaryDirectory() as t:
        def fake(e,delay):return [dict({k:e[k] for k in ('strategy','date','code')},**r) for r in bars(day=e['date'])]
        collect(SEL,P,t,fetch=fake,sleep_seconds=0)
        p=Path(t)/PRE_NAME
        with p.open('ab') as f:f.write(b'TAMPER')
        if ZIP and Path(ZIP).is_file():
            with self.assertRaisesRegex(ValueError,'PRE_SOURCE_SHA_DRIFT'):
                evaluate(SEL,P,t,ZIP,Path(t)/'out')
 def test_missing_signal_day_pre_history_rejected(self):
    ev={'strategy':C,'date':'2023-09-12','code':'123456'}
    b=bars(day='2023-09-11')
    with self.assertRaisesRegex(ValueError,'SUPPORT_FUTURE_OR_SHORT_HISTORY'):
        levels(b,ev)
 def test_gap_through_stop_does_not_fill_at_support(self):
    q=[dict(trade_date='2023-01-03',open=100,high=102,low=98,close=99),dict(trade_date='2023-01-04',open=82,high=88,low=80,close=85)]
    a=stop_intraday(q,90)
    self.assertEqual(a[1],'STOP_GAP_OPEN')
    self.assertAlmostEqual(a[0],-18.2)
 def test_strategy_and_order_immutability(self):
    self.assertEqual(P['strategy_C_static_supports'],['C_CONFIRMED_SWING_LOW','C_SWING_ATR05','C_PULLBACK_LOW20','C_MA20_STATIC','C_MA60_STATIC'])
    self.assertEqual(P['strategy_D_static_supports'],['D_PRE20_BREAKOUT_RETEST','D_BREAKOUT_ATR05','D_PRE20_BOX_FLOOR','D_MA20_STATIC','D_MA60_STATIC'])
    self.assertEqual(P['exit_types'],['intraday_assumed_stop','close_confirm_next_open'])
 def test_no_live_no_orders_no_oos(self):
    self.assertTrue(P['research_only']);self.assertFalse(P['strategy_ready']);self.assertFalse(P['prospective_OOS']);self.assertFalse(P['live_changed']);self.assertEqual(P['orders_placed'],0)

if __name__=='__main__':unittest.main()
