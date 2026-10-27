"""R1D26O strict chronological, two-leg risk, source-integrity regression."""
import csv,io,json,os,tempfile,unittest
from pathlib import Path
from research_formulas.stockhunter_r1d26o_reclaim_reentry import (
    first_stop,reclaim_close_index,reentry_signal,secondary_exit,simulate_pair,
    audit,assert_source_integrity,sha,readcsv,C,D)
R=Path(__file__).resolve().parents[1]
P=json.loads((R/'R1D26O_PREDECLARED_PROTOCOL.json').read_text())
N=os.environ.get('R1D26O_TEST_N_ZIP','')
L=os.environ.get('R1D26O_TEST_L_ZIP','')

def bs(seq):
    return [{'trade_date':f'2025-01-{i+1:02}','open':a,'high':b,'low':c,'close':d}
            for i,(a,b,c,d) in enumerate(seq)]

class Chronology(unittest.TestCase):
 def test_intraday_stop_at_level_and_gap(self):
    x=bs([(105,108,99,102),(100,110,95,107)])
    self.assertEqual(first_stop(x,100,'intraday_assumed_stop'),(0,100,0,'STOP_INTRADAY_ASSUMED_AT_LEVEL'))
    self.assertEqual(first_stop(bs([(96,105,91,100)]),100,'intraday_assumed_stop'),(0,96,0,'STOP_GAP_OPEN'))
 def test_close_confirm_not_instantaneous(self):
    x=bs([(105,111,95,99),(97,102,92,101),(103,110,101,108)])
    self.assertEqual(first_stop(x,100,'close_confirm_next_open'),(1,97,0,'CONFIRMED_CLOSE_NEXT_OPEN'))
 def test_close_confirm_last_day_does_not_invent_extra_day(self):
    x=bs([(105,108,101,105),(105,106,98,99)])
    self.assertEqual(first_stop(x,100,'close_confirm_next_open'),(1,99,None,'HOLD_TO_D30'))
 def test_reclaim_same_exit_day_close_is_allowed_for_intraday(self):
    x=bs([(105,107,99,101),(106,108,101,107),(110,113,100,111)])
    s=reentry_signal(x,100,0,'reclaim_next_open',P)
    self.assertEqual((s['reclaim_i'],s['signal_i'],s['entry_i']),(0,0,1))
 def test_never_reenter_at_same_day_close(self):
    x=bs([(105,110,95,102),(105,110,102,106),(110,112,99,110)])
    self.assertEqual(reentry_signal(x,100,0,'reclaim_next_open',P)['entry_i'],1)
 def test_reclaim_not_before_exit_execution(self):
    x=bs([(105,107,92,103),(101,103,98,99),(102,106,98,104),(103,105,97,104)])
    s=reentry_signal(x,100,1,'reclaim_next_open',P)
    self.assertEqual((s['reclaim_i'],s['entry_i']),(2,3))
 def test_reclaim_on_last_bar_no_reentry(self):
    x=bs([(105,108,95,99),(102,104,97,101)])
    self.assertIsNone(reentry_signal(x,100,1,'reclaim_next_open',P)['entry_i'])
 def test_retest_cannot_use_reclaim_days_low(self):
    x=bs([(105,108,90,101),(106,111,104,109),(107,110,103,107),(107,109,103,108),(110,113,103,111)])
    s=reentry_signal(x,100,0,'retest_next_open',P)
    self.assertEqual(s['status'],'RECLAIM_WITHOUT_CONFIRMED_RETEST')
 def test_retest_requires_subsequent_close_plus_next_open(self):
    x=bs([(105,110,95,101),(108,109,103,104),(105,110,99,101),(104,108,101,106)])
    s=reentry_signal(x,100,0,'retest_next_open',P)
    self.assertEqual((s['reclaim_i'],s['signal_i'],s['entry_i']),(0,2,3))
 def test_retest_outside_five_days_rejected(self):
    x=bs([(106,111,95,101)]+[(110,115,108,111)]*5+[(111,114,99,111),(111,114,101,113)])
    self.assertIsNone(reentry_signal(x,100,0,'retest_next_open',P)['entry_i'])
 def test_no_buy_below_reclaimed_line(self):
    x=bs([(105,109,95,101),(99,103,96,102),(102,104,100,103)])
    self.assertEqual(reentry_signal(x,100,0,'reclaim_next_open',P)['status'],'NEXT_OPEN_ALREADY_BELOW_LINE_SKIP')
 def test_second_stop_immediate_broken_support(self):
    x=bs([(105,110,101,105),(102,110,95,109),(110,115,108,110)])
    i,p,reason=secondary_exit(x,100,1,'intraday_assumed_stop')
    self.assertEqual((i,p,reason),(1,100,'SECOND_STOP_INTRADAY_ASSUMED'))
 def test_second_stop_close_next_open(self):
    x=bs([(105,110,101,105),(106,110,97,99),(96,100,90,98)])
    self.assertEqual(secondary_exit(x,100,1,'close_confirm_next_open'),(2,96,'SECOND_STOP_CLOSE_NEXT_OPEN'))
 def test_second_stop_gap_uses_real_open(self):
    x=bs([(105,110,101,105),(95,110,92,99)])
    self.assertEqual(secondary_exit(x,100,1,'intraday_assumed_stop'),(1,95,'SECOND_STOP_GAP_OPEN'))
 def test_two_leg_cost_applies_again(self):
    bars=bs([(105,111,95,101),(103,111,101,107),(105,115,98,113)])
    r={'strategy':C,'date':'2025-01-01','code':'000001','year':'2025','method':'C_CONFIRMED_SWING_LOW',
       'exit_type':'intraday_assumed_stop','support':'100','support_risk_pct':'4.762','status':'VALID',
       'stop_net_pct':str((100/105-1)*100-0.2),'hold_net_pct':str((113/105-1)*100-0.2),
       'exit_reason':'STOP_INTRADAY_ASSUMED_AT_LEVEL','exit_date':'2025-01-01'}
    t=simulate_pair(r,bars,P)
    self.assertEqual(t['reclaim_reentry'],1)
    self.assertEqual(t['reclaim_second_stop'],1)
    # Initial trade loses 4.96%, second trade (103->100) loses 3.11% plus fee.
    expected=((1+float(r['stop_net_pct'])/100)*(1+((100/103-1)*100-0.2)/100)-1)*100
    self.assertAlmostEqual(t['reclaim_return_net_pct'],expected,5)
 def test_missing_reentry_keeps_initial_stop_only(self):
    x=bs([(105,109,98,99),(99,100,94,98),(98,99,92,93)])
    self.assertEqual(reentry_signal(x,100,0,'reclaim_next_open',P)['status'],'NO_RECLAIM_CLOSE_BEFORE_D30')
 def test_source_hash_tamper_fails_closed(self):
    if not N or not L:self.skipTest('upstream GitHub ZIPs supplied on GH runner')
    with tempfile.TemporaryDirectory() as tmp:
      q=Path(tmp)/'mutated.zip';q.write_bytes(Path(N).read_bytes()+b'x')
      with self.assertRaisesRegex(ValueError,'PINNED_UPSTREAM_ZIP_SHA_MISMATCH'):assert_source_integrity(q,L,P)
 def test_real_data_590_rows_and_reconciled_all_564(self):
    if not N or not L:self.skipTest('upstream GitHub ZIPs supplied on GH runner')
    n,lev,b,nm,lm=assert_source_integrity(N,L,P)
    self.assertEqual((len(n),len(lev),len(b)),(590,295,59))
    t=[simulate_pair(r,b[(r['strategy'],r['date'],r['code'])],P) for r in n]
    self.assertEqual(sum(r['calc_status']=='VALID' for r in t),564)
    self.assertEqual(sum(r.get('original_stop_triggered',0) for r in t),245)
 def test_real_analysis_determinism_and_sha(self):
    if not N or not L:self.skipTest('upstream GitHub ZIPs supplied on GH runner')
    with tempfile.TemporaryDirectory() as a,tempfile.TemporaryDirectory() as b:
      m1=audit(N,L,R/'R1D26O_PREDECLARED_PROTOCOL.json',a)
      m2=audit(N,L,R/'R1D26O_PREDECLARED_PROTOCOL.json',b)
      self.assertEqual(m1['files_sha256'],m2['files_sha256'])
      self.assertEqual(m1['n_N_eligible'],564)
      self.assertFalse(m1['strategy_ready'])
      self.assertFalse(m1['prospective_OOS'])
 def test_reentry_has_next_day_gap_and_no_lookahead(self):
    if not N or not L:self.skipTest('upstream GitHub ZIPs supplied on GH runner')
    n,_,b,_,_=assert_source_integrity(N,L,P)
    for r in n:
      t=simulate_pair(r,b[(r['strategy'],r['date'],r['code'])],P)
      if t['calc_status']!='VALID':continue
      for kind in ('reclaim','retest'):
        if t[f'{kind}_reentry']:
          d=[x['trade_date'] for x in b[(r['strategy'],r['date'],r['code'])]]
          self.assertGreater(d.index(t[f'{kind}_reentry_date']),d.index(t[f'{kind}_signal_date']))
          self.assertGreaterEqual(d.index(t[f'{kind}_signal_date']),t['original_exit_day_i'])
 def test_pinned_no_live_no_optimization(self):
    self.assertTrue(P['research_only'])
    self.assertFalse(P['strategy_ready']);self.assertFalse(P['prospective_OOS']);self.assertFalse(P['live_changed'])
    self.assertEqual(P['orders_placed'],0);self.assertEqual(P['max_reentries_per_signal'],1)
    self.assertEqual(P['retest_upper_tolerance_pct'],2)
    self.assertEqual(P['retest_max_observation_sessions'],5)

if __name__=='__main__':unittest.main()
