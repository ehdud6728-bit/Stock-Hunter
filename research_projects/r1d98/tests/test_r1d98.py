import sys
import unittest
from pathlib import Path
import pandas as pd

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from state_machine import evaluate, enrich, pick


def candles(values,low=None,high=None):
    lows=low or [x*.98 for x in values]
    highs=high or [x*1.02 for x in values]
    return pd.DataFrame(dict(date=pd.date_range('2024-01-01',periods=len(values),freq='D'),
                              idx=range(len(values)),close=values,low=lows,high=highs))


def candidate(kind='RESISTANCE',value=100,**kwargs):
    r=dict(kind=kind,tier='SMALL',h1_idx=0,h2_idx=2,h2_confirm_idx=4,
           h1_value=value,h2_value=value,line_id='A',rank_score=32,
           inter_anchor_largest_one_day_drop_pct=-2,h2_upper_wick_pct=0)
    r.update(kwargs);return r


def state(r,d,k):
    return evaluate(r,d,str(d.iloc[k].date.date()))


class TestStateMachine(unittest.TestCase):
    def test_active_resistance_before_first_cross(self):
        d=candles([95,94,93,94,95,96,97,98]);s=state(candidate(),d,7)
        self.assertEqual(s['status'],'ACTIVE_RESISTANCE');self.assertTrue(s['eligible_primary'])
    def test_first_break_today_exactly_once(self):
        d=candles([96,96,96,97,98,101,102,103]);r=candidate()
        self.assertEqual(state(r,d,5)['status'],'FIRST_BREAK_TODAY')
        self.assertTrue(state(r,d,5)['fresh_break_today'])
        self.assertFalse(state(r,d,6)['fresh_break_today'])
        self.assertEqual(state(r,d,6)['first_event_date'],'2024-01-06')
    def test_old_break_then_below_is_archived(self):
        d=candles([95,95,95,95,96,102,103,98,97]);s=state(candidate(),d,8)
        self.assertEqual(s['status'],'HISTORIC_RESISTANCE_BACK_BELOW')
        self.assertFalse(s['eligible_primary'])
    def test_old_break_then_recross_not_fresh(self):
        d=candles([95,95,95,95,96,102,96,102]);s=state(candidate(),d,7)
        self.assertEqual(s['first_event_date'],'2024-01-06')
        self.assertFalse(s['fresh_break_today'])
    def test_retest_candidate_not_proven(self):
        d=candles([95,95,95,95,96,103,103,101],low=[94,94,94,94,95,102,102,99])
        s=state(candidate(),d,7)
        self.assertEqual(s['status'],'BREAKOUT_RETEST_SUPPORT_FLIP_WATCH')
        self.assertFalse(s['support_flip_proven'])
    def test_support_holds(self):
        d=candles([105,105,105,104,103,102,101]);s=state(candidate('SUPPORT'),d,6)
        self.assertEqual(s['status'],'ACTIVE_SUPPORT')
    def test_broken_support_archived(self):
        d=candles([105,105,105,104,103,99,98]);s=state(candidate('SUPPORT'),d,6)
        self.assertEqual(s['status'],'BROKEN_SUPPORT_BELOW')
        self.assertFalse(s['eligible_primary'])
    def test_broken_support_reclaim_not_auto_active(self):
        d=candles([105,105,105,104,103,98,102]);s=state(candidate('SUPPORT'),d,6)
        self.assertEqual(s['status'],'BROKEN_SUPPORT_RECLAIM_WATCH')
    def test_crash_bridge_quarantined(self):
        r=candidate(inter_anchor_largest_one_day_drop_pct=-13)
        self.assertEqual(state(r,candles([95]*7),6)['status'],'STRUCTURE_QUARANTINE')
    def test_wick_second_anchor_quarantined(self):
        r=candidate(h2_upper_wick_pct=9)
        self.assertEqual(state(r,candles([95]*7),6)['status'],'STRUCTURE_QUARANTINE')
    def test_explicit_confirm_delay(self):
        d=candles([96]*7)
        with self.assertRaises(ValueError):state(candidate(),d,3)
    def test_future_tamper_pit_identical(self):
        d=candles([95,95,95,95,96,101,100,99,98]);r=candidate()
        a=state(r,d,6);changed=d.copy();changed['close']=changed['close'].astype(float);changed.loc[7:,'close']*=1.2
        self.assertEqual(a,state(r,changed,6))
    def test_picker_excludes_quarantine_and_broken(self):
        d=candles([105,105,105,104,103,99,98])
        a=enrich([candidate('SUPPORT'),candidate('RESISTANCE',h2_upper_wick_pct=10)],d,'2024-01-07')
        s=pick(a);self.assertEqual(s['PRIMARY'],[]);self.assertEqual(s['WATCH'],[])
    def test_picker_no_mixed_watch_primary(self):
        d=candles([95,95,95,95,95,95,95])
        s=pick(enrich([candidate()],d,'2024-01-07'))
        self.assertEqual(len(s['PRIMARY']),1);self.assertEqual(len(s['WATCH']),0)
    def test_no_retest_same_day_as_first_break(self):
        d=candles([95,95,95,95,95,101]);s=state(candidate(),d,5)
        self.assertEqual(s['retest_touch_count'],0)
    def test_support_broken_at_confirmation_never_auto_active(self):
        d=candles([105,105,105,104,98,102,103]);r=candidate('SUPPORT')
        self.assertEqual(state(r,d,4)['status'],'SUPPORT_BROKEN_AT_CONFIRMATION')
        self.assertEqual(state(r,d,6)['status'],'BROKEN_SUPPORT_RECLAIM_WATCH')
    def test_resistance_above_at_confirmation_is_not_new(self):
        d=candles([94,94,94,98,102,103,102]);r=candidate()
        self.assertEqual(state(r,d,4)['status'],'ABOVE_AT_CONFIRMATION_REFERENCE')
        self.assertFalse(state(r,d,4)['fresh_break_today'])
        self.assertNotEqual(state(r,d,6)['status'],'ACTIVE_RESISTANCE')
    def test_unknown_kind_rejected(self):
        with self.assertRaises(ValueError):state(candidate('FAKE'),candles([95]*7),6)


if __name__=='__main__':unittest.main()
