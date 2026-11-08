import unittest
import pandas as pd
import numpy as np
from engine import detect,simulate,candidate,last_fresh_pivot,retry_candidate

class TimingTests(unittest.TestCase):
    def d(self,n=85):
        dates=pd.bdate_range('2023-01-02',periods=n).strftime('%Y-%m-%d')
        d=pd.DataFrame(dict(date=dates,open=[100.]*n,high=[102.]*n,low=[99.]*n,close=[101.]*n,volume=[1000.]*n,amount=[100000.]*n))
        d['body']=d.close-d.open;d['prior_atr20']=2.;d['prior_amount5']=100000.;d['prior_high5']=105.;d['blue_UPPER']=False;d['blue_LOWER']=False;d['rsi_gc']=False
        d.loc[0,['open','low','high','close','volume','body']]=[90,89,110,109,10000,19]
        return d
    def test_same_confirmation_different_stops(self):
        d=self.d();cal=pd.Index(d.date);e=candidate(d,2,98,1,'LOW');d.loc[3,['open','high','low','close']]=[100,101,97,100]
        a=simulate(d,0,e,cal,'FIXED5');b=simulate(d,0,e,cal,'PULLBACK_LOW')
        self.assertEqual(a['entry_date'],b['entry_date']);self.assertEqual(b['reason'],'SL');self.assertEqual(b['exit_price'],98)
    def test_gap_stop_at_real_open(self):
        d=self.d();d.loc[2,['open','high','low','close']]=[90,92,88,91];e=candidate(d,0,95,0,'LOW')
        r=simulate(d,0,e,pd.Index(d.date),'PULLBACK_LOW');self.assertEqual((r['reason'],r['exit_price']),('SL_GAP',90))
    def test_same_bar_stop_first(self):
        d=self.d();d.loc[1,['open','high','low','close']]=[100,112,94,101]
        r=simulate(d,0,candidate(d,0,95,0,'LOW'),pd.Index(d.date),'PULLBACK_LOW');self.assertEqual(r['reason'],'SL_BOTH_HIT')
    def test_stop_at_entry_not_removed_as_profitable_trade(self):
        d=self.d();e=candidate(d,0,101,0,'LOW');r=simulate(d,0,e,pd.Index(d.date),'PULLBACK_LOW')
        self.assertEqual(r['status'],'INVALID_STOP_AT_ENTRY');self.assertFalse(r['entered'])
    def test_missing_next_market_bar_not_next_observed(self):
        d=self.d();cal=pd.Index(d.date);e=candidate(d,0,95,0,'LOW');d=d.drop(index=1).reset_index(drop=True)
        self.assertEqual(simulate(d,0,e,cal,'FIXED5')['status'],'MISSING_ENTRY_BAR')
    def test_late_entry_gets_fresh_horizon_but_old_deadline_skips(self):
        d=self.d();e=candidate(d,25,95,24,'LOW');cal=pd.Index(d.date)
        self.assertTrue(simulate(d,0,e,cal,'FIXED5','FRESH20')['closed']);self.assertEqual(simulate(d,0,e,cal,'FIXED5','OLD20')['status'],'ENTRY_AFTER_DEADLINE')
    def test_stop_immutable_when_later_low_is_lower(self):
        d=self.d();e=candidate(d,0,95,0,'LOW');d.loc[2,'low']=1
        r=simulate(d,0,e,pd.Index(d.date),'PULLBACK_LOW');self.assertEqual(r['stop_price'],95);self.assertEqual(r['exit_price'],95)
    def test_wait_confirmation_prefix_unchanged(self):
        d=self.d();d.loc[4,'blue_UPPER']=True;d.loc[4,'rsi_gc']=True
        a,_,_=detect(d.iloc[:12],0,pd.Index(d.date[:12]),{'low':103,'high':105})
        d.loc[12:,['open','high','low','close']]=[1000,9999,900,9999]
        b,_,_=detect(d,0,pd.Index(d.date),{'low':103,'high':105})
        for key,event in a.items():self.assertEqual(event,b[key])
    def test_future_gc_not_backdated_to_blue(self):
        d=self.d();d.loc[4,'blue_UPPER']=True;d.loc[7,'rsi_gc']=True
        a,_,_=detect(d,0,pd.Index(d.date));self.assertEqual(a['D_NEW_BLUE']['confirm_date'],d.date.iloc[4]);self.assertEqual(a['D_BLUE_RSI']['confirm_date'],d.date.iloc[7]);self.assertEqual(a['D_BLUE_RSI']['followup_gc_offset'],3)
    def test_newly_broken_zone_can_retest_after_d0(self):
        d=self.d();d.loc[1,['open','high','low','close','body']]=[106,108,104,107,1];d.loc[2,['open','high','low','close','body']]=[107,110,104.5,108,1]
        events,_,_=detect(d,0,pd.Index(d.date),{'low':103,'high':105})
        self.assertEqual(events['C_RETEST']['confirm_date'],d.date.iloc[2]);self.assertEqual(events['C_RETEST']['stop_low'],104)
    def test_fresh_pivot_requires_right_two_completed_bars(self):
        d=self.d();d.loc[7,'low']=95
        self.assertIsNone(last_fresh_pivot(d,8,7));self.assertEqual(last_fresh_pivot(d,9,7),(95.,7))
    def test_retry_cannot_reuse_old_low_before_stop(self):
        d=self.d();d.loc[4,'low']=90;d.loc[10,'blue_UPPER']=True
        self.assertIsNone(retry_candidate(d,d.date.iloc[7],0,pd.Index(d.date)))
    def test_same_bar_high_does_not_invent_pullback(self):
        d=self.d();d.loc[1,['open','high','low','close','body']]=[111,150,110,149,38]
        _,_,trace=detect(d.iloc[:2],0,pd.Index(d.date[:2]));self.assertFalse(any(x['state']=='PULLBACK_OBSERVED' for x in trace))

if __name__=='__main__':unittest.main()
