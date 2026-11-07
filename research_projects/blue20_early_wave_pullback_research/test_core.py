import unittest
import numpy as np
import pandas as pd
from core import indicators,observe,trade

class CausalTests(unittest.TestCase):
    def raw(self,n=520):
        dates=pd.bdate_range('2022-01-03',periods=n).strftime('%Y-%m-%d')
        close=100+np.sin(np.arange(n)/8)*2
        return pd.DataFrame(dict(code=['123456']*n,date=dates,open=close-.2,high=close+1,low=close-1,close=close,volume=np.full(n,1000.),amount=np.full(n,100000.)))
    def prepared(self,n=80):
        d=self.raw(n);d.date=pd.bdate_range('2023-01-02',periods=n).strftime('%Y-%m-%d');d['session']=np.arange(n)
        for k,v in dict(p0_condition=False,p1_signal=False,body=.2,prior_high5=110.,prior_amount5=100000.).items():d[k]=v
        return d
    def test_prefix_features_unchanged(self):
        d=self.raw();a=indicators(d);d.loc[450:,['open','low','high','close','volume','amount']]=[500,400,999,900,1e9,1e12];b=indicators(d)
        pd.testing.assert_frame_equal(a.iloc[:450],b.iloc[:450])
    def test_band_parity_and_shift(self):
        d=self.raw();a=indicators(d);expected=(d.close.rolling(20).mean()+2*d.close.rolling(20).std(ddof=0)).shift(26)
        np.testing.assert_allclose(a.UPPER,expected,equal_nan=True)
    def test_448_unknown_not_false(self):
        a=indicators(self.raw(250));self.assertTrue(a.video_alignment.eq('UNKNOWN').all())
    def test_p0_cannot_coincide_with_or_follow_raw_p1(self):
        d=self.raw();d.loc[350,['open','low','high','close','volume','amount']]=[100,99,116,115,10000,1200000]
        a=indicators(d);self.assertTrue(a.p1_raw.iloc[350]);self.assertFalse(a.p0_condition.iloc[350:371].any())
    def test_p0_exists_without_future_wave(self):
        d=self.prepared();d.loc[2,'p0_condition']=True;s,e,_=observe(d)
        self.assertEqual([x['stage'] for x in s],['P0']);self.assertEqual(e[0]['final_status'],'NO_P1_20D')
    def test_p0_right_censor_not_failure(self):
        d=self.prepared(10);d.loc[8,'p0_condition']=True;_,e,_=observe(d);self.assertEqual(e[0]['final_status'],'RIGHT_CENSORED')
    def test_p0_missing_days_not_no_signal(self):
        d=self.prepared();d.loc[2,'p0_condition']=True;d=d.drop(index=10).reset_index(drop=True);_,e,_=observe(d)
        self.assertEqual(e[0]['final_status'],'MISSING_BARS_UNVERIFIED')
    def test_p2_early_candidate_and_p3_later(self):
        d=self.prepared();d.loc[5,['p1_signal','open','low','high','close','body','volume']]=[True,100,99,116,115,15,10000]
        d.loc[6,['open','low','high','close','body','volume']]=[111,109,113,110,-1,1000]
        d.loc[7,['open','low','high','close','body','amount','prior_high5']]=[110,109.5,117,116,6,150000,115]
        s,_,_=observe(d);self.assertEqual([(x['stage'],x['index']) for x in s],[('P1',5),('P2',6),('P3',7)])
    def test_future_success_does_not_move_p2(self):
        d=self.prepared();d.loc[5,['p1_signal','low','high','body','volume']]=[True,90,120,15,10000]
        a,_,_=observe(d.iloc[:10]);d.loc[20:,'close']=999
        b,_,_=observe(d)
        self.assertEqual([(x['stage'],x['index']) for x in a],[(x['stage'],x['index']) for x in b if x['index']<10])
    def test_next_session_not_signal_open(self):
        d=self.prepared(30);d.loc[1,'open']=105;d.loc[1,['high','low','close']]=[106,104,105]
        r=trade(d,0,d.date.iloc[20],pd.Index(d.date));self.assertEqual(r['entry_price'],105);self.assertEqual(r['entry_date'],d.date.iloc[1])
    def test_stop_first_and_gap(self):
        d=self.prepared(30);d.loc[1,['open','high','low','close']]=[100,112,94,102]
        r=trade(d,0,d.date.iloc[20],pd.Index(d.date));self.assertEqual(r['reason'],'SL_BOTH_HIT');self.assertAlmostEqual(r['net_pct'],-5.25)
        d.loc[1,['high','low']]=[102,99];d.loc[2,['open','high','low','close']]=[90,92,88,91]
        self.assertEqual(trade(d,0,d.date.iloc[20],pd.Index(d.date))['exit_price'],90)
    def test_missing_entry_not_next_observed_bar(self):
        d=self.prepared(30);cal=pd.Index(d.date);d=d.drop(index=1).reset_index(drop=True)
        self.assertEqual(trade(d,0,cal[20],cal)['trade_status'],'MISSING_ENTRY_BAR')

if __name__=='__main__':unittest.main()
