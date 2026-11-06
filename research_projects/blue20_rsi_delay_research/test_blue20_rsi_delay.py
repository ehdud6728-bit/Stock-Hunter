import sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import numpy as np
import pandas as pd
from blue20_rsi_delay import (cutler_rsi14,wilder_rsi14,first_gc_delay,simulate_anchored,
                             delay_table,cutoff_table,run_rsi_analysis,add_rsi_features)

class TestRsiDelay(unittest.TestCase):
    def setUp(self):
        self.days=pd.bdate_range('2023-01-02', periods=40)
        self.g=pd.DataFrame({'open':100.,'high':100.,'low':100.,'close':100.},index=self.days)

    def test_rsi_flat_is_50(self):
        z=pd.Series([100.]*50)
        self.assertEqual(cutler_rsi14(z).iloc[-1],50.)
        self.assertEqual(wilder_rsi14(z).iloc[-1],50.)

    def test_rsi_up_is_100_down_is_zero(self):
        self.assertEqual(cutler_rsi14(pd.Series(range(50))).iloc[-1],100.)
        self.assertEqual(wilder_rsi14(pd.Series(range(50))).iloc[-1],100.)
        self.assertEqual(cutler_rsi14(pd.Series(range(50,0,-1))).iloc[-1],0.)
        self.assertEqual(wilder_rsi14(pd.Series(range(50,0,-1))).iloc[-1],0.)

    def test_wilder_initial_seed(self):
        z=pd.Series([100.,101.,99.,103.,100.,105.,104.,107.,105.,110.,109.,112.,110.,113.,111.])
        v=wilder_rsi14(z)
        ch=z.diff().iloc[1:15];gain=ch.clip(lower=0).mean();loss=(-ch.clip(upper=0)).mean()
        self.assertAlmostEqual(v.iloc[14],100-100/(1+gain/loss),places=8)
        self.assertTrue(v.iloc[:14].isna().all())

    def _cross_group(self,delay=None,already=False):
        self.g['CUTLER_SMA9_RSI']=48.
        self.g['CUTLER_SMA9_SIGNAL']=50.
        self.g['CUTLER_SMA9_GC']=False
        if already: self.g.loc[self.days[0:11],'CUTLER_SMA9_RSI']=55.
        if delay is not None:
            self.g.loc[self.days[delay],'CUTLER_SMA9_RSI']=55.
            self.g.loc[self.days[delay],'CUTLER_SMA9_GC']=True
        return self.g

    def test_exact_D0_GC(self):
        g=self._cross_group(0)
        d,status,*_=first_gc_delay(g,self.days[:11],'CUTLER_SMA9')
        self.assertEqual(d,0);self.assertEqual(status,'GC_OBSERVED')

    def test_exact_D3_GC_not_D0(self):
        g=self._cross_group(3)
        d,status,*_=first_gc_delay(g,self.days[:11],'CUTLER_SMA9')
        self.assertEqual(d,3);self.assertEqual(status,'GC_OBSERVED')

    def test_no_gc_above_is_not_cross(self):
        g=self._cross_group(already=True)
        d,status,*_=first_gc_delay(g,self.days[:11],'CUTLER_SMA9')
        self.assertIsNone(d);self.assertEqual(status,'ALREADY_ABOVE_NO_NEW_GC')

    def test_signal_incomplete_not_no_gc(self):
        g=self._cross_group();g.loc[self.days[3],'CUTLER_SMA9_SIGNAL']=np.nan
        d,status,*_=first_gc_delay(g,self.days[:11],'CUTLER_SMA9')
        self.assertIsNone(d);self.assertEqual(status,'RSI_INSUFFICIENT')

    def test_GC_close_D3_trades_only_D4_through_D20(self):
        g=self.g.copy()
        g.loc[self.days[1],'low']=1 # before GC, should not be treated as later stop
        g.loc[self.days[3],'close']=100
        g.loc[self.days[4],'high']=111
        result=simulate_anchored(g,self.days,0,3,'GC_CLOSE')
        self.assertEqual(result['entry_date'],self.days[3].strftime('%Y-%m-%d'))
        self.assertEqual(result['exit_offset'],4)
        self.assertEqual(result['exit_reason'],'TP')
        self.assertAlmostEqual(result['net_pct'],9.75)

    def test_GC_next_open_D3_includes_D4_same_session(self):
        g=self.g.copy()
        g.loc[self.days[4],['open','high','low','close']]=[105.,116.,99.,106.]
        result=simulate_anchored(g,self.days,0,3,'GC_NEXT_OPEN')
        self.assertEqual(result['entry_date'],self.days[4].strftime('%Y-%m-%d'))
        self.assertEqual(result['exit_reason'],'SL_BOTH_HIT')
        self.assertTrue(result['ambiguous'])
        self.assertAlmostEqual(result['net_pct'],-5.25)

    def test_gc_close_d3_horizon_fixed_d0_d20(self):
        g=self.g.copy()
        g.loc[self.days[20],'close']=102
        g.loc[self.days[21],'high']=115
        r=simulate_anchored(g,self.days,0,3,'GC_CLOSE')
        self.assertEqual(r['exit_offset'],20)
        self.assertEqual(r['exit_reason'],'TIME_D20')
        self.assertAlmostEqual(r['net_pct'],1.75)

    def test_cutoff_denominator_uses_all_candidates(self):
        rows=[]
        for i,d in enumerate([0,3,None,None]):
            rows.append({'rsi_mode':'CUTLER_SMA9','band':'UPPER','label':'CLEAR_BODY',
                         'gc_offset':d,'d0_base_net_pct':[-1,2,3,4][i],
                         'GC_CLOSE_net_pct':[4,6,np.nan,np.nan][i],
                         'GC_NEXT_OPEN_net_pct':[3,5,np.nan,np.nan][i],
                         'GC_CLOSE_exit_reason':['TP','SL',None,None][i]})
        z=cutoff_table(pd.DataFrame(rows))
        t=z[z.cutoff_days==3].iloc[0]
        self.assertEqual(t.candidate_n,4)
        self.assertEqual(t.trade_n,2)
        self.assertEqual(t.no_trade_n,2)
        self.assertAlmostEqual(t.trade_rate_pct,50)
        self.assertAlmostEqual(t.universe_contribution_gc_close_pct,2.5)
        self.assertAlmostEqual(t.trading_only_gc_close_net_mean_pct,5.)

    def test_full_smoke_empty_is_valid(self):
        days=pd.bdate_range('2023-01-02', periods=75)
        v=100+np.linspace(0,7,len(days))
        ohlcv=pd.DataFrame(dict(code='000001',date=days,open=v,high=v+1,low=v-1,close=v))
        ev,count=run_rsi_analysis(ohlcv,days)
        self.assertTrue(ev.empty)
        self.assertIsInstance(count,dict)

if __name__=='__main__':unittest.main()
