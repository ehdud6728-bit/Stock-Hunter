import sys
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from analyze_candle_exhaustion import (candle_features,d0_features,d1_features,
    high_risk_d0,bin_numeric,screen_baseline,paired_policy_summary)
from post_entry_flow import simulate_fixed_or_structural


class CandlePITTests(unittest.TestCase):
    @staticmethod
    def frame(n=280):
        # Realistic consistency: highs/lows always envelop open and close.
        base=100+np.arange(n)*.025
        closes=base+np.cos(np.arange(n)*.28)*.7
        opens=base+np.sin(np.arange(n)*.35)*.6
        d=pd.DataFrame(dict(date=pd.date_range('2022-01-01',periods=n,freq='B').strftime('%Y-%m-%d'),
                  open=opens,close=closes,high=np.maximum(opens,closes)+1.1,
                  low=np.minimum(opens,closes)-1.3,volume=1e5+np.arange(n)*6,
                  amount=1e9+np.arange(n)*2e5))
        for lag in [5,20,60,112,224]:
            d[f'ma{lag}']=d.close.rolling(lag,min_periods=lag).mean()
            for k in [5,10,20]:
                d[f'ma{lag}_slope{k}']=100*(d[f'ma{lag}']/d[f'ma{lag}'].shift(k)-1)
        av=d.close.rolling(20,min_periods=20).mean()
        st=d.close.rolling(20,min_periods=20).std(ddof=0)
        d['bb20_upper_shift26']=(av+2*st).shift(26)
        d['bb20_lower_shift26']=(av-2*st).shift(26)
        return d

    def test_lookahead_does_not_change_d0(self):
        d=self.frame();a=candle_features(d)
        altered=d.copy();altered.loc[260:,['open','high','low','close','amount','volume']]*=2
        b=candle_features(altered)
        for col in ['atr20','body_abs_atr20','prior20_return_pct','amount_ratio20','prev20_high','d0_not_a_real_column'][:-1]:
            self.assertAlmostEqual(float(a.loc[250,col]),float(b.loc[250,col]),places=8)

    def test_amount_ratio_uses_prior_not_current(self):
        d=self.frame()
        d.loc[250,'amount']=1e12
        x=candle_features(d)
        past=d.amount.iloc[230:250].mean()
        self.assertAlmostEqual(x.amount_ratio20.iloc[250],1e12/past)

    def test_atr_body_and_wick_are_finite(self):
        d=candle_features(self.frame())
        for c in ['atr20','body_abs_atr20','range_atr20','upper_wick_to_range','close_location']:
            self.assertTrue(np.isfinite(d.loc[270,c]))

    def test_d1_requires_close_then_d2_entry(self):
        d=candle_features(self.frame())
        d.loc[260,'open']=100;d.loc[260,'close']=110;d.loc[260,'high']=111;d.loc[260,'low']=99
        # Recompute close-dependent features and source band's history.
        d=candle_features(d)
        feat=d1_features(d,261,260)
        self.assertEqual(feat['d1_date'],d.date.iloc[261])
        self.assertEqual(feat['d1_mid_body_hold'],d.close.iloc[261]>=105)
        self.assertTrue(feat['d1_day_only_eod_known'])
        self.assertNotIn('d2_close',feat)

    def test_strict_highrisk_requires_all_three_predeclared_gates(self):
        r=dict(d0_amount_ratio20=13,d0_ma60_slope10=-.1,d0_ma112_slope10=-.2)
        self.assertTrue(high_risk_d0(r))
        for c in list(r):
            z=r.copy();z[c]=0
            self.assertFalse(high_risk_d0(z))

    def test_bins_thresholds_are_predeclared(self):
        self.assertEqual(bin_numeric(10,[-np.inf,5,10,15,np.inf],['<5','5-10','10-15','15+']),'10-15')
        self.assertEqual(bin_numeric(float('nan'),[-np.inf,0,np.inf],['A','B']),'PIT_MISSING')

    def test_stop_first_d2_and_gap_closes(self):
        d=candle_features(self.frame())
        e=250
        d.loc[e+2,['open','high','low','close']]=[100,112,94,105]
        d.loc[e+3,['open','high','low','close']]=[80,82,75,78]
        f=simulate_fixed_or_structural(d,e,e+2,100,structural=False)
        self.assertEqual(f['exit_reason'],'SL_BOTH_HIT')
        self.assertAlmostEqual(f['net_pct'],-5.25)
        # Gap below stop on the second day must be filled at gap open.
        d.loc[e+2,['open','high','low','close']]=[100,101,99,100]
        g=simulate_fixed_or_structural(d,e,e+2,100,structural=False)
        self.assertEqual(g['exit_reason'],'SL_GAP')
        self.assertAlmostEqual(g['net_pct'],-20.25)

    def test_reference_counts_fail_closed(self):
        with self.assertRaisesRegex(ValueError,'BASELINE_EVENT_COUNT_INVALID'):
            screen_baseline(pd.DataFrame({'code':['000001'],'date':['2023-01-01'],'band':['UPPER'],'year':[2023]}))

    def test_nontrades_denominator_not_hidden(self):
        # A toy D0 subset illustrates selected-trade ROI != opportunity ROI.
        toy=pd.DataFrame({
            'code':['000001','000002'],'year':[2023,2024],'band':['UPPER','UPPER'],'gc_offset':[0,0],
            'd0_high_risk_turnover_ma_down':[True,False],
            'baseline_net_pct':[-5.25,9.75],
        })
        for m in ['D1_CLOSE_HOLD_BODY_MID','D1_CLOSE_HOLD_UPPER_BAND','D1_BOTH_MID_AND_UPPER','D1_CLOSE_ABOVE_D0_HIGH']:
            toy[m+'_enter']=[True,False]
            toy[m+'_net_pct_per_opportunity']=[2.0,0.0]
        a=paired_policy_summary(toy)
        x=a[(a.cohort=='ALL_UPPER_D0_GC')&(a.policy=='D1_CLOSE_HOLD_BODY_MID')].iloc[0]
        self.assertEqual(x.executed_trades,1)
        self.assertEqual(x.opportunities,2)
        self.assertAlmostEqual(x.policy_all_opportunities_net_pct,1.0)
        self.assertAlmostEqual(x.policy_per_executed_trade_net_pct,2.0)


if __name__=='__main__':unittest.main()

# The separate filter audit always counts skipped winners as missed opportunities.
class FilterAuditTests(unittest.TestCase):
    def test_avoid_lookahead_in_prefilter_and_count_lost_winners(self):
        from analyze_candle_exhaustion import filter_policy_summary
        toy=pd.DataFrame(dict(code=['000001','000002','000003'],year=[2023,2023,2024],
            band=['UPPER']*3,gc_offset=[0,0,1],d0_return_pct=[30,4,5],
            d0_body_abs_prior_atr20=[5,.5,.4],d0_amount_ratio20=[20,1,1],
            d0_close_over_ma60_pct=[21,2,3],d0_high_risk_turnover_ma_down=[True,False,False],
            baseline_net_pct=[9.75,-5.25,-5.25],baseline_tp=[True,False,False],
            baseline_stop=[False,True,True],baseline_mfe_pct=[40,3,4]))
        t=filter_policy_summary(toy)
        r=t[(t.cohort=='ALL_UPPER')&(t.policy=='SKIP_D0_GAIN_GE20')].iloc[0]
        self.assertEqual(r.skips,1)
        self.assertEqual(r.skipped_original_TP,1)
        self.assertAlmostEqual(r.filter_all_opportunity_avg_pct,-3.5)

class BuildTableSmoke(unittest.TestCase):
    def test_single_upper_d0_event_full_pipeline_with_mock_reference_count(self):
        from unittest.mock import patch
        from analyze_candle_exhaustion import build_table
        d=CandlePITTests.frame(300)
        d.loc[255,['open','high','low','close']]=[106.,113.,105.,112.]
        d=candle_features(d)
        entry=256
        base=simulate_fixed_or_structural(d,255,entry,float(d.open.iloc[entry]),False)
        ev=pd.DataFrame([dict(code='000001',date=d.date.iloc[255],year=2023,band='UPPER',
            gc_offset=0,gc_date=d.date.iloc[255],GC_NEXT_OPEN_entry_date=d.date.iloc[entry],
            GC_NEXT_OPEN_entry_price=d.open.iloc[entry],GC_NEXT_OPEN_net_pct=base['net_pct'],
            GC_NEXT_OPEN_exit_reason=base['exit_reason'],GC_NEXT_OPEN_exit_date=base['exit_date'],
            GC_NEXT_OPEN_mfe_pct=6.,GC_NEXT_OPEN_mae_pct=-3.,d0_close=d.close.iloc[255],
            d0_amount_ratio=d.amount_ratio20.iloc[255],gc_market_regime='MIXED',
            wave_state='NOT_WAVE_PULLBACK',stoch_state_gc='ABOVE_AND_RISING')])
        with patch('analyze_candle_exhaustion.screen_baseline',lambda _:None):
            result=build_table(ev,{'000001':d})
        self.assertEqual(len(result),1)
        self.assertEqual(result.iloc[0].d1_date,str(d.date.iloc[256]))
        self.assertEqual(result.iloc[0].gc_offset,0)
        self.assertIn('D1_BOTH_MID_AND_UPPER_net_pct_per_opportunity',result.columns)
        self.assertIn('d0_body_abs_prior_atr20',result.columns)
