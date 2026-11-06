from __future__ import annotations
import sys,unittest,tempfile
from pathlib import Path
import numpy as np
import pandas as pd
HERE=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(HERE))
from analyze_ma import calc_indicators
from post_entry_flow import add_bar_features, simulate_fixed_or_structural, load_optional_investor_pit


def sample(n=335):
    p=np.linspace(400,540,n)
    d=pd.DataFrame({'date':pd.date_range('2022-01-01',periods=n,freq='B').strftime('%Y-%m-%d'),
                    'open':p-1,'high':p+3,'low':p-2,'close':p,'volume':np.full(n,1000.),
                    'amount':np.full(n,500000.)})
    return d


class FlowCandlePointInTime(unittest.TestCase):
    def test_candle_morphology_and_prior20_volume(self):
        d=sample();d.loc[310,['open','high','low','close','amount']]=[100,112,96,111,1000000]
        f=add_bar_features(calc_indicators(d))
        self.assertEqual(f.loc[310,'candle_shape'],'BIG_BULL')
        self.assertAlmostEqual(f.loc[310,'amount_ratio20'],2.0,places=9)
        self.assertGreater(f.loc[310,'close_location'],0.9)
        self.assertTrue(f.loc[310,'bull'])

    def test_lower_wick_shape(self):
        d=sample();d.loc[310,['open','high','low','close']]=[100,103,80,101]
        f=add_bar_features(calc_indicators(d))
        self.assertEqual(f.loc[310,'candle_shape'],'LONG_LOWER_WICK')
        self.assertTrue(f.loc[310,'long_lower_wick'])

    def test_no_future_data_in_todays_signal(self):
        d=sample();a=add_bar_features(calc_indicators(d))
        d.loc[334,['open','high','low','close','amount','volume']]=[2000,2200,1900,2000,1e10,1e10]
        b=add_bar_features(calc_indicators(d))
        fields=['candle_shape','amount_ratio20','volume_ratio20','band_width_pct',
                'shifted_band_squeeze','heavy_down_proxy','pre_next_session_soft_stop_deferral']
        for fld in fields:
            x=a.loc[333,fld];y=b.loc[333,fld]
            if isinstance(x,(str,np.str_)):self.assertEqual(x,y)
            elif pd.isna(x):self.assertTrue(pd.isna(y))
            else:self.assertAlmostEqual(float(x),float(y),places=9)

    def test_same_day_candle_cannot_cancel_intra_day_stop(self):
        d=sample();d.loc[301,['open','high','low','close']]=[100,103,98,100];d.loc[302,['open','high','low','close']]=[100,110,94,106]
        a=add_bar_features(calc_indicators(d))
        a.loc[301,'pre_next_session_soft_stop_deferral']=False
        a.loc[302,'pre_next_session_soft_stop_deferral']=True
        # next-open entry on 301; stop 95 from entry=100 on following day 302.
        x=simulate_fixed_or_structural(a,d0_idx=300,entry_idx=301,entry_price=100,structural=True)
        self.assertEqual(x['exit_date'],str(a.date.iloc[302]))
        self.assertEqual(x['exit_reason'],'SOFT_STOP')
        self.assertAlmostEqual(x['net_pct'],-5.25)

    def test_prev_close_can_defer_soft_but_hard_active(self):
        d=sample();d.loc[301,['open','high','low','close']]=[100,103,98,100];d.loc[302,['open','high','low','close']]=[100,105,94,100]
        d.loc[303,['open','high','low','close']]=[100,105,87,100]
        a=add_bar_features(calc_indicators(d))
        a.loc[301,'pre_next_session_soft_stop_deferral']=True
        a.loc[302,'pre_next_session_soft_stop_deferral']=True
        x=simulate_fixed_or_structural(a,d0_idx=300,entry_idx=301,entry_price=100,structural=True)
        self.assertEqual(x['exit_reason'],'HARD_STOP')
        self.assertEqual(x['exit_date'],str(a.date.iloc[303]))
        self.assertAlmostEqual(x['net_pct'],-12.25)

    def test_gap_below_hard_stop_fills_open_not_stated_stop(self):
        d=sample();d.loc[301,['open','high','low','close']]=[100,103,98,100];d.loc[302,['open','high','low','close']]=[86,90,85,88]
        a=add_bar_features(calc_indicators(d));a.loc[301,'pre_next_session_soft_stop_deferral']=True
        x=simulate_fixed_or_structural(a,300,301,100,True)
        self.assertEqual(x['exit_reason'],'HARD_GAP')
        self.assertAlmostEqual(x['net_pct'],-14.25)

    def test_soft_stop_gap_without_prior_deferral_must_use_gap_open(self):
        d=sample();d.loc[301,['open','high','low','close']]=[100,103,98,100]
        d.loc[302,['open','high','low','close']]=[92,95,91,93]
        a=add_bar_features(calc_indicators(d))
        a.loc[301,'pre_next_session_soft_stop_deferral']=False
        x=simulate_fixed_or_structural(a,300,301,100,True)
        self.assertEqual(x['exit_reason'],'SOFT_GAP')
        self.assertAlmostEqual(x['net_pct'],-8.25)

    def test_both_intraday_tp_and_stop_uses_stop_first(self):
        d=sample();d.loc[301,['open','high','low','close']]=[100,103,98,100];d.loc[302,['open','high','low','close']]=[100,114,94,108]
        a=add_bar_features(calc_indicators(d))
        x=simulate_fixed_or_structural(a,300,301,100,False)
        self.assertEqual(x['exit_reason'],'SL_BOTH_HIT')
        self.assertTrue(x['ambiguous_same_day'])

    def test_candle_complete_not_known_at_open(self):
        d=sample();d.loc[310,['open','high','low','close']]=[100,110,86,104]
        a=add_bar_features(calc_indicators(d))
        self.assertTrue(a.loc[310,'long_lower_wick'])
        # The earlier day had no information about today's wick.
        self.assertFalse(a.loc[309,'long_lower_wick'])

    def test_investor_pit_requires_known_at_and_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            f=Path(tmp)/'investor.csv'
            f.write_text('code,date,known_at,foreign_net_krw,institution_net_krw,retail_net_krw,source_id\n950130,2024-07-23,2024-07-24,100,200,-300,archived_KRX_01\n',encoding='utf-8')
            t=load_optional_investor_pit(f)
            self.assertEqual(t.loc[('950130','2024-07-23')].known_at,'2024-07-24')
            f.write_text('code,date,foreign_net_krw,institution_net_krw,retail_net_krw,source_id\n950130,2024-07-23,100,200,-300,archived_KRX_01\n')
            with self.assertRaisesRegex(ValueError,'INVESTOR_PIT_SCHEMA_MISSING'):
                load_optional_investor_pit(f)

    def test_amount_history_missing_not_fake_zero_ratio(self):
        d=sample(n=45);f=add_bar_features(calc_indicators(d))
        self.assertTrue(pd.isna(f.loc[1,'amount_ratio20']))
        self.assertFalse(bool(f.loc[1,'pre_next_session_soft_stop_deferral']))

if __name__=='__main__':unittest.main()
