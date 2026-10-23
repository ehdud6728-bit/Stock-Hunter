import unittest,sys,inspect
from pathlib import Path
import numpy as np
import pandas as pd
from research_formulas.formula66_r1d5_support_reaction import _roll_indicators, _calc_behavior, one_episode, BANDS
from research_formulas import formula66_r1d5_support_reaction as lib

class SupportResearchTests(unittest.TestCase):
    def setUp(self):
        nd=330
        self.dates=pd.date_range('2025-01-02',periods=nd,freq='B').strftime('%Y-%m-%d')
        close=np.full(nd,100.0)
        # Freeze trough index 320, peak 325, signal 329: valid event date selection
        close[320]=99;close[325]=130;close[326]=115;close[327]=96;close[328]=100;close[329]=106
        self.df=pd.DataFrame({'open':close.copy(),'high':close+2,'low':close-2,'close':close,
                              'volume':np.full(nd,1000.),'amount':np.full(nd,1000000.)},index=self.dates)
        self.df.loc[self.dates[320],'low']=95
        self.df.loc[self.dates[325],'high']=132
        self.df=_roll_indicators(self.df)
        self.ep={'date':self.dates[329],'code':'012345','wave_peak_date':self.dates[325],
                 'wave_trough_date':self.dates[320]}

    def test_01_bb20_formula_ddof0(self):
        w=self.df.close.iloc[-20:]
        expect=w.mean()-2*w.std(ddof=0)
        self.assertAlmostEqual(self.df['BB20_L'].iloc[-1],expect)

    def test_02_bb40_formula_ddof0(self):
        w=self.df.close.iloc[-40:]
        self.assertAlmostEqual(self.df['BB40_L'].iloc[-1],w.mean()-2*w.std(ddof=0))

    def test_03_env20_10pct(self):
        self.assertAlmostEqual(self.df['ENV20_L_10'].iloc[-1],self.df.MA20.iloc[-1]*.90)

    def test_04_env40_10pct(self):
        self.assertAlmostEqual(self.df['ENV40_L_10'].iloc[-1],self.df.MA40.iloc[-1]*.90)

    def test_05_no_future_bar_leak(self):
        a=one_episode(self.df,self.ep)
        future=self.df.copy()
        nextday='2027-01-04'
        future.loc[nextday, ['open','high','low','close','volume','amount']]=[100000,100000,1,1,300000,1]
        future=_roll_indicators(future)
        b=one_episode(future,self.ep)
        for field in BANDS:
            self.assertEqual(a[field+'_touch_type'],b[field+'_touch_type'])
            self.assertEqual(a[field+'_signal_above'],b[field+'_signal_above'])
        self.assertEqual(a['pullback_intraday_max_retrace_pct'],b['pullback_intraday_max_retrace_pct'])

    def test_06_break_reclaim_only_before_signal(self):
        lib.DF_CTX={'close':np.array([120.,90.,110.]),'low':np.array([118.,88.,95.]),'high':np.array([125.,95.,112.])}
        r=_calc_behavior('TEST',np.array([100.,100.,100.]),0,2)
        self.assertEqual(r['TEST_reclaim'],1)
        self.assertEqual(r['TEST_hold'],0)

    def test_07_no_close_break_is_hold(self):
        lib.DF_CTX={'close':np.array([120.,101.,110.]),'low':np.array([118.,99.,95.]),'high':np.array([125.,111.,112.])}
        r=_calc_behavior('TEST',np.array([100.,100.,100.]),0,2)
        self.assertEqual(r['TEST_hold'],1)

    def test_08_signal_below_is_fail(self):
        lib.DF_CTX={'close':np.array([120.,102.,80.]),'low':np.array([118.,99.,77.]),'high':np.array([125.,111.,112.])}
        r=_calc_behavior('TEST',np.array([100.,100.,100.]),0,2)
        self.assertEqual(r['TEST_fail'],1)

    def test_09_peak_below_not_support_from_above(self):
        lib.DF_CTX={'close':np.array([90.,80.,110.]),'low':np.array([89.,70.,95.]),'high':np.array([110.,95.,112.])}
        r=_calc_behavior('TEST',np.array([100.,100.,100.]),0,2)
        self.assertEqual(r['TEST_touch_from_above'],0)
        self.assertEqual(r['TEST_touch_type'],'PEAK_BELOW_LEVEL')

    def test_10_wave_low_retrace_is_intraday_not_close(self):
        a=one_episode(self.df,self.ep)
        self.assertNotEqual(a['pullback_intraday_max_retrace_pct'],a['pullback_close_retrace_pct'])

    def test_11_study_no_trade(self):
        code=inspect.getsource(lib)
        self.assertNotIn('send_order(',code)
        self.assertNotIn('LIVE_ORDER',code)

    def test_12_asof_features_dont_contain_group_or_outcome(self):
        d=one_episode(self.df,self.ep)
        self.assertEqual(d['status_feature'],'ASOF_OK')
        self.assertFalse(any(x in d for x in ('group','net_20bps_pct','future_return')))

    def test_13_missing_signal_fails_closed(self):
        d=one_episode(self.df,{**self.ep,'date':'2099-01-01'})
        self.assertEqual(d['status_feature'],'MISSING_EVENT_PRICE_DAY')

    def test_14_support_level_names_separated(self):
        self.assertEqual(len(BANDS),14)
        self.assertIn('WAVE_LOW',BANDS)
        self.assertIn('BB40_L',BANDS)
        self.assertIn('ENV20_L_10',BANDS)

    def test_16_exclusive_nearest_support_features_asof(self):
        x=one_episode(self.df,self.ep)
        self.assertIn('postpeak_low_nearest_support_within2pct',x)
        self.assertIn('postpeak_low_support_confluence_count_within2pct',x)
        self.assertNotIn('net_20bps_pct',x)

    def test_15_asof_one_episode_no_trades(self):
        r=one_episode(self.df,self.ep)
        self.assertEqual(r['code'],'012345')
        self.assertIn('FIB618_touch_from_above',r)
        self.assertNotIn('orders',r)

if __name__=='__main__':unittest.main()
