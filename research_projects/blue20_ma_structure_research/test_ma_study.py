from __future__ import annotations
import sys,unittest, tempfile
from pathlib import Path
import numpy as np
import pandas as pd
HERE=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(HERE))
from analyze_ma import calc_indicators,characterize,stats,select_cases,sha256

class TechnicalTest(unittest.TestCase):
    def test_sma_no_future_price_leakage(self):
        p=np.linspace(100,150,400)
        d=pd.DataFrame({'date':pd.date_range('2022-01-01',periods=400,freq='B').strftime('%Y-%m-%d'),
            'open':p-1,'high':p+1,'low':p-2,'close':p,'volume':np.ones(400)*1000})
        base=calc_indicators(d)
        d.loc[399,'close']=1000000
        changed=calc_indicators(d)
        for c in ['ma60','ma112','ma224','ma112_slope20','gap_60_112_pct','bb20_upper_shift26']:
            self.assertAlmostEqual(float(base[c].iloc[398]),float(changed[c].iloc[398]),places=9)
    def test_inverse_and_price_below(self):
        z=characterize(pd.Series({'close':75,'ma60':80,'ma112':90,'ma224':100}))
        self.assertEqual(z['alignment'],'MA_INVERSE_60_112_224')
        self.assertEqual(z['price_position'],'PRICE_BELOW_60_112')
    def test_early_turn_60_112_below_224(self):
        z=characterize(pd.Series({'close':118,'ma60':105,'ma112':99,'ma224':120}))
        self.assertEqual(z['alignment'],'MA_EARLY_60_UP_OVER_112_BELOW_224')
        self.assertEqual(z['price_position'],'PRICE_ABOVE_60_112')
    def test_ma224_warmup_must_be_explicit_unknown(self):
        z=characterize(pd.Series({'close':100,'ma60':100,'ma112':90,'ma224':np.nan}))
        self.assertEqual(z['alignment'],'INSUFFICIENT_224_HISTORY')
    def test_summary_screen_needs_2023_and_2024(self):
        d=pd.DataFrame({'code':[f'{i:06d}' for i in range(40)],'year':[2023]*40,
          'net_pct':[0]*40,'success_tp':[False]*40,'failed_sl':[True]*40,
          'mfe_pct':[2]*40,'mae_pct':[-6]*40})
        self.assertFalse(stats(d)['research_screen_n30_stocks15_eachyear10'])
    def test_wilder_signal_smooth(self):
        p=np.concatenate([np.linspace(100,180,250),np.linspace(180,185,40)])
        d=pd.DataFrame({'date':pd.date_range('2022-01-01',periods=len(p),freq='B').strftime('%Y-%m-%d'),
            'open':p,'high':p+1,'low':p-1,'close':p,'volume':1000.})
        f=calc_indicators(d)
        self.assertTrue(np.isfinite(f.ma224.iloc[-1]))
        self.assertTrue(np.isfinite(f.rsi_signal9.iloc[-1]))
    def test_224_is_not_reused_from_wave_high(self):
        p=np.linspace(250,100,270)
        d=pd.DataFrame({'date':pd.date_range('2022-01-01',periods=len(p),freq='B').strftime('%Y-%m-%d'),
            'open':p,'high':p+1,'low':p-1,'close':p,'volume':1000.})
        f=calc_indicators(d)
        z=characterize(f.iloc[-1]);self.assertEqual(z['alignment'],'MA_INVERSE_60_112_224')

if __name__=='__main__':unittest.main()
