import unittest,sys
from pathlib import Path
import numpy as np,pandas as pd
BASE=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(BASE))
from candle_context import candle_features
from lifecycle import track_lines

class CandleTests(unittest.TestCase):
    def setUp(self):
        o=[100,100,100,100,101,102,101,100,102,103,103,104,104,106,107,110,111,112,113,114,115,116,117,118,119,120]
        self.d=pd.DataFrame({'date':pd.bdate_range('2024-01-01',periods=len(o)),
                  'open':o,'close':[x+1 for x in o],'high':[x+4 for x in o],
                  'low':[x-2 for x in o],'volume':[100]*len(o),'amount':[100000]*len(o)})
    def test_parts_100pct(self):
        a=candle_features(self.d)
        v=a.iloc[15]
        self.assertAlmostEqual(v.candle_body_share_pct+v.candle_upper_wick_share_pct+v.candle_lower_wick_share_pct,100)
    def test_wick_vs_body(self):
        a=candle_features(self.d)
        self.assertAlmostEqual(a.iloc[0].candle_upper_wick_body_ratio,3)
        self.assertAlmostEqual(a.iloc[0].candle_lower_wick_body_ratio,2)
    def test_zero_range_safe(self):
        d=self.d.copy();d.loc[1,['open','high','low','close']]=100
        a=candle_features(d);self.assertTrue(pd.isna(a.loc[1,'candle_body_share_pct']))
    def test_doji_no_div_zero(self):
        d=self.d.copy();d.loc[1,'close']=d.loc[1,'open'];a=candle_features(d)
        self.assertTrue(pd.isna(a.loc[1,'candle_upper_wick_body_ratio']))
    def test_prev2_3_not_future(self):
        a=candle_features(self.d);d=self.d.copy();d[['close','high']]=d[['close','high']].astype(float);d.loc[21:,'close']*=1.1;d.loc[21:,'high']*=1.1
        b=candle_features(d)
        for k in ('close_vs_prev3_high_max_pct','candle_body_share_pct','candle_range_vs_prev14_atr'):
            pd.testing.assert_series_equal(a.loc[:20,k],b.loc[:20,k])
    def test_compare_exact_vs_window(self):
        a=candle_features(self.d)
        self.assertTrue('close_vs_tminus2_high_pct' in a)
        self.assertTrue('close_vs_prev2_high_max_pct' in a)
    def test_tolerance(self):
        from candle_context import _relation
        self.assertEqual(_relation(.2),'SIMILAR');self.assertEqual(_relation(.31),'ABOVE')
    def test_chaotic_order_rejected(self):
        with self.assertRaises(ValueError):candle_features(self.d.iloc[::-1])
    def test_invalid_ohlc_rejected(self):
        d=self.d.copy();d.loc[0,'high']=1
        with self.assertRaises(ValueError):candle_features(d)
    def test_dio_actual_2024_09_13_candle(self):
        r=pd.DataFrame({'date':['2024-09-13'],'open':[16270], 'high':[17210], 'low':[16200],
                        'close':[17030], 'volume':[78749], 'amount':[1327184140]})
        d=candle_features(r).iloc[0]
        self.assertAlmostEqual(d.candle_body_share_pct,100*760/1010)
        self.assertAlmostEqual(d.candle_upper_wick_share_pct,100*180/1010)
        self.assertAlmostEqual(d.candle_lower_wick_share_pct,100*70/1010)
    def test_sdi_actual_2024_02_07_upper_wick(self):
        r=pd.DataFrame({'date':['2024-02-07'],'open':[378000],'high':[395000],
          'low':[378000],'close':[382000],'volume':[454296],'amount':[176295445500]})
        d=candle_features(r).iloc[0]
        self.assertAlmostEqual(d.candle_upper_wick_share_pct,100*13000/17000)
        self.assertAlmostEqual(d.candle_upper_wick_body_ratio,3.25)
        self.assertEqual(d.candle_shape_diagnostic,'LONG_UPPER_WICK_REJECTION_CANDIDATE')
    def test_atr_previous_only(self):
        a=candle_features(self.d)
        self.assertTrue(pd.isna(a.loc[13,'candle_range_vs_prev14_atr']))
        self.assertTrue(np.isfinite(a.loc[15,'candle_range_vs_prev14_atr']))
    def test_wick_pattern_not_forecast(self):
        a=candle_features(self.d)
        self.assertFalse(any('PREDICT' in str(s) for s in a.candle_shape_diagnostic))

class StateTests(unittest.TestCase):
    def setUp(self):
        n=28
        c=np.array([100.0]*n);c[12]=108;c[13]=111
        self.d=pd.DataFrame({'date':pd.bdate_range('2024-01-01',periods=n),
           'open':c-1,'close':c,'high':c+2,'low':c-2,'amount':[100000]*n,'volume':[100]*n})
        self.line=pd.DataFrame([{'line_id':'U1','kind':'UPPER','family':'BULL_BODY_TOP',
                'i1':3,'i2':7,'known_idx':7,'known_date':str(self.d.date.iloc[7].date()),
                'v1':103,'v2':103,'slope_krw_per_bar':0,'slope_pct_per_bar':0,
                'anchor_strength':1.,'penetrations_1p2pct':0,
                'a1':'2024-01-04','a2':'2024-01-10'},
                {'line_id':'U2','kind':'UPPER','family':'WICK_HIGH_PIVOT2',
                'i1':14,'i2':18,'known_idx':20,'known_date':str(self.d.date.iloc[20].date()),
                'v1':120,'v2':119,'slope_krw_per_bar':-.25,'slope_pct_per_bar':-.2083,
                'anchor_strength':1.,'penetrations_1p2pct':0,
                'a1':'2024-01-19','a2':'2024-01-25'}])
    def test_ever_archived(self):
        before=self.line.copy(deep=True);_ =track_lines(self.d,self.line,start_idx=3)
        pd.testing.assert_frame_equal(before,self.line)
    def test_known_before_used(self):
        r=track_lines(self.d,self.line)
        self.assertEqual(len(r),4*len(self.d))
        rr=r[r.active_line_id.eq('U2')];self.assertTrue((rr.idx>=20).all())
    def test_pit(self):
        a=track_lines(self.d,self.line).query('idx <= 15').reset_index(drop=True)
        alt=self.d.copy();alt.loc[16:,'high']+=50;alt.loc[16:,'close']+=40
        b=track_lines(alt,self.line).query('idx <= 15').reset_index(drop=True)
        pd.testing.assert_frame_equal(a,b)
    def test_wait_not_resurrect(self):
        r=track_lines(self.d,self.line)
        u=r.query("lane == 'UPPER_SHORT'")
        self.assertTrue((u.state=='RESISTANCE_BROKEN_UP').any())
        t=u.query("state == 'RESISTANCE_BROKEN_UP'").idx.iloc[0]
        self.assertFalse((u[u.idx.eq(t+1)].active_line_id=='U1').any())
    def test_empty_lines(self):
        x=track_lines(self.d,self.line.head(0),start_idx=20)
        self.assertEqual(len(x),8*4)

if __name__=='__main__':unittest.main()
