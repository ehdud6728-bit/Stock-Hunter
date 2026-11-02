import sys,unittest,re
from pathlib import Path
import pandas as pd
import numpy as np
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from multiline import (anchors,make_lines,price_crosses,triangles,followthrough,validate,
    BULL,LOW,HIGH,line_value)

DIO_TEXT='''2024-08-16 15920 16270 15830 16000 82435 1318312230
2024-08-19 16200 16920 16020 16350 155978 2564925860
2024-08-20 16350 16400 16060 16200 47067 760447960
2024-08-21 16030 16200 15790 16000 67509 1074869090
2024-08-22 16050 16070 15860 16000 36617 585448610
2024-08-23 15800 15960 15660 15830 53424 842224940
2024-08-26 16000 16000 15610 15830 29380 461294990
2024-08-27 15860 15860 15630 15850 19612 308290840
2024-08-28 15900 15980 15750 15860 29558 467558750
2024-08-29 15800 16250 15750 16200 62668 1006898080
2024-08-30 16160 16280 16040 16200 30456 491118390
2024-09-02 16020 16300 16010 16200 23808 384844870
2024-09-03 16240 16690 16160 16600 48139 793714900
2024-09-04 16350 16490 15950 16340 54520 879217760
2024-09-05 16250 16420 16140 16300 26793 435333280
2024-09-06 16160 16250 15550 16040 50454 796455530
2024-09-09 15610 16200 15610 16200 27144 434048820
2024-09-10 16220 16240 15970 16160 20856 335419640
2024-09-11 16060 16200 16000 16200 26862 431925940
2024-09-12 16180 16490 16080 16370 67592 1097496850
2024-09-13 16270 17210 16200 17030 78749 1327184140
2024-09-19 16810 17210 16810 17210 52519 897110950
2024-09-20 17310 17850 16730 17160 141853 2454923570
2024-09-23 17290 17290 16630 16760 56335 948720540
2024-09-24 16700 16800 16330 16370 59872 985130190
2024-09-25 16370 16640 16370 16420 29175 480974320
2024-09-26 16620 16620 16320 16470 49251 808791560
2024-09-27 16500 16680 16180 16500 23317 384797300'''

def fixture():
    rows=[]
    for line in DIO_TEXT.splitlines():
        v=line.split();rows.append([v[0],*map(float,v[1:])])
    d=pd.DataFrame(rows,columns=['date','open','high','low','close','volume','amount'])
    d['date']=pd.to_datetime(d.date)
    return d

class R1D106Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.d=fixture();cls.l=make_lines(cls.d);cls.ev=price_crosses(cls.d,cls.l)
        cls.tr=triangles(cls.d,cls.l)
        cls.u=cls.l[(cls.l.family==BULL)&cls.l.a1.eq('2024-09-03')&cls.l.a2.eq('2024-09-09')].iloc[0]
        cls.lo=cls.l[(cls.l.family==LOW)&cls.l.a1.eq('2024-08-26')&cls.l.a2.eq('2024-09-06')].iloc[0]
    def test_open_high_low_close_valid(self):validate(self.d)
    def test_unsorted_rejected(self):
        bad=self.d.iloc[::-1].reset_index(drop=True)
        with self.assertRaises(ValueError):validate(bad)
    def test_candle_anchor_same_day_known(self):self.assertEqual(self.u.known_date,'2024-09-09')
    def test_body_top_basis(self):self.assertEqual(self.u.v1,16600)
    def test_bull_top_second_value(self):self.assertEqual(self.u.v2,16200)
    def test_bull_top_slope(self):self.assertEqual(self.u.slope_krw_per_bar,-100)
    def test_low_confirm_delay(self):self.assertEqual(self.lo.known_date,'2024-09-10')
    def test_low_first_price(self):self.assertEqual(self.lo.v1,15610)
    def test_low_second_price(self):self.assertEqual(self.lo.v2,15550)
    def test_low_slope(self):self.assertAlmostEqual(self.lo.slope_krw_per_bar,-60/9,5)
    def test_0910_top(self):self.assertEqual(line_value(self.u,17),16100)
    def test_0910_lower(self):self.assertAlmostEqual(line_value(self.lo,17),15536.666666,3)
    def test_body_candidate_not_marked_pivot(self):self.assertEqual(self.u.anchor_confirmation_type,'CLOSE_SAME_DAY')
    def test_no_same_day_cross(self):
        a=self.ev[self.ev.line_id.eq(self.u.line_id)]
        self.assertTrue((a.idx>self.u.known_idx).all())
    def test_first_cross_0910(self):
        a=self.ev[(self.ev.line_id==self.u.line_id)&(self.ev.event=='CROSS_UP')]
        self.assertEqual(a.iloc[0].date,'2024-09-10')
        self.assertAlmostEqual(a.iloc[0].pct_beyond,.3727,4)
    def test_weak_break_0p4_gate(self):
        a=self.ev[(self.ev.line_id==self.u.line_id)&(self.ev.date=='2024-09-10')]
        self.assertFalse(bool(a.iloc[0].strong_close_0p4pct))
    def test_triangle_first_known_0910(self):
        x=self.tr[(self.tr.upper_id==self.u.line_id)&(self.tr.lower_id==self.lo.line_id)]
        self.assertEqual(len(x),1);self.assertEqual(x.iloc[0].date,'2024-09-10')
    def test_triangle_no_0909(self):
        x=self.tr[(self.tr.upper_id==self.u.line_id)&(self.tr.lower_id==self.lo.line_id)]
        self.assertFalse((x.idx<17).any())
    def test_triangle_contraction(self):
        x=self.tr[(self.tr.upper_id==self.u.line_id)&(self.tr.lower_id==self.lo.line_id)]
        self.assertLess(x.iloc[0].width_change_krw_per_bar,0)
    def test_triangle_price_above_top_at_discovery(self):
        x=self.tr[(self.tr.upper_id==self.u.line_id)&(self.tr.lower_id==self.lo.line_id)]
        self.assertEqual(x.iloc[0].price_position_at_first_known,'ABOVE_TOP')
    def test_apex_causal_geometry(self):
        x=self.tr[(self.tr.upper_id==self.u.line_id)&(self.tr.lower_id==self.lo.line_id)].iloc[0]
        self.assertAlmostEqual(x.bars_until_apex,6.036,2)
        self.assertEqual(x.signal_readiness,'NO_PRIOR_TRIANGLE_BREAKOUT_SIGNAL')
    def test_immutable_archive(self):self.assertTrue((self.l.status=='ARCHIVE_FOREVER_NO_EXPIRY').all())
    def test_line_identifiers_unique(self):self.assertFalse(self.l.line_id.duplicated().any())
    def test_all_family_catalog(self):self.assertIn(LOW,self.l.family.values)
    def test_future_prices_no_change_on_pit_lines(self):
        b=self.d.copy()
        idx=17
        for c in ('open','high','low','close'):b.loc[idx+1:,c]*=1.19
        b.loc[idx+1:,'volume']*=1.5;b.loc[idx+1:,'amount']*=1.5
        ll=make_lines(b)
        old=self.l[self.l.known_idx<=idx].set_index('line_id').sort_index()
        new=ll[ll.known_idx<=idx].set_index('line_id').sort_index()
        pd.testing.assert_frame_equal(old,new)
    def test_future_prices_no_change_on_pit_crosses(self):
        b=self.d.copy();idx=17
        for c in ('open','high','low','close'):b.loc[idx+1:,c]*=1.19
        ll=make_lines(b)
        a=self.ev[self.ev.idx<=idx].sort_values(['line_id','idx']).reset_index(drop=True)
        q=price_crosses(b,ll);q=q[q.idx<=idx].sort_values(['line_id','idx']).reset_index(drop=True)
        pd.testing.assert_frame_equal(a,q)
    def test_future_prices_no_change_triangle_before_t(self):
        b=self.d.copy();idx=17
        for c in ('open','high','low','close'):b.loc[idx+1:,c]*=1.19
        ll=make_lines(b)
        q=triangles(b,ll)
        a=self.tr[self.tr.idx<=idx].sort_values(['upper_id','lower_id']).reset_index(drop=True)
        c=q[q.idx<=idx].sort_values(['upper_id','lower_id']).reset_index(drop=True)
        pd.testing.assert_frame_equal(a,c)
    def test_followthrough_not_input_to_event_selection(self):
        future=followthrough(self.d,self.ev,self.l)
        a=self.ev[(self.ev.line_id==self.u.line_id)&self.ev.date.eq('2024-09-10')]
        b=future[(future.line_id==self.u.line_id)&future.date.eq('2024-09-10')]
        self.assertEqual(len(a),1);self.assertEqual(len(b),1)
        self.assertTrue(bool(b.iloc[0].Dp5_available))
        self.assertNotIn('Dp5_close_pct',self.ev.columns)
    def test_prolonged_hypothesis_no_return_calibrated_threshold(self):
        self.assertTrue((self.l.anchor_strength>0).all())
    def test_nonfinite_rejected(self):
        b=self.d.copy();b.loc[3,'high']=np.nan
        with self.assertRaises(ValueError):validate(b)
    def test_invalid_high_rejected(self):
        b=self.d.copy();b.loc[5,'high']=b.loc[5,'low']-1
        with self.assertRaises(ValueError):validate(b)

if __name__=='__main__':unittest.main()
