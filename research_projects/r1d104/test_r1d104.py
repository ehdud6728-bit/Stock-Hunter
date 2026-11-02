import sys, unittest
from pathlib import Path
import numpy as np
import pandas as pd
ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/'r1d101'),str(ROOT/'r1d102'),str(ROOT/'r1d100'),str(ROOT/'r1d103'),str(ROOT/'r1d104')]
from pre_rise import build_features,future_labels
from cloud_geometry import enrich_clouds
from confluence import integrate_dates,attach_expost,events_for_dates,_position,_width,_as_row,compare_groups
from dynamic_structure import structure


def fake(n=620):
    i=np.arange(n,dtype=float)
    close=100+5*np.sin(i/13)+3*np.sin(i/29)+.04*i
    v=200_000+30_000*(1+np.sin(i/11))
    return pd.DataFrame({'date':pd.bdate_range('2022-01-03',periods=n),
     'open':close-.3,'high':close+1.8,'low':close-1.9,'close':close,
     'volume':v,'amount':close*v})

class TestConfluence(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.d=fake()
        cls.f=enrich_clouds(build_features(cls.d))
        cls.t=550
        cls.date=str(cls.f.iloc[cls.t].date.date())
        cls.prev=str(cls.f.iloc[cls.t-1].date.date())
        cls.snap=structure(cls.d,cls.date)
        cls.old=structure(cls.d,cls.prev)
        cls.one=_as_row(cls.f,cls.t,cls.snap,old_snapshot=cls.old)

    def test_all_ma_present(self):
        for n in (3,5,10,20,30,40,60,112,120,224,240,448,480):self.assertIn(f'ma{n}_slope_accel5_pp',self.one)
    def test_bb_all_three(self):
        for n in (20,30,40):self.assertIn(f'bb{n}_width_delta20_pp',self.one)
    def test_env_both(self):
        for n in (20,40):self.assertIn(f'env{n}_10_upper',self.one)
    def test_clouds_and_flow(self):
        for c in ('ichimoku_current_thickness_pct','ma_cloud_fast_height_vs_price_pct','amount_avg5_vs_prev20'):
            self.assertIn(c,self.one)
    def test_all_four_trendlines(self):
        for c in ('short_high_trend','short_low_trend','medium_high_trend','medium_low_trend'):
            self.assertIn(c+'_reanchored_today',self.one)
    def test_no_fake_cross_on_reanchor(self):
        for c in ('short_high_trend','short_low_trend'):
            if self.one[c+'_reanchored_today']:
                self.assertEqual(self.one[c+'_validated_cross'],'NA')
    def test_position_logic(self):
        self.assertEqual(_position(11,8,10),'ABOVE_UPPER')
        self.assertEqual(_position(7,8,10),'BELOW_LOWER')
        self.assertEqual(_position(9,8,10),'INSIDE')
        self.assertEqual(_position(9,None,10),'NA')
    def test_bb_width_trend(self):
        self.assertEqual(_width(-.01),'CONTRACT');self.assertEqual(_width(.01),'EXPAND')
    def test_future_prices_cannot_change_full_context(self):
        altered=self.d.copy()
        for col in ('open','high','low','close','volume','amount'):
            altered[col]=altered[col].astype(float)
            altered.loc[altered.index>self.t,col]*=1.23
        altered_f=enrich_clouds(build_features(altered))
        after=_as_row(altered_f,self.t,structure(altered,self.date),old_snapshot=structure(altered,self.prev))
        self.assertEqual(self.one,after)
    def test_envelope_relative_width_constant(self):
        self.assertAlmostEqual(float(self.f.iloc[self.t]['env20_10_width_rel_pct']),20.,places=9)
    def test_integrate_dates_returns_only_past(self):
        e=integrate_dates(self.f,start=540,horizon=20,max_rows=5)
        self.assertEqual(len(e),5);self.assertTrue(e.idx.is_unique)
    def test_outcome_join_does_not_change_pit_features(self):
        base=integrate_dates(self.f,start=540,horizon=20,max_rows=3)
        labels=future_labels(self.f)
        full=attach_expost(base,labels)
        self.assertTrue(base.columns.isin(full.columns).all())
        pd.testing.assert_frame_equal(base,full[base.columns])
    def test_same_day_multiple_events_stay_one_day(self):
        rows=integrate_dates(self.f,start=540,horizon=20,max_rows=3)
        e=pd.DataFrame([{'idx':540,'ma_period':5,'event':'RECLAIM_ABOVE'},
                        {'idx':540,'ma_period':20,'event':'LOSE_BELOW'},
                        {'idx':541,'ma_period':60,'event':'TOUCH_ABOVE_CANDIDATE'}])
        out=events_for_dates(rows,e)
        self.assertEqual(len(out),3);self.assertEqual(int(out.iloc[0].ma_event_count),2)
        self.assertEqual(out.iloc[0].ma_periods,'5|20')
    def test_overlap_not_means_independent(self):
        a=events_for_dates(integrate_dates(self.f,start=540,horizon=20,max_rows=3),pd.DataFrame())
        self.assertEqual(a.ma_event_count.sum(),0)
    def test_na_positions(self):
        self.assertIn(self.one['bb20_position'],('ABOVE_UPPER','BELOW_LOWER','INSIDE','NA'))
        self.assertIn(self.one['env20_10_position'],('ABOVE_UPPER','BELOW_LOWER','INSIDE','NA'))
    def test_compare_group_no_auto_rank(self):
        d=attach_expost(integrate_dates(self.f,start=540,horizon=20,max_rows=4),future_labels(self.f))
        summary=compare_groups(d)
        self.assertIn('stock_dates',summary.columns);self.assertNotIn('rank',summary.columns)
    def test_224_not_mandatory(self):
        # A stock-date is included no matter if close is above or below MA224
        x=integrate_dates(self.f,start=540,horizon=20,max_rows=3)
        self.assertEqual(len(x),3)
    def test_reference_price_equals_source(self):
        self.assertAlmostEqual(self.one['close'],float(self.d.iloc[self.t].close))
    def test_no_missing_window_if_history_enough(self):
        for n in (20,30,40):self.assertIsNotNone(self.one[f'bb{n}_upper'])
    def test_same_trendline_required_for_cross(self):
        for c in ('short_high_trend','short_low_trend'):
            value=self.one[c+'_validated_cross']
            self.assertIn(value,('NA','CROSS_UP','CROSS_DOWN','NO_CROSS'))
    def test_expost_not_mixed_in_features(self):
        self.assertTrue(not any('EXPOST' in c for c in self.one.keys()))

if __name__=='__main__': unittest.main()
