import sys,unittest
from pathlib import Path
import numpy as np
import pandas as pd
HERE=Path(__file__).resolve().parents[1]
ROOT=HERE.parents[1]
sys.path[:0]=[str(HERE),str(ROOT/'research_projects'/'r1d101')]
from pre_rise import build_features,future_labels
from cloud_geometry import enrich_clouds,choose_unrestricted,pattern_state,MA_GROUPS


def raw(n=620):
    date=pd.bdate_range('2022-01-03',periods=n)
    x=120+np.arange(n)*.04+3*np.sin(np.arange(n)/9.)
    # Non-constant high/low; avoid future look-ahead disguised as cloud projection.
    return pd.DataFrame({'code':['000001']*n,'date':date,'open':x-.2,'high':x+1.2,
                         'low':x-1.7,'close':x,'volume':100000+1000*np.arange(n)%40000,
                         'amount':(100000+1000*np.arange(n)%40000)*x})


class CloudGeometryPITTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.d=raw()
        cls.f=enrich_clouds(build_features(cls.d))
    def test_ma_groups_nonoverlap_and_complete(self):
        sizes=[len(MA_GROUPS[z]) for z in ['FAST','MID','LONG']]
        self.assertEqual(sizes,[4,5,4]);self.assertEqual(len(MA_GROUPS['ALL']),13)
    def test_cloud_thickness_nonnegative(self):
        f=self.f
        for z in ['fast','mid','long','all']:
            self.assertTrue((f[f'ma_cloud_{z}_thickness_pct'].dropna()>=0).all())
    def test_cloud_upper_lower_known(self):
        f=self.f.iloc[-1]
        self.assertAlmostEqual(f['ma_cloud_fast_upper'],max(f[f'ma{n}'] for n in MA_GROUPS['FAST']))
        self.assertAlmostEqual(f['ma_cloud_fast_lower'],min(f[f'ma{n}'] for n in MA_GROUPS['FAST']))
    def test_no_fabricated_long_cloud_early(self):
        self.assertTrue(np.isnan(self.f.iloc[200].ma_cloud_long_thickness_pct))
        self.assertEqual(self.f.iloc[200].ma_cloud_long_position,'NOT_AVAILABLE')
    def test_height_sign_above_price(self):
        row=self.f.iloc[-1]
        center=(row.ma_cloud_fast_upper+row.ma_cloud_fast_lower)/2
        self.assertAlmostEqual(row.ma_cloud_fast_height_vs_price_pct,100*(center/row.close-1))
    def test_current_ichimoku_uses_prior_26_bars(self):
        f=self.f
        for t in [120,280,610]:
            self.assertAlmostEqual(f.iloc[t].ichimoku_current_span_a,f.iloc[t-26].ichimoku_projected_span_a_known_today)
            self.assertAlmostEqual(f.iloc[t].ichimoku_current_span_b,f.iloc[t-26].ichimoku_projected_span_b_known_today)
    def test_projected_span_is_known_today(self):
        f=self.f.iloc[-1];d=self.d
        h9=d.high.tail(9).max();l9=d.low.tail(9).min()
        h26=d.high.tail(26).max();l26=d.low.tail(26).min()
        h52=d.high.tail(52).max();l52=d.low.tail(52).min()
        self.assertAlmostEqual(f.ichimoku_projected_span_a_known_today,((h9+l9)/2+(h26+l26)/2)/2)
        self.assertAlmostEqual(f.ichimoku_projected_span_b_known_today,(h52+l52)/2)
    def test_ichimoku_no_future_cloud(self):
        self.assertTrue(np.isnan(self.f.iloc[75].ichimoku_current_span_b))
        self.assertTrue(np.isfinite(self.f.iloc[77].ichimoku_current_span_b))
    def test_all_13_ma_accel_measured(self):
        row=self.f.iloc[-1]
        for n in MA_GROUPS['ALL']:
            self.assertIn(f'ma{n}_slope20_acceleration5_pp',self.f)
            self.assertTrue(np.isfinite(row[f'ma{n}_slope20_acceleration5_pp']))
    def test_future_mutation_pit(self):
        t=550
        changed=self.d.copy()
        for col in ('open','high','low','close'):
            changed[col]=changed[col].astype(float)
            changed.loc[changed.index>t,col]*=1.217
        new=enrich_clouds(build_features(changed))
        pd.testing.assert_series_equal(self.f.iloc[t],new.iloc[t],check_names=False)
    def test_no_future_case_selection_at_end(self):
        l=future_labels(self.f)
        eligible,cases=choose_unrestricted(self.f,l,min_idx=541)
        self.assertTrue((eligible.idx < len(self.f)-20).all())
    def test_case_eligibility_includes_above_ma224(self):
        l=future_labels(self.f)
        eligible,_=choose_unrestricted(self.f,l,min_idx=541)
        self.assertTrue((eligible.ma224_zone=='ABOVE').any())
    def test_case_eligibility_includes_below_ma224_if_present(self):
        d=raw();d['close']=d['close']-12
        d.loc[d.index>=560,'close']-=15
        # Original open/high/low would become inconsistent; update them consistently.
        d['open']=d['close']-.2;d['high']=d['close']+1.2;d['low']=d['close']-1.7
        f=enrich_clouds(build_features(d));l=future_labels(f)
        eligible,_=choose_unrestricted(f,l,min_idx=541)
        self.assertTrue((eligible.ma224_zone=='BELOW').any())
    def test_global_cooldown_no_cross_label_overlap(self):
        l=future_labels(self.f,control_max_pct=30.,surge_pct=.1,hold_pct=.01)
        _,c=choose_unrestricted(self.f,l,min_idx=541,cooldown=20)
        if len(c)>=2:
            self.assertTrue((c.idx.sort_values().diff().dropna()>=20).all())
    def test_determinism(self):
        l=future_labels(self.f)
        _,a=choose_unrestricted(self.f,l,min_idx=541)
        _,b=choose_unrestricted(self.f,l,min_idx=541)
        pd.testing.assert_frame_equal(a,b)
    def test_pattern_not_future_outcome(self):
        a=pattern_state(self.f.iloc[-1]);self.assertIn('ma224_zone',a)
        self.assertNotIn('future20_close_pct',a)
    def test_fast_thickness_has_5bar_change(self):
        self.assertTrue(np.isfinite(self.f.iloc[-1].ma_cloud_fast_thickness_change5_pp))
    def test_current_cloud_position_is_string(self):
        self.assertIn(self.f.iloc[-1].ichimoku_current_position,['ABOVE','BELOW','INSIDE'])
    def test_cloud_twist_recorded(self):
        self.assertIn('ichimoku_current_twist_today',self.f)
        self.assertIn('ichimoku_projected_known_today_twist_today',self.f)
    def test_cloud_thickness_zero_possible(self):
        c=raw(620)
        c[['open','high','low','close']]=100.
        f=enrich_clouds(build_features(c))
        self.assertEqual(f.iloc[-1].ichimoku_current_thickness_pct,0.)
        self.assertEqual(f.iloc[-1].ma_cloud_fast_thickness_pct,0.)
        self.assertTrue(np.isnan(f.iloc[-1].ma_cloud_fast_price_percentile_inside))
    def test_unsorted_date_rejected(self):
        d=raw();d.loc[9,'date']=d.loc[8,'date']
        with self.assertRaises(ValueError):enrich_clouds(build_features(d))

if __name__=='__main__':unittest.main()
